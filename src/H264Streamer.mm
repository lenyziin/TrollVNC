/*
 This file is part of TrollVNC
 Copyright (c) 2025 contributors

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2
 as published by the Free Software Foundation.
*/

#import "H264Streamer.h"
#import "ScreenCapturer.h"

#import <Accelerate/Accelerate.h>
#import <CoreVideo/CoreVideo.h>
#import <QuartzCore/QuartzCore.h>
#import <VideoToolbox/VideoToolbox.h>

#import <arpa/inet.h>
#import <fcntl.h>
#import <netinet/in.h>
#import <netinet/tcp.h>
#import <pthread.h>
#import <sys/socket.h>
#import <sys/time.h>
#import <unistd.h>

#import <vector>

#define HLog(fmt, ...) fprintf(stderr, "[H264] " fmt "\n", ##__VA_ARGS__)

static const uint8_t kAnnexBStart[4] = {0x00, 0x00, 0x00, 0x01};

static void H264_OutputCallback(void *outputCallbackRefCon, void *sourceFrameRefCon, OSStatus status,
                                VTEncodeInfoFlags infoFlags, CMSampleBufferRef sampleBuffer);
static void writeAll(int fd, const uint8_t *buf, size_t len, bool *ok);

@interface H264Streamer () {
    int _listenFd;
    int _port;
    double _scale;
    int _fps;
    int _bitrateKbps;
    volatile BOOL _running;
    pthread_t _acceptThread;
    BOOL _acceptStarted;

    VTCompressionSessionRef _session;
    int _encW, _encH;
    CVPixelBufferPoolRef _pool;
    int64_t _frameIndex;

    std::vector<int> *_clients;
    std::vector<bool> *_synced; // per-client: has it received its first IDR yet?
    pthread_mutex_t _clientsMutex;
    volatile BOOL _forceKeyframe;

    // Steady-framerate idle repeat: keeps a live player's clock stable (raw H.264 has no timestamps).
    CVPixelBufferRef _lastBuf;
    NSData *_lastKeyframe; // last keyframe as TS (PAT+PMT+IDR), replayed to new clients
    double _lastEncTime;

    // MPEG-TS muxer state
    uint8_t _ccPat, _ccPmt, _ccVid;
    int64_t _pts90;
    dispatch_queue_t _idleQueue;
    dispatch_source_t _idleTimer;
    pthread_mutex_t _encMutex;
    BOOL _idleStarted;
}
- (instancetype)initPrivate;
- (void)acceptLoop;
- (void)ensureSessionForWidth:(size_t)nativeW height:(size_t)nativeH;
- (void)doEncodeBuffer:(CVPixelBufferRef)buf force:(BOOL)force;
- (void)idleTick;
- (void)broadcast:(NSData *)data keyframe:(bool)isKeyframe;
- (void)handleEncoded:(CMSampleBufferRef)sampleBuffer;
@end

static void *acceptTrampoline(void *ctx) {
    @autoreleasepool {
        [(__bridge H264Streamer *)ctx acceptLoop];
    }
    return NULL;
}

// ---- Minimal MPEG-TS muxer -------------------------------------------------
// Wraps the H.264 Annex B access units in an MPEG-TS stream so any player reads
// it instantly with correct timestamps (raw Annex B forces huge buffering).
#define TS_PID_PAT 0x0000
#define TS_PID_PMT 0x1000
#define TS_PID_VID 0x0100

static uint32_t tsCRC32(const uint8_t *d, size_t n) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < n; i++) {
        crc ^= (uint32_t)d[i] << 24;
        for (int b = 0; b < 8; b++)
            crc = (crc & 0x80000000u) ? (crc << 1) ^ 0x04C11DB7u : (crc << 1);
    }
    return crc;
}

static void tsBuildPAT(uint8_t *pkt, uint8_t cc) {
    memset(pkt, 0xFF, 188);
    pkt[0] = 0x47;
    pkt[1] = 0x40 | ((TS_PID_PAT >> 8) & 0x1F);
    pkt[2] = TS_PID_PAT & 0xFF;
    pkt[3] = 0x10 | (cc & 0x0F);
    pkt[4] = 0x00; // pointer field
    uint8_t *sec = pkt + 5;
    int s = 0;
    sec[s++] = 0x00;                                  // table_id (PAT)
    sec[s++] = 0xB0;                                  // syntax=1, section_length hi
    sec[s++] = 0x0D;                                  // section_length = 13
    sec[s++] = 0x00; sec[s++] = 0x01;                 // transport_stream_id
    sec[s++] = 0xC1;                                  // version 0, current_next 1
    sec[s++] = 0x00;                                  // section_number
    sec[s++] = 0x00;                                  // last_section_number
    sec[s++] = 0x00; sec[s++] = 0x01;                 // program_number 1
    sec[s++] = 0xE0 | ((TS_PID_PMT >> 8) & 0x1F);     // PMT PID hi
    sec[s++] = TS_PID_PMT & 0xFF;                      // PMT PID lo
    uint32_t crc = tsCRC32(sec, s);
    sec[s++] = (crc >> 24) & 0xFF;
    sec[s++] = (crc >> 16) & 0xFF;
    sec[s++] = (crc >> 8) & 0xFF;
    sec[s++] = crc & 0xFF;
}

static void tsBuildPMT(uint8_t *pkt, uint8_t cc) {
    memset(pkt, 0xFF, 188);
    pkt[0] = 0x47;
    pkt[1] = 0x40 | ((TS_PID_PMT >> 8) & 0x1F);
    pkt[2] = TS_PID_PMT & 0xFF;
    pkt[3] = 0x10 | (cc & 0x0F);
    pkt[4] = 0x00; // pointer field
    uint8_t *sec = pkt + 5;
    int s = 0;
    sec[s++] = 0x02;                                  // table_id (PMT)
    sec[s++] = 0xB0;                                  // syntax; length hi patched below
    sec[s++] = 0x00;                                  // length lo patched below
    sec[s++] = 0x00; sec[s++] = 0x01;                 // program_number 1
    sec[s++] = 0xC1;                                  // version 0, current_next 1
    sec[s++] = 0x00;                                  // section_number
    sec[s++] = 0x00;                                  // last_section_number
    sec[s++] = 0xE0 | ((TS_PID_VID >> 8) & 0x1F);     // PCR PID hi
    sec[s++] = TS_PID_VID & 0xFF;                      // PCR PID lo
    sec[s++] = 0xF0;                                  // program_info_length hi
    sec[s++] = 0x00;                                  // program_info_length lo
    sec[s++] = 0x1B;                                  // stream_type H.264
    sec[s++] = 0xE0 | ((TS_PID_VID >> 8) & 0x1F);     // elementary PID hi
    sec[s++] = TS_PID_VID & 0xFF;                      // elementary PID lo
    sec[s++] = 0xF0;                                  // ES_info_length hi
    sec[s++] = 0x00;                                  // ES_info_length lo
    int section_length = (s - 3) + 4;                 // bytes after length field + CRC
    sec[1] = 0xB0 | ((section_length >> 8) & 0x0F);
    sec[2] = section_length & 0xFF;
    uint32_t crc = tsCRC32(sec, s);
    sec[s++] = (crc >> 24) & 0xFF;
    sec[s++] = (crc >> 16) & 0xFF;
    sec[s++] = (crc >> 8) & 0xFF;
    sec[s++] = crc & 0xFF;
}

// Wrap one access unit (Annex B) as PES, split into 188-byte TS packets.
static void tsAppendPES(NSMutableData *out, const uint8_t *au, size_t auLen, int64_t pts, bool keyframe,
                        uint8_t *vidCC) {
    static const uint8_t kAUD[6] = {0x00, 0x00, 0x00, 0x01, 0x09, 0xF0};
    size_t pesLen = 14 + sizeof(kAUD) + auLen;
    uint8_t *pes = (uint8_t *)malloc(pesLen);
    if (!pes)
        return;
    int h = 0;
    pes[h++] = 0x00; pes[h++] = 0x00; pes[h++] = 0x01; pes[h++] = 0xE0;
    pes[h++] = 0x00; pes[h++] = 0x00;                 // PES length: 0 = unbounded (video)
    pes[h++] = 0x84;                                  // '10', data_alignment
    pes[h++] = 0x80;                                  // PTS only
    pes[h++] = 0x05;                                  // PES header data length
    pes[h++] = (uint8_t)(0x21 | ((pts >> 29) & 0x0E));
    pes[h++] = (uint8_t)((pts >> 22) & 0xFF);
    pes[h++] = (uint8_t)(0x01 | ((pts >> 14) & 0xFE));
    pes[h++] = (uint8_t)((pts >> 7) & 0xFF);
    pes[h++] = (uint8_t)(0x01 | ((pts << 1) & 0xFE));
    memcpy(pes + h, kAUD, sizeof(kAUD)); h += sizeof(kAUD);
    memcpy(pes + h, au, auLen);

    size_t pos = 0;
    bool firstPkt = true;
    while (pos < pesLen) {
        size_t remaining = pesLen - pos;
        bool wantPCR = (firstPkt && keyframe);
        size_t payloadLen;
        bool af;
        if (wantPCR) {
            payloadLen = remaining < 176 ? remaining : 176;
            af = true;
        } else if (remaining < 184) {
            payloadLen = remaining;
            af = true;
        } else {
            payloadLen = 184;
            af = false;
        }
        uint8_t pkt[188];
        pkt[0] = 0x47;
        pkt[1] = (uint8_t)((firstPkt ? 0x40 : 0x00) | ((TS_PID_VID >> 8) & 0x1F));
        pkt[2] = (uint8_t)(TS_PID_VID & 0xFF);
        uint8_t afc = af ? (payloadLen > 0 ? 0x30 : 0x20) : 0x10;
        pkt[3] = (uint8_t)(afc | (*vidCC & 0x0F));
        *vidCC = (*vidCC + 1) & 0x0F;
        int idx = 4;
        if (af) {
            int afLen = 183 - (int)payloadLen;
            pkt[idx++] = (uint8_t)afLen;
            if (afLen > 0) {
                pkt[idx++] = wantPCR ? 0x10 : 0x00; // adaptation flags
                int already = 1;
                if (wantPCR) {
                    int64_t base = pts & 0x1FFFFFFFFLL; // PCR base = PTS (ext 0)
                    pkt[idx++] = (uint8_t)((base >> 25) & 0xFF);
                    pkt[idx++] = (uint8_t)((base >> 17) & 0xFF);
                    pkt[idx++] = (uint8_t)((base >> 9) & 0xFF);
                    pkt[idx++] = (uint8_t)((base >> 1) & 0xFF);
                    pkt[idx++] = (uint8_t)(((base & 1) << 7) | 0x7E);
                    pkt[idx++] = 0x00;
                    already += 6;
                }
                for (int k = 0; k < afLen - already; k++)
                    pkt[idx++] = 0xFF;
            }
        }
        memcpy(pkt + idx, pes + pos, payloadLen);
        [out appendBytes:pkt length:188];
        pos += payloadLen;
        firstPkt = false;
    }
    free(pes);
}
// ---------------------------------------------------------------------------

@implementation H264Streamer

+ (instancetype)sharedStreamer {
    static H264Streamer *s = nil;
    static dispatch_once_t once;
    dispatch_once(&once, ^{
        s = [[H264Streamer alloc] initPrivate];
    });
    return s;
}

- (instancetype)initPrivate {
    if ((self = [super init])) {
        _listenFd = -1;
        _session = NULL;
        _pool = NULL;
        _clients = new std::vector<int>();
        _synced = new std::vector<bool>();
        pthread_mutex_init(&_clientsMutex, NULL);
        pthread_mutex_init(&_encMutex, NULL);
    }
    return self;
}

- (BOOL)isRunning {
    return _running;
}

- (BOOL)startOnPort:(int)port scale:(double)scale fps:(int)fps bitrateKbps:(int)kbps {
    if (_running)
        return YES;
    _port = port;
    _scale = (scale > 0.0 && scale <= 1.0) ? scale : 1.0;
    _fps = fps > 0 ? fps : 30;
    _bitrateKbps = kbps > 0 ? kbps : 500;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        HLog("socket() failed: %s", strerror(errno));
        return NO;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)_port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        HLog("bind(%d) failed: %s", _port, strerror(errno));
        close(fd);
        return NO;
    }
    if (listen(fd, 4) != 0) {
        HLog("listen() failed: %s", strerror(errno));
        close(fd);
        return NO;
    }
    _listenFd = fd;
    _running = YES;

    if (pthread_create(&_acceptThread, NULL, acceptTrampoline, (__bridge void *)self) == 0) {
        _acceptStarted = YES;
        pthread_detach(_acceptThread);
    }
    HLog("listening for H.264 clients on TCP port %d (scale=%.2f fps=%d bitrate=%dkbps)", _port, _scale, _fps,
         _bitrateKbps);
    return YES;
}

- (void)acceptLoop {
    while (_running) {
        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(_listenFd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) {
            if (!_running)
                break;
            continue;
        }
        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)); // don't let a dead client SIGPIPE the daemon
        struct timeval sndto = {5, 0}; // generous on a slow cellular uplink; a timeout skips a frame, not the client
        setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &sndto, sizeof(sndto));
        // Replay the last keyframe immediately so this client can decode from its very first
        // packet, whenever it happened to connect.
        pthread_mutex_lock(&_clientsMutex);
        bool primed = false;
        if (_lastKeyframe && _lastKeyframe.length) {
            bool ok = true;
            writeAll(cfd, (const uint8_t *)_lastKeyframe.bytes, _lastKeyframe.length, &ok);
            primed = ok;
        }
        _clients->push_back(cfd);
        _synced->push_back(primed);
        pthread_mutex_unlock(&_clientsMutex);
        _forceKeyframe = YES; // next frame becomes an IDR so the new client can decode immediately
        // Cold start: on a static screen the capturer delivers nothing, so we'd never encode
        // a first frame and the client would wait forever. Ask for one explicitly.
        dispatch_async(dispatch_get_main_queue(), ^{
            [[ScreenCapturer sharedCapturer] forceNextFrameUpdate];
        });
        char ip[INET_ADDRSTRLEN] = {0};
        inet_ntop(AF_INET, &cli.sin_addr, ip, sizeof(ip));
        HLog("client connected from %s (fd=%d)", ip, cfd);
    }
}

- (void)ensureSessionForWidth:(size_t)nativeW height:(size_t)nativeH {
    if (_session)
        return;
    int w = (int)lround((double)nativeW * _scale);
    int h = (int)lround((double)nativeH * _scale);
    w &= ~1;
    h &= ~1;
    if (w < 16)
        w = 16;
    if (h < 16)
        h = 16;
    _encW = w;
    _encH = h;

    NSDictionary *bufAttrs = @{
        (id)kCVPixelBufferPixelFormatTypeKey : @(kCVPixelFormatType_32BGRA),
        (id)kCVPixelBufferWidthKey : @(w),
        (id)kCVPixelBufferHeightKey : @(h),
        (id)kCVPixelBufferIOSurfacePropertiesKey : @{},
    };
    CVPixelBufferPoolCreate(kCFAllocatorDefault, NULL, (__bridge CFDictionaryRef)bufAttrs, &_pool);

    OSStatus st = VTCompressionSessionCreate(kCFAllocatorDefault, w, h, kCMVideoCodecType_H264, NULL,
                                             (__bridge CFDictionaryRef)bufAttrs, NULL, H264_OutputCallback,
                                             (__bridge void *)self, &_session);
    if (st != noErr || !_session) {
        HLog("VTCompressionSessionCreate failed: %d", (int)st);
        _session = NULL;
        return;
    }
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_RealTime, kCFBooleanTrue);
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_ProfileLevel, kVTProfileLevel_H264_Baseline_AutoLevel);
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_AllowFrameReordering, kCFBooleanFalse);

    int bps = _bitrateKbps * 1000;
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_AverageBitRate, (__bridge CFNumberRef) @(bps));
    // Cap short-term bursts to ~2x average over 1s to protect the cellular uplink.
    NSArray *limits = @[ @(bps / 8 * 2), @(1) ];
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_DataRateLimits, (__bridge CFArrayRef)limits);
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_MaxKeyFrameInterval, (__bridge CFNumberRef) @(_fps * 2));
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_MaxKeyFrameIntervalDuration, (__bridge CFNumberRef) @(2));
    VTSessionSetProperty(_session, kVTCompressionPropertyKey_ExpectedFrameRate, (__bridge CFNumberRef) @(_fps));
    VTCompressionSessionPrepareToEncodeFrames(_session);

    if (!_idleStarted) {
        _idleStarted = YES;
        _idleQueue = dispatch_queue_create("com.trollvnc.h264.idle", DISPATCH_QUEUE_SERIAL);
        _idleTimer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, _idleQueue);
        uint64_t interval = (uint64_t)((1.0 / (double)_fps) * NSEC_PER_SEC);
        dispatch_source_set_timer(_idleTimer, dispatch_time(DISPATCH_TIME_NOW, (int64_t)interval), interval,
                                  interval / 4);
        __unsafe_unretained H264Streamer *weak = self;
        dispatch_source_set_event_handler(_idleTimer, ^{
            [weak idleTick];
        });
        dispatch_resume(_idleTimer);
    }
    HLog("VT session ready: encode %dx%d (steady %d fps)", w, h, _fps);
}

- (void)doEncodeBuffer:(CVPixelBufferRef)buf force:(BOOL)force {
    pthread_mutex_lock(&_encMutex);
    if (!_session) {
        pthread_mutex_unlock(&_encMutex);
        return;
    }
    CMTime pts = CMTimeMake(_frameIndex++, _fps);
    CFDictionaryRef frameProps = NULL;
    if (force) {
        const void *keys[] = {(const void *)kVTEncodeFrameOptionKey_ForceKeyFrame};
        const void *vals[] = {(const void *)kCFBooleanTrue};
        frameProps = CFDictionaryCreate(kCFAllocatorDefault, keys, vals, 1, &kCFTypeDictionaryKeyCallBacks,
                                        &kCFTypeDictionaryValueCallBacks);
    }
    VTCompressionSessionEncodeFrame(_session, buf, pts, kCMTimeInvalid, frameProps, NULL, NULL);
    if (frameProps)
        CFRelease(frameProps);
    _lastEncTime = CACurrentMediaTime();
    if (buf != _lastBuf) {
        CVPixelBufferRetain(buf);
        if (_lastBuf)
            CVPixelBufferRelease(_lastBuf);
        _lastBuf = buf;
    }
    pthread_mutex_unlock(&_encMutex);
}

// Re-encode the last frame when the capturer goes quiet (static screen), so the
// stream keeps a steady framerate. Without this, live players lose their clock.
- (void)idleTick {
    if (!_running || !_session)
        return;
    pthread_mutex_lock(&_clientsMutex);
    bool has = !_clients->empty();
    pthread_mutex_unlock(&_clientsMutex);
    if (!has)
        return;
    // Nudge the capturer, but rate-limited: flooding the main queue starves capture itself.
    static int sNudge = 0;
    if (++sNudge % 6 == 0) {
        dispatch_async(dispatch_get_main_queue(), ^{
            [[ScreenCapturer sharedCapturer] forceNextFrameUpdate];
        });
    }

    pthread_mutex_lock(&_encMutex);
    CVPixelBufferRef buf = _lastBuf;
    double age = buf ? (CACurrentMediaTime() - _lastEncTime) : 0.0;
    if (buf)
        CVPixelBufferRetain(buf);
    pthread_mutex_unlock(&_encMutex);
    // When the screen is static, re-encode at only ~3 fps (not the full rate): keeps the
    // stream warm for the player while drastically cutting encoder/CPU heat.
    if (buf && age > 0.30) {
        BOOL force = _forceKeyframe;
        _forceKeyframe = NO;
        [self doEncodeBuffer:buf force:force];
    } else if (!buf) {
        // No frame captured yet (fully static screen): nudge the capturer so we get one.
        dispatch_async(dispatch_get_main_queue(), ^{
            [[ScreenCapturer sharedCapturer] forceNextFrameUpdate];
        });
    }
    if (buf)
        CVPixelBufferRelease(buf);
}

- (void)encodeSampleBuffer:(CMSampleBufferRef)sampleBuffer {
    if (!_running || !sampleBuffer)
        return;
    CVPixelBufferRef src = CMSampleBufferGetImageBuffer(sampleBuffer);
    if (!src)
        return;

    size_t nW = CVPixelBufferGetWidth(src);
    size_t nH = CVPixelBufferGetHeight(src);
    [self ensureSessionForWidth:nW height:nH];
    if (!_session || !_pool)
        return;

    // Skip all work when nobody is watching (saves CPU/battery/heat).
    pthread_mutex_lock(&_clientsMutex);
    bool hasClients = !_clients->empty();
    pthread_mutex_unlock(&_clientsMutex);
    if (!hasClients)
        return;

    static int sFeed = 0;
    if (++sFeed <= 5 || sFeed % 100 == 0)
        HLog("capture frame #%d fed to encoder", sFeed);

    CVPixelBufferRef dst = NULL;
    if (CVPixelBufferPoolCreatePixelBuffer(kCFAllocatorDefault, _pool, &dst) != kCVReturnSuccess || !dst)
        return;

    CVPixelBufferLockBaseAddress(src, kCVPixelBufferLock_ReadOnly);
    CVPixelBufferLockBaseAddress(dst, 0);
    vImage_Buffer vs = {CVPixelBufferGetBaseAddress(src), (vImagePixelCount)nH, (vImagePixelCount)nW,
                        CVPixelBufferGetBytesPerRow(src)};
    vImage_Buffer vd = {CVPixelBufferGetBaseAddress(dst), (vImagePixelCount)_encH, (vImagePixelCount)_encW,
                        CVPixelBufferGetBytesPerRow(dst)};
    vImageScale_ARGB8888(&vs, &vd, NULL, kvImageNoFlags);
    CVPixelBufferUnlockBaseAddress(dst, 0);
    CVPixelBufferUnlockBaseAddress(src, kCVPixelBufferLock_ReadOnly);

    BOOL force = _forceKeyframe;
    _forceKeyframe = NO;
    [self doEncodeBuffer:dst force:force];
    CVPixelBufferRelease(dst);
}

// Returns via *ok: false only for a real connection error. A slow-link timeout is reported
// through *timedOut so the caller can skip the frame instead of dropping the viewer.
static void writeAllEx(int fd, const uint8_t *buf, size_t len, bool *ok, bool *timedOut) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            if (timedOut)
                *timedOut = true; // uplink is saturated: give up on THIS frame only
            return;
        }
        *ok = false;
        return;
    }
}

static void writeAll(int fd, const uint8_t *buf, size_t len, bool *ok) {
    bool ignored = false;
    writeAllEx(fd, buf, len, ok, &ignored);
}

- (void)broadcast:(NSData *)data keyframe:(bool)isKeyframe {
    const uint8_t *bytes = (const uint8_t *)data.bytes;
    size_t len = data.length;
    pthread_mutex_lock(&_clientsMutex);
    for (size_t i = 0; i < _clients->size();) {
        // A client must start its stream on an IDR (with SPS/PPS); skip P-frames until then,
        // otherwise a live player (ffplay) chokes on "non-existing PPS" at connect time.
        if (!(*_synced)[i] && !isKeyframe) {
            _forceKeyframe = YES; // someone is still waiting: make the next frame an IDR
            static int sSkip = 0;
            if (++sSkip % 25 == 1)
                HLog("client not synced yet, waiting for keyframe (skipped %d)", sSkip);
            i++;
            continue;
        }
        int fd = (*_clients)[i];
        bool ok = true, slow = false;
        writeAllEx(fd, bytes, len, &ok, &slow);
        if (!ok) {
            HLog("client fd=%d dropped (connection error)", fd);
            close(fd);
            _clients->erase(_clients->begin() + i);
            _synced->erase(_synced->begin() + i);
        } else {
            if (slow) {
                // Partial write on a saturated uplink: the client's stream is now broken,
                // so re-sync it with a fresh keyframe instead of killing the connection.
                (*_synced)[i] = false;
                _forceKeyframe = YES;
                static int sSlow = 0;
                if (++sSlow % 10 == 1)
                    HLog("uplink saturated, skipped a frame (x%d)", sSlow);
            } else if (isKeyframe) {
                (*_synced)[i] = true;
            }
            i++;
        }
    }
    pthread_mutex_unlock(&_clientsMutex);
}

- (void)handleEncoded:(CMSampleBufferRef)sb {
    if (!sb || !CMSampleBufferDataIsReady(sb))
        return;

    // Default to "sync sample": VideoToolbox often attaches nothing for keyframes.
    bool keyframe = true;
    CFArrayRef attachments = CMSampleBufferGetSampleAttachmentsArray(sb, false);
    if (attachments && CFArrayGetCount(attachments) > 0) {
        CFDictionaryRef d = (CFDictionaryRef)CFArrayGetValueAtIndex(attachments, 0);
        if (CFDictionaryContainsKey(d, kCMSampleAttachmentKey_NotSync)) {
            CFBooleanRef notSync = (CFBooleanRef)CFDictionaryGetValue(d, kCMSampleAttachmentKey_NotSync);
            keyframe = (notSync && !CFBooleanGetValue(notSync));
        }
    }

    // Pass 1: convert AVCC -> Annex B and inspect NAL types. Type 5 (IDR) is the
    // authoritative keyframe signal; type 1 means a plain P-slice.
    NSMutableData *slices = [NSMutableData data];
    bool sawIDR = false, sawNonIDR = false;
    CMBlockBufferRef bb = CMSampleBufferGetDataBuffer(sb);
    if (bb) {
        size_t totalLen = 0;
        char *dataPtr = NULL;
        if (CMBlockBufferGetDataPointer(bb, 0, NULL, &totalLen, &dataPtr) == noErr && dataPtr) {
            size_t offset = 0;
            while (offset + 4 <= totalLen) {
                uint32_t nalLen = 0;
                memcpy(&nalLen, dataPtr + offset, 4);
                nalLen = CFSwapInt32BigToHost(nalLen);
                offset += 4;
                if (nalLen == 0 || offset + nalLen > totalLen)
                    break;
                uint8_t nalType = (uint8_t)(dataPtr[offset] & 0x1F);
                if (nalType == 5)
                    sawIDR = true;
                else if (nalType == 1)
                    sawNonIDR = true;
                [slices appendBytes:kAnnexBStart length:4];
                [slices appendBytes:(dataPtr + offset) length:nalLen];
                offset += nalLen;
            }
        }
    }
    if (sawIDR)
        keyframe = true;
    else if (sawNonIDR)
        keyframe = false;

    // Pass 2: on a keyframe, lead with SPS/PPS so any player can start decoding here.
    NSMutableData *out = [NSMutableData data];
    if (keyframe) {
        CMFormatDescriptionRef fmt = CMSampleBufferGetFormatDescription(sb);
        if (fmt) {
            size_t count = 0;
            CMVideoFormatDescriptionGetH264ParameterSetAtIndex(fmt, 0, NULL, NULL, &count, NULL);
            for (size_t i = 0; i < count; i++) {
                const uint8_t *ps = NULL;
                size_t psSize = 0;
                if (CMVideoFormatDescriptionGetH264ParameterSetAtIndex(fmt, i, &ps, &psSize, NULL, NULL) == noErr) {
                    [out appendBytes:kAnnexBStart length:4];
                    [out appendBytes:ps length:psSize];
                }
            }
        }
    }
    [out appendData:slices];

    if (!out.length)
        return;

    // Mux the access unit into MPEG-TS (PAT/PMT before each keyframe).
    _pts90 += 90000 / (_fps > 0 ? _fps : 20);
    NSMutableData *ts = [NSMutableData dataWithCapacity:out.length + 1024];
    if (keyframe) {
        uint8_t pat[188], pmt[188];
        tsBuildPAT(pat, _ccPat);
        _ccPat = (_ccPat + 1) & 0x0F;
        tsBuildPMT(pmt, _ccPmt);
        _ccPmt = (_ccPmt + 1) & 0x0F;
        [ts appendBytes:pat length:188];
        [ts appendBytes:pmt length:188];
    }
    tsAppendPES(ts, (const uint8_t *)out.bytes, out.length, _pts90, keyframe, &_ccVid);

    static int sEnc = 0;
    if (++sEnc <= 10 || sEnc % 50 == 0)
        HLog("encoded #%d: %zu bytes AU -> %zu TS, keyframe=%d (idr=%d p=%d)", sEnc, (size_t)out.length,
             (size_t)ts.length, (int)keyframe, (int)sawIDR, (int)sawNonIDR);

    if (ts.length) {
        if (keyframe) {
            pthread_mutex_lock(&_clientsMutex);
            _lastKeyframe = [ts copy];
            pthread_mutex_unlock(&_clientsMutex);
        }
        [self broadcast:ts keyframe:keyframe];
    }
}

- (void)stop {
    if (!_running)
        return;
    _running = NO;
    if (_listenFd >= 0) {
        close(_listenFd);
        _listenFd = -1;
    }
    pthread_mutex_lock(&_clientsMutex);
    for (int fd : *_clients)
        close(fd);
    _clients->clear();
    _synced->clear();
    pthread_mutex_unlock(&_clientsMutex);
    if (_session) {
        VTCompressionSessionCompleteFrames(_session, kCMTimeInvalid);
        VTCompressionSessionInvalidate(_session);
        CFRelease(_session);
        _session = NULL;
    }
    if (_idleTimer) {
        dispatch_source_cancel(_idleTimer);
        _idleTimer = NULL;
    }
    pthread_mutex_lock(&_encMutex);
    if (_lastBuf) {
        CVPixelBufferRelease(_lastBuf);
        _lastBuf = NULL;
    }
    pthread_mutex_unlock(&_encMutex);
    if (_pool) {
        CVPixelBufferPoolRelease(_pool);
        _pool = NULL;
    }
}

@end

static void H264_OutputCallback(void *outputCallbackRefCon, void *sourceFrameRefCon, OSStatus status,
                                VTEncodeInfoFlags infoFlags, CMSampleBufferRef sampleBuffer) {
    if (status != noErr || !sampleBuffer)
        return;
    H264Streamer *self = (__bridge H264Streamer *)outputCallbackRefCon;
    [self handleEncoded:sampleBuffer];
}
