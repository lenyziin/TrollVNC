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
    double _lastEncTime;
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
        struct timeval sndto = {2, 0}; // 2s send timeout: drop a stuck client instead of freezing the broadcast
        setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &sndto, sizeof(sndto));
        pthread_mutex_lock(&_clientsMutex);
        _clients->push_back(cfd);
        _synced->push_back(false);
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

static void writeAll(int fd, const uint8_t *buf, size_t len, bool *ok) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) {
            *ok = false;
            return;
        }
        sent += (size_t)n;
    }
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
        bool ok = true;
        writeAll(fd, bytes, len, &ok);
        if (!ok) {
            HLog("client fd=%d dropped", fd);
            close(fd);
            _clients->erase(_clients->begin() + i);
            _synced->erase(_synced->begin() + i);
        } else {
            if (isKeyframe)
                (*_synced)[i] = true;
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

    static int sEnc = 0;
    if (++sEnc <= 10 || sEnc % 50 == 0)
        HLog("encoded #%d: %zu bytes, keyframe=%d (idr=%d p=%d)", sEnc, (size_t)out.length, (int)keyframe, (int)sawIDR,
             (int)sawNonIDR);

    if (out.length)
        [self broadcast:out keyframe:keyframe];
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
