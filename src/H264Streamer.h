/*
 This file is part of TrollVNC
 Copyright (c) 2025 contributors

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2
 as published by the Free Software Foundation.
*/

#ifndef H264Streamer_h
#define H264Streamer_h

#import <CoreMedia/CoreMedia.h>
#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/**
 H264Streamer
 ------------
 Hardware H.264 (VideoToolbox) encoder + minimal TCP server.
 Consumes the BGRA CVPixelBuffer sample buffers produced by ScreenCapturer,
 scales them by `scale`, encodes to H.264 (Annex B elementary stream) and
 broadcasts the NALUs to every connected TCP client.

 A PC-side player connects with e.g.:
   ffplay -fflags nobuffer -flags low_delay -f h264 tcp://<device-ip>:<port>

 Control (touch/keyboard) stays on the existing RFB path; this class is
 video-only. Designed for low-upstream links (cellular): inter-frame
 compression gives far smoother motion than per-frame JPEG.
 */
@interface H264Streamer : NSObject

+ (instancetype)sharedStreamer;

/** Start the TCP listener. VT session is created lazily on the first frame. */
- (BOOL)startOnPort:(int)port scale:(double)scale fps:(int)fps bitrateKbps:(int)kbps;

/** Feed one captured frame (BGRA CVPixelBuffer inside). No-op if not running or no clients. */
- (void)encodeSampleBuffer:(CMSampleBufferRef)sampleBuffer;

/** Stop the server and release the encoder. */
- (void)stop;

@property(nonatomic, readonly) BOOL isRunning;

@end

NS_ASSUME_NONNULL_END

#endif /* H264Streamer_h */
