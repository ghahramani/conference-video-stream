package com.uncutfarsi.streaming.service

import com.uncutfarsi.streaming.domain.VideoFrame
import com.uncutfarsi.streaming.domain.VideoQuality
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.core.io.buffer.DataBufferUtils
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.concurrent.ConcurrentHashMap

@Service
class StreamDispatcherService {

    private val processors = ConcurrentHashMap<String, Sinks.Many<VideoFrame>>()

    // Cache for the header (First frame) of each pId
    private val streamHeaders = ConcurrentHashMap<String, VideoFrame>()

    fun dispatch(streamId: String, pId: String, quality: VideoQuality, data: DataBuffer) {
        // 1. RETAIN: Increment reference count so Netty doesn't release memory
        // while we are still processing it.
        DataBufferUtils.retain(data)

        // 2. SLICE: Create a view of the data for this frame.
        // 'readPosition()' ensures we read from where the producer left off.
        val slicedData = data.slice(data.readPosition(), data.readableByteCount())

        val frame = VideoFrame(pId = pId, data = slicedData, quality = quality)

        // 3. Header Logic: Cache the first frame (Initialization Segment)
        streamHeaders.computeIfAbsent("$streamId:$pId") {
            // If we cache it long-term, we must RETAIN it again effectively
            // (The first retain covers us, but slicing shares the ref count)
            // For a robust implementation, it is often safer to COPY the header
            // since it stays in memory forever.
            val headerCopy = data.factory().allocateBuffer(data.readableByteCount())
            headerCopy.write(slicedData.slice(0, slicedData.readableByteCount()))
            VideoFrame(pId, headerCopy, quality)
        }

        // 4. Multicast
        processors
            .computeIfAbsent(streamId) { Sinks.many().multicast().onBackpressureBuffer() }
            .emitNext(frame, Sinks.EmitFailureHandler.FAIL_FAST)
    }

    fun retrieveStream(streamId: String): Flux<VideoFrame> {
        val cachedHeaders = streamHeaders.filterKeys { it.startsWith("$streamId:") }.values

        val liveStream = processors
            .computeIfAbsent(streamId) { Sinks.many().multicast().onBackpressureBuffer() }
            .asFlux()

        return Flux.fromIterable(cachedHeaders)
            .concatWith(liveStream)
            .onBackpressureDrop { droppedFrame ->
                // IMPORTANT: If we drop a frame due to backpressure,
                // we must release its memory to avoid leaks!
                DataBufferUtils.release(droppedFrame.data)
            }
            .doOnDiscard(VideoFrame::class.java) { discardedFrame ->
                // Safety net: Release memory if Reactor discards the object
                DataBufferUtils.release(discardedFrame.data)
            }
    }

}