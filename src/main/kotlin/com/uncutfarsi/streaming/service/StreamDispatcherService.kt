package com.uncutfarsi.streaming.service

import com.uncutfarsi.streaming.domain.VideoFrame
import com.uncutfarsi.streaming.domain.VideoQuality
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.concurrent.ConcurrentHashMap

@Service
class StreamDispatcherService {

    private val processors = ConcurrentHashMap<String, Sinks.Many<VideoFrame>>()
    private val streamHeaders = ConcurrentHashMap<String, VideoFrame>()
    private val lastHeartbeat = ConcurrentHashMap<String, Long>()

    fun dispatch(streamId: String, pId: String, quality: VideoQuality, data: DataBuffer) {
        val bytes = ByteArray(data.readableByteCount())
        data.read(bytes) // releases the Netty buffer reading position

        val frame = VideoFrame(pId, bytes, quality)
        val key = "$streamId:$pId"

        // Update Heartbeat & Header
        lastHeartbeat[key] = System.currentTimeMillis()
        streamHeaders.putIfAbsent(key, frame)

        // This allows the sink to drop frames for slow subscribers
        // without killing the entire stream.
        val sink = processors.computeIfAbsent(streamId) {
            Sinks.many().multicast().directBestEffort()
        }

        // 'tryEmitNext' will return FAIL_OVERFLOW instead of throwing an Exception.
        // We simply ignore the failure, meaning we drop the frame safely.
        sink.tryEmitNext(frame)
    }

    fun retrieveStream(streamId: String): Flux<VideoFrame> {
        val cachedHeaders = streamHeaders.filterKeys { it.startsWith("$streamId:") }.values

        val liveStream = processors
            .computeIfAbsent(streamId) { Sinks.many().multicast().directBestEffort() }
            .asFlux()
            // Extra safety: If the HTTP connection itself is blocked, drop packets immediately
            .onBackpressureDrop()

        return Flux
            .fromIterable(cachedHeaders)
            .concatWith(liveStream)
    }

    @Scheduled(fixedRate = 2000)
    fun cleanUpInactivePresenters() {
        val now = System.currentTimeMillis()
        val timeout = 20000L

        lastHeartbeat.entries.removeIf { (key, lastSeen) ->
            if (now - lastSeen > timeout) {
                streamHeaders.remove(key)
                println("Cleaning up inactive stream: $key")
                true
            } else {
                false
            }
        }
    }

}