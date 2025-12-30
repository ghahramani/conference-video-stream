package com.uncutfarsi.streaming.config

import com.uncutfarsi.streaming.domain.VideoQuality
import com.uncutfarsi.streaming.service.StreamDispatcherService
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import java.util.*

@Component
class ReactiveStreamHandler(private val service: StreamDispatcherService) : WebSocketHandler {

    override fun handle(session: WebSocketSession): Mono<Void> {
        // 1. Parse all 3 required parameters from the URL
        // Example URL: ws://localhost:8080/publish/{streamId}/{pId}/{quality}
        val streamId = parseStreamId(session)
        val pId = parseParticipantId(session)
        val quality = parseQuality(session)

        return session
            .receive()
            .map(WebSocketMessage::getPayload)
            .doOnNext { buffer ->
                // 2. Dispatch to the service
                // Note: The service now handles DataBufferUtils.retain(buffer) internally
                service.dispatch(
                    streamId = streamId,
                    pId = pId,
                    quality = quality,
                    data = buffer
                )
            }
            .then()
    }

    private fun parseStreamId(session: WebSocketSession): String {
        val path = session.handshakeInfo.uri.path
        val parts = path.split("/")
        // URL path starts with empty string, so parts[0] is "", parts[1] is "publish"
        return if (parts.size >= 3) parts[2] else UUID.randomUUID().toString()
    }

    private fun parseParticipantId(session: WebSocketSession): String {
        val path = session.handshakeInfo.uri.path
        val parts = path.split("/")
        return if (parts.size >= 4) parts[3] else UUID.randomUUID().toString()
    }

    private fun parseQuality(session: WebSocketSession): VideoQuality {
        val path = session.handshakeInfo.uri.path
        val parts = path.split("/")

        // Default to HIGH if not specified
        if (parts.size >= 5) {
            return try {
                VideoQuality.valueOf(parts[4].uppercase())
            } catch (e: IllegalArgumentException) {
                VideoQuality.HIGH
            }
        }
        return VideoQuality.HIGH
    }
}