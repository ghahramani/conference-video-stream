package com.uncutfarsi.streaming.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy
import reactor.netty.http.server.WebsocketServerSpec

@Configuration
class WebSocketConfig {

    @Bean
    fun handlerMapping(streamHandler: ReactiveStreamHandler): HandlerMapping {
        val map = mapOf("/publish/**" to streamHandler)
        val mapping = SimpleUrlHandlerMapping()
        mapping.urlMap = map
        mapping.order = -1 // High priority

        val cors = CorsConfiguration()
        cors.addAllowedOrigin("*")
        cors.addAllowedHeader("*")
        cors.addAllowedMethod("*")
        mapping.setCorsConfigurations(mapOf("/publish/**" to cors))

        return mapping
    }

    @Bean
    fun handlerAdapter(): WebSocketHandlerAdapter {
        // Explicitly set the frame limit to 5MB (5 * 1024 * 1024)
        val limit = 5 * 1024 * 1024

        val upgradeStrategy = ReactorNettyRequestUpgradeStrategy {
            WebsocketServerSpec.builder()
                .maxFramePayloadLength(limit)
                .handlePing(true)
        }

        val webSocketService = HandshakeWebSocketService(upgradeStrategy)
        return WebSocketHandlerAdapter(webSocketService)
    }
}