package com.uncutfarsi.streaming.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter

@Configuration
class WebSocketConfig {

    @Bean
    fun handlerMapping(streamHandler: ReactiveStreamHandler): HandlerMapping {
        val map = mapOf("/publish/**" to streamHandler)
        val mapping = SimpleUrlHandlerMapping()
        mapping.urlMap = map

        // HIGHEST PRIORITY ensures this mapping is checked before default controllers
        mapping.order = -1

        // Explicit CORS for the WebSocket Handshake
        val corsConfig = CorsConfiguration()
        corsConfig.addAllowedOrigin("*") // Allow all origins
        corsConfig.addAllowedHeader("*")
        corsConfig.addAllowedMethod("*")

        mapping.setCorsConfigurations(mapOf("/publish/**" to corsConfig))

        return mapping
    }

    @Bean
    fun handlerAdapter(): WebSocketHandlerAdapter {
        return WebSocketHandlerAdapter()
    }
}