package com.uncutfarsi.streaming

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class StreamingApplication

fun main(args: Array<String>) {
    runApplication<StreamingApplication>(*args)
}
