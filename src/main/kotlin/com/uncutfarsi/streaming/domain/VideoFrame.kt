package com.uncutfarsi.streaming.domain

import org.springframework.core.io.buffer.DataBuffer

data class VideoFrame(
    val pId: String,
    val data: DataBuffer,
    val quality: VideoQuality
)
