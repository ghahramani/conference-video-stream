package com.uncutfarsi.streaming.web.restmodel

import com.uncutfarsi.streaming.domain.VideoQuality

data class VideoFrameResponseRestModel(
    val pId: String,
    val dataBase64: String,
    val quality: VideoQuality
)