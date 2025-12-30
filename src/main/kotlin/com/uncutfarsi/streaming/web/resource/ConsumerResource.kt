package com.uncutfarsi.streaming.web.resource

import com.uncutfarsi.streaming.domain.VideoQuality
import com.uncutfarsi.streaming.service.StreamDispatcherService
import com.uncutfarsi.streaming.web.restmodel.VideoFrameResponseRestModel
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import java.util.*

@RestController
class ConsumerResource(private val service: StreamDispatcherService) {

    @GetMapping("/watch/{streamId}", produces = [MediaType.APPLICATION_NDJSON_VALUE])
    fun watchStream(
        @PathVariable streamId: String,
        @RequestParam(defaultValue = "HIGH") quality: VideoQuality
    ): Flux<VideoFrameResponseRestModel> = service
        .retrieveStream(streamId)
        .filter { frame -> frame.quality == quality }
        .onBackpressureDrop()
        .map { frame ->
            VideoFrameResponseRestModel(
                pId = frame.pId,
                dataBase64 = Base64.getEncoder().encodeToString(frame.data),
                quality = frame.quality
            )
        }

}