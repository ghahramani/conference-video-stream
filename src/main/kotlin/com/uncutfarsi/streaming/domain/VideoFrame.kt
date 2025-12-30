package com.uncutfarsi.streaming.domain

// VideoFrame.kt
data class VideoFrame(
    val pId: String,
    val data: ByteArray,
    val quality: VideoQuality
) {
    // Generated equals/hashCode is recommended for ByteArray in data classes
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as VideoFrame
        return pId == other.pId && data.contentEquals(other.data) && quality == other.quality
    }

    override fun hashCode(): Int {
        var result = pId.hashCode()
        result = 31 * result + data.contentHashCode()
        result = 31 * result + quality.hashCode()
        return result
    }
}
