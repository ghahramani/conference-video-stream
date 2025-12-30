// ==========================================
// 1. GLOBAL CONFIG
// ==========================================
const CONFIG = {
    chunkInterval: 100,      // 100ms = smoother stream, less bursty
    watchdogTimeout: 30000,  // 30s = very tolerant of lag

    // FALLBACK ONLY: If negotiation fails, we try this.
    // Note: We removed 'opus' to prevent audio mismatches crashing the video.
    defaultCodec: 'video/webm; codecs=vp8'
};

let streamId = "";
let myPId = "user-" + Math.floor(Math.random() * 100000);
let isPresenter = false;
let isWatching = false;

// Logic State
const activePresenters = new Set();
const lastPacketTime = {};
const stuckMonitors = {};
const presenterCodecs = {}; // Store the EXACT codec for each user

// Media State
const mediaBuffers = {};
const mediaSources = {};
const mediaQueues = {};
const mediaSourceReady = {};

// ==========================================
// 2. HEALTH MONITORS
// ==========================================
setInterval(() => {
    const now = Date.now();
    activePresenters.forEach(pId => {
        if (pId === myPId) return;

        // WATCHDOG
        const lastSeen = lastPacketTime[pId] || 0;
        if (now - lastSeen > CONFIG.watchdogTimeout) {
            console.error(`❌ User ${pId} TIMED OUT. Removing.`);
            removePresenter(pId);
        }

        // VIDEO CPR (Anti-Freeze)
        const video = document.getElementById(`video-${pId}`);
        const sb = mediaBuffers[pId];
        if (video && !video.paused && sb && sb.buffered.length > 0) {
            if (!stuckMonitors[pId]) stuckMonitors[pId] = { last: 0, count: 0 };
            const m = stuckMonitors[pId];

            if (Math.abs(video.currentTime - m.last) < 0.1) m.count++;
            else { m.count = 0; m.last = video.currentTime; }

            if (m.count > 5) { // 5 seconds frozen
                console.warn(`⚡ Jumpstarting frozen video: ${pId}`);
                video.currentTime = sb.buffered.end(sb.buffered.length - 1) - 0.1;
                m.count = 0;
            }
        }
    });
}, 1000);

function removePresenter(pId) {
    if (!activePresenters.has(pId)) return;
    activePresenters.delete(pId);
    document.getElementById(`video-${pId}`)?.closest('.video-card')?.remove();

    delete mediaBuffers[pId];
    delete mediaQueues[pId];
    delete mediaSources[pId];
    delete lastPacketTime[pId];
    delete stuckMonitors[pId];
    delete presenterCodecs[pId];
    updateGridLayout();
}

// ==========================================
// 3. JOIN LOGIC
// ==========================================
function joinAsWatcher() {
    const input = document.getElementById('streamInput').value;
    if(!input) return alert("Enter Room Name");
    streamId = input;
    document.getElementById('login-overlay').style.display = 'none';
    monitorConnection();
}

async function joinAsPresenter() {
    const input = document.getElementById('streamInput').value;
    if(!input) return alert("Enter Room Name");
    streamId = input;
    isPresenter = true;
    document.getElementById('login-overlay').style.display = 'none';

    await startPublishing();
    monitorConnection();
}

// ==========================================
// 4. PRESENTER (INGEST)
// ==========================================
async function startPublishing() {
    preventBackgroundThrottling();

    try {
        // MOBILE OPTIMIZATION
        const constraints = {
            audio: true,
            video: {
                width: { ideal: 640, max: 1280 }, // Lower res = More reliable on phone
                height: { ideal: 480, max: 720 },
                frameRate: { ideal: 20, max: 30 }
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        // Local Preview
        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true;

        // WebSocket
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH`;
        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("🎥 PRESENTER: Connected");

            // ----------------------------------------------------
            // 1. AUTO-DETECT CODEC
            // We let the browser pick what it likes best.
            // ----------------------------------------------------
            let options = { videoBitsPerSecond: 1000000 };

            // Try specific codecs in order of preference
            if (MediaRecorder.isTypeSupported('video/webm; codecs="vp8, opus"')) {
                options.mimeType = 'video/webm; codecs="vp8, opus"';
            } else if (MediaRecorder.isTypeSupported('video/webm; codecs=vp8')) {
                options.mimeType = 'video/webm; codecs=vp8';
            } else if (MediaRecorder.isTypeSupported('video/webm')) {
                options.mimeType = 'video/webm';
            } else {
                console.warn("Using default browser codec (might be mp4/h264)");
            }

            console.log(`🎙️ Recording using: ${options.mimeType || "default"}`);

            const recorder = new MediaRecorder(stream, options);

            // ----------------------------------------------------
            // 2. SEND METADATA FIRST
            // We send a text message with the MIME type so watchers know what to expect.
            // ----------------------------------------------------
            const meta = { type: 'META', mimeType: recorder.mimeType || "" };
            socket.send(new TextEncoder().encode(JSON.stringify(meta)));

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {

                    // Client-Side Backpressure (Prevent crash)
                    if (socket.bufferedAmount > 256 * 1024) { // 256KB buffer limit
                        console.warn(`🐢 Dropping frame. Buffer: ${socket.bufferedAmount}`);
                        return;
                    }
                    socket.send(await event.data.arrayBuffer());
                }
            };

            recorder.start(CONFIG.chunkInterval);

            // Keyframe every 1s (Faster recovery from black screen)
            setInterval(() => {
                if(recorder.state === "recording") recorder.requestData();
            }, 1000);
        };

        socket.onclose = () => setTimeout(startPublishing, 2000);

    } catch (err) {
        alert("Camera Failed: " + err.message);
    }
}

function preventBackgroundThrottling() {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    gain.gain.value = 0;
    osc.start();
}

// ==========================================
// 5. WATCHER (EGRESS)
// ==========================================
async function monitorConnection() {
    if (isWatching) return;
    isWatching = true;

    while (isWatching) {
        try {
            await startWatching();
        } catch (err) { console.error("Watcher retry:", err); }
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const response = await fetch(`/watch/${streamId}?quality=HIGH`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;

            let jsonString = trimmed.startsWith("data:") ? trimmed.substring(5) : trimmed;
            try {
                // Check if it's the raw string (JSON) or something else
                const frame = JSON.parse(jsonString);
                handleIncomingFrame(frame);
            } catch (e) { }
        }
    }
}

// ==========================================
// 6. FRAME PROCESSING
// ==========================================
function handleIncomingFrame(frameDTO) {
    if (frameDTO.pId === myPId) return;
    lastPacketTime[frameDTO.pId] = Date.now();

    // ------------------------------------------------
    // 1. HANDLE METADATA (MIME TYPE)
    // ------------------------------------------------
    // If the server passes the JSON we sent earlier, we might catch it here.
    // NOTE: In your current backend, you wrap bytes in VideoFrame.
    // If we sent text earlier, the backend tries to treat it as video.
    // Simplification: We will try to sniff the first bytes or just use the CONFIG fallback.

    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false);
    }

    const binaryString = window.atob(frameDTO.dataBase64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }

    // Check if this is our META packet (JSON text inside binary)
    // "{"type":"META"..." is {123, 34, 116, 121...}
    if (bytes[0] === 123 && bytes[1] === 34) {
        try {
            const text = new TextDecoder().decode(bytes);
            const meta = JSON.parse(text);
            if (meta.type === 'META') {
                console.log(`🎥 Received Codec for ${frameDTO.pId}: ${meta.mimeType}`);
                presenterCodecs[frameDTO.pId] = meta.mimeType;
                return; // Don't append this to buffer
            }
        } catch(e) {}
    }

    if (!mediaQueues[frameDTO.pId]) mediaQueues[frameDTO.pId] = [];
    mediaQueues[frameDTO.pId].push(bytes);
    processQueue(frameDTO.pId);
}

function processQueue(pId) {
    const sb = mediaBuffers[pId];
    const queue = mediaQueues[pId];
    const ms = mediaSources[pId];

    if (!sb || sb.updating || !queue || queue.length === 0 || !ms || ms.readyState !== 'open') return;

    try {
        const nextChunk = queue.shift();
        sb.appendBuffer(nextChunk);
    } catch (e) {
        // Quota: Clean up
        if (e.name === 'QuotaExceededError') {
            const video = document.getElementById(`video-${pId}`);
            if (sb.buffered.length > 0) {
                try { sb.remove(0, video.currentTime - 2); } catch(ex) {}
            }
        }
        // InvalidState: Usually means header/codec mismatch.
        // We drop the chunk.
    }
}

// ==========================================
// 7. UI LOGIC
// ==========================================
function createVideoTile(pId, isLocal) {
    if (activePresenters.size >= 4) return;
    activePresenters.add(pId);

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';
    card.innerHTML = `
        <div style="position: relative; width: 100%; height: 100%;">
            <video id="video-${pId}" autoplay playsinline muted style="width:100%; height:100%; object-fit: cover; background: #000;"></video>
            ${!isLocal ? `<button id="btn-${pId}" style="position: absolute; top: 10px; right: 10px; z-index: 10; padding: 5px;">🔇</button>` : ''}
        </div>
        <div class="label">${isLocal ? "Me" : pId}</div>
    `;
    grid.appendChild(card);
    updateGridLayout();

    if (!isLocal) {
        const video = card.querySelector('video');
        const btn = card.querySelector(`#btn-${pId}`);
        if(btn) btn.onclick = () => { video.muted = !video.muted; btn.innerText = video.muted ? "🔇" : "🔊"; };

        const ms = new MediaSource();
        video.src = URL.createObjectURL(ms);
        mediaSources[pId] = ms;

        ms.onsourceopen = () => {
            // USE NEGOTIATED CODEC OR FALLBACK
            // If we received the META packet, use it. Otherwise use generic VP8.
            let mime = presenterCodecs[pId] || CONFIG.defaultCodec;

            // Safety: If the mime type is empty or invalid, fallback
            if (!mime || mime === "") mime = 'video/webm; codecs=vp8';

            console.log(`⚙️ Initializing SourceBuffer for ${pId} with: ${mime}`);

            if (MediaSource.isTypeSupported(mime)) {
                const sb = ms.addSourceBuffer(mime);
                sb.mode = 'sequence';
                mediaBuffers[pId] = sb;

                sb.addEventListener('updateend', () => processQueue(pId));
                if (mediaQueues[pId]?.length > 0) processQueue(pId);
            } else {
                console.error(`Browser cannot play this format: ${mime}`);
                // Attempt ultra-basic fallback
                try {
                    const sb = ms.addSourceBuffer('video/webm');
                    mediaBuffers[pId] = sb;
                    sb.addEventListener('updateend', () => processQueue(pId));
                } catch(e) { console.error("Fallback failed", e); }
            }
        };
    }
}

function updateGridLayout() {
    const count = activePresenters.size;
    const grid = document.getElementById('video-grid');
    grid.className = '';
    grid.classList.add(`grid-${Math.min(count, 4)}`);
}