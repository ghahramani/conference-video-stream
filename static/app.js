// ==========================================
// 1. GLOBAL CONFIGURATION
// ==========================================
const CONFIG = {
    chunkInterval: 100,      // 100ms chunks = Low Latency
    watchdogTimeout: 2000,  // 2s timeout before removing a disconnected user
};

// Global State
let streamId = "";
let myPId = "user-" + Math.floor(Math.random() * 100000);
let isPresenter = false;
let isWatching = false;

// Logic State
const activePresenters = new Set();
const lastPacketTime = {};

// Media State
const mediaBuffers = {};   // pId -> SourceBuffer
const mediaSources = {};   // pId -> MediaSource
const mediaQueues = {};    // pId -> Array<Uint8Array>

// ==========================================
// 2. WATCHDOG & CLEANUP
// ==========================================
// Removes users who haven't sent data in 20 seconds
setInterval(() => {
    const now = Date.now();
    activePresenters.forEach(pId => {
        if (pId === myPId) return;

        const lastSeen = lastPacketTime[pId] || 0;
        if (now - lastSeen > CONFIG.watchdogTimeout) {
            console.error(`❌ User ${pId} TIMED OUT.`);
            removePresenter(pId);
        }
    });
}, 2000);

function removePresenter(pId) {
    if (!activePresenters.has(pId)) return;
    activePresenters.delete(pId);

    // Remove UI
    const videoEl = document.getElementById(`video-${pId}`);
    if (videoEl) videoEl.closest('.video-card')?.remove();

    // Clear Memory
    delete mediaBuffers[pId];
    delete mediaQueues[pId];
    delete mediaSources[pId];
    delete lastPacketTime[pId];

    updateGridLayout();
}

// ==========================================
// 3. JOIN LOGIC
// ==========================================
function joinAsWatcher() {
    const input = document.getElementById('streamInput').value;
    if (!input) return alert("Enter Room Name");
    streamId = input;
    document.getElementById('login-overlay').style.display = 'none';
    monitorConnection();
}

async function joinAsPresenter() {
    const input = document.getElementById('streamInput').value;
    if (!input) return alert("Enter Room Name");
    streamId = input;
    isPresenter = true;
    document.getElementById('login-overlay').style.display = 'none';

    await startPublishing();
    monitorConnection(); // Presenters also watch others
}

// ==========================================
// 4. PRESENTER (SENDING VIDEO)
// ==========================================
async function startPublishing() {
    preventBackgroundThrottling();

    try {
        // 1. Get Camera & Mic
        const constraints = {
            audio: true,
            video: {
                width: {ideal: 640, max: 1280},
                height: {ideal: 480, max: 720},
                frameRate: {ideal: 24, max: 30}
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        // Show Local Preview
        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true; // Always mute self

        // 2. Connect WebSocket
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH`;

        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("🎥 PRESENTER: Connected");

            // 3. Setup Recorder
            // We do NOT hardcode mimeType. We let the browser pick (VP8 for Chrome, H.264 for iOS).
            const options = {videoBitsPerSecond: 1000000};
            const recorder = new MediaRecorder(stream, options);

            console.log(`🎙️ Recording using: ${recorder.mimeType}`);

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {
                    // Backpressure check (skip frame if network is choked)
                    if (socket.bufferedAmount > 64 * 1024) return;

                    socket.send(await event.data.arrayBuffer());
                }
            };

            // Start recording (fires dataavailable every 100ms)
            recorder.start(CONFIG.chunkInterval);

            // 4. KEYFRAME GENERATOR (The Late Joiner Fix)
            // Forces a full picture every 1 second so new watchers don't wait long.
            setInterval(() => {
                if (recorder.state === "recording") recorder.requestData();
            }, 1000);
        };

        socket.onclose = () => {
            console.warn("⚠️ Socket closed. Reconnecting in 2s...");
            setTimeout(startPublishing, 2000);
        };

    } catch (err) {
        alert("Camera Failed: " + err.message);
        console.error(err);
    }
}

// Hack to keep tab active in background
function preventBackgroundThrottling() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        gain.gain.value = 0;
        osc.start();
    } catch (e) {
    }
}

// ==========================================
// 5. WATCHER (RECEIVING VIDEO)
// ==========================================
async function monitorConnection() {
    if (isWatching) return;
    isWatching = true;

    while (isWatching) {
        try {
            await startWatching();
        } catch (err) {
            console.error("Watcher Error:", err);
        }
        console.log("♻️ Reconnecting Watcher...");
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const response = await fetch(`/watch/${streamId}?quality=HIGH`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
        const {value, done} = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            try {
                // Parse "data: {...}"
                const jsonStr = trimmed.startsWith("data:") ? trimmed.substring(5) : trimmed;
                const frame = JSON.parse(jsonStr);
                handleIncomingFrame(frame);
            } catch (e) {
            }
        }
    }
}

function handleIncomingFrame(frameDTO) {
    if (frameDTO.pId === myPId) return;

    // Update Watchdog
    lastPacketTime[frameDTO.pId] = Date.now();

    // Create Tile if not exists
    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false);
    }

    // Decode Base64
    const binaryString = window.atob(frameDTO.dataBase64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }

    // 1. LAZY INIT: If we haven't set up the player for this user yet, do it now.
    // This expects the FIRST packet to be the Header (thanks to Rust fix).
    if (!mediaBuffers[frameDTO.pId]) {
        setupPlayerBruteForce(frameDTO.pId, bytes);
    }

    // 2. Queue Data
    if (!mediaQueues[frameDTO.pId]) mediaQueues[frameDTO.pId] = [];
    mediaQueues[frameDTO.pId].push(bytes);

    processQueue(frameDTO.pId);
}

// ==========================================
// 6. CODEC BRUTE FORCE (The Magic Fix)
// ==========================================
function setupPlayerBruteForce(pId, firstChunk) {
    const ms = mediaSources[pId];
    if (!ms || ms.readyState !== 'open') return;

    // Debug: Print Magic Bytes
    const hex = Array.from(firstChunk.slice(0, 8)).map(b => b.toString(16).padStart(2, '0')).join(' ');
    console.log(`🔍 Init Player ${pId}. Magic Bytes: [ ${hex} ]`);

    // List of codecs to try (in order of likelihood)
    const candidates = [
        'video/webm; codecs="vp8, opus"',           // Standard WebM (Chrome/Android)
        'video/mp4; codecs="avc1.42E01E, mp4a.40.2"', // Standard MP4 (iOS/Safari)
        'video/webm',                               // Generic WebM
        'video/mp4'                                 // Generic MP4
    ];

    let sb = null;

    for (const mime of candidates) {
        if (MediaSource.isTypeSupported(mime)) {
            try {
                // Attempt to create SourceBuffer with this codec
                sb = ms.addSourceBuffer(mime);
                console.log(`✅ ${pId} assigned codec: ${mime}`);
                break; // It worked! Stop trying others.
            } catch (e) {
                console.warn(`❌ Browser rejected ${mime}`);
            }
        }
    }

    if (sb) {
        sb.mode = 'sequence';
        mediaBuffers[pId] = sb;
        sb.addEventListener('updateend', () => processQueue(pId));
    } else {
        console.error("💥 CRITICAL: Browser could not play this stream format.");
    }
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
        console.error("Append Error:", e);
        // If buffer is full, remove old video to make space
        if (e.name === 'QuotaExceededError') {
            const video = document.getElementById(`video-${pId}`);
            try {
                sb.remove(0, video.currentTime - 1);
            } catch (ex) {
            }
        }
    }
}

// ==========================================
// 7. UI HELPER
// ==========================================
function createVideoTile(pId, isLocal) {
    if (activePresenters.size >= 4) return;
    activePresenters.add(pId);

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';

    // Add "Unmute" button for remote streams (Autoplay requires mute initially)
    card.innerHTML = `
        <div style="position: relative; width: 100%; height: 100%;">
            <video id="video-${pId}" autoplay playsinline muted style="width:100%; height:100%; object-fit: cover; background: #000;"></video>
            ${!isLocal ? `<button id="btn-${pId}" style="position: absolute; top: 10px; right: 10px; z-index: 10; padding: 5px 10px;">🔇 Unmute</button>` : ''}
        </div>
        <div class="label">${isLocal ? "Me" : pId}</div>
    `;
    grid.appendChild(card);
    updateGridLayout();

    // Setup MediaSource for remote user
    if (!isLocal) {
        const video = card.querySelector('video');
        const btn = card.querySelector(`#btn-${pId}`);

        // Unmute Logic
        if (btn) btn.onclick = () => {
            video.muted = !video.muted;
            btn.innerText = video.muted ? "🔇 Unmute" : "🔊 On";
        };

        const ms = new MediaSource();
        video.src = URL.createObjectURL(ms);
        mediaSources[pId] = ms;
        // Note: We do NOT addSourceBuffer here. We wait for the first packet to detect the codec.
    }
}

function updateGridLayout() {
    const count = activePresenters.size;
    const grid = document.getElementById('video-grid');
    grid.className = '';
    grid.classList.add(`grid-${Math.min(count, 4)}`);
}