// ==========================================
// 1. GLOBAL CONFIGURATION
// ==========================================
const CONFIG = {
    // 250ms chunks: Best balance for mobile upload & latency
    chunkInterval: 250,

    // 20s timeout: Tolerates bad 4G/5G connections without removing the user
    watchdogTimeout: 10000,

    // Codec: Using generic VP8 allows the browser to auto-detect Opus audio.
    // Specifying "opus" explicitly sometimes breaks audio on specific Androids/iOS.
    codec: 'video/webm; codecs=vp8'
};

// Global State
let streamId = "";
let myPId = "user-" + Math.floor(Math.random() * 100000);
let isPresenter = false;
let isWatching = false;

// Logic State
const activePresenters = new Set();
const lastPacketTime = {};
const stuckMonitors = {}; // Tracks frozen video state

// Media State
const mediaBuffers = {};   // pId -> SourceBuffer
const mediaSources = {};   // pId -> MediaSource
const mediaQueues = {};    // pId -> Array<Uint8Array>
const mediaSourceReady = {};

// ==========================================
// 2. HEALTH MONITORS (AUTO-REPAIR)
// ==========================================

// A. WATCHDOG: Removes users who have truly disconnected (>20s)
setInterval(() => {
    const now = Date.now();
    activePresenters.forEach(pId => {
        if (pId === myPId) return;

        const lastSeen = lastPacketTime[pId] || 0;
        if (now - lastSeen > CONFIG.watchdogTimeout) {
            console.error(`❌ User ${pId} TIMED OUT. Removing.`);
            removePresenter(pId);
        }
    });
}, 2000);

// B. VIDEO CPR: Detects frozen video (common on mobile) and jumpstarts it
setInterval(() => {
    activePresenters.forEach(pId => {
        const video = document.getElementById(`video-${pId}`);
        const sb = mediaBuffers[pId];

        if (!video || !sb || video.paused || sb.buffered.length === 0) return;

        // Initialize monitor if needed
        if (!stuckMonitors[pId]) stuckMonitors[pId] = { lastTime: 0, stuckCount: 0 };
        const monitor = stuckMonitors[pId];

        // Check if playhead moved
        if (Math.abs(video.currentTime - monitor.lastTime) < 0.1) {
            monitor.stuckCount++;
        } else {
            monitor.stuckCount = 0;
            monitor.lastTime = video.currentTime;
        }

        // If stuck for > 3 seconds, nudge the player
        if (monitor.stuckCount > 3) {
            console.warn(`⚠️ Video ${pId} frozen! Jumpstarting...`);
            const end = sb.buffered.end(sb.buffered.length - 1);

            // If lag is huge (>2s), jump to live edge. Otherwise just nudge.
            if (end - video.currentTime > 2) {
                video.currentTime = end - 0.1;
            } else {
                video.currentTime += 0.1;
            }

            // Prevent infinite loop
            if (monitor.stuckCount > 10) monitor.stuckCount = 0;
        }
    });
}, 1000);

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
    delete stuckMonitors[pId];

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
    // 1. Anti-Throttle Hack (Keeps tab alive in background)
    preventBackgroundThrottling();

    try {
        // 2. MOBILE OPTIMIZATION: Limit resolution to 720p
        // 4K/1080p kills mobile encoders and bandwidth.
        const constraints = {
            audio: true,
            video: {
                width: { ideal: 1280, max: 1280 },
                height: { ideal: 720, max: 720 },
                frameRate: { ideal: 24, max: 30 }
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        // Show Local Video
        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true; // Always mute self to avoid feedback

        // 3. Dynamic Protocol (WSS for Production, WS for Localhost)
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH`;

        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("🎥 PRESENTER: Connected");

            // 4. BITRATE: 1 Mbps is safe for mobile 4G/5G
            const options = {
                mimeType: CONFIG.codec,
                videoBitsPerSecond: 1000000
            };

            if (!MediaRecorder.isTypeSupported(options.mimeType)) {
                console.warn(`Codec ${options.mimeType} not supported, using default.`);
                delete options.mimeType;
            }

            const recorder = new MediaRecorder(stream, options);

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {

                    // 5. CLIENT-SIDE BACKPRESSURE (Critical for Mobile)
                    // If buffer > 64KB, network is lagging. Drop frame to save connection.
                    if (socket.bufferedAmount > 64 * 1024) {
                        console.warn(`🐢 Slow Network! Dropping frame. Buffer: ${socket.bufferedAmount}`);
                        return;
                    }

                    socket.send(await event.data.arrayBuffer());
                }
            };

            recorder.start(CONFIG.chunkInterval);

            // Force Keyframe every 2s
            setInterval(() => {
                if(recorder.state === "recording") recorder.requestData();
            }, 2000);
        };

        socket.onclose = (e) => {
            console.error(`⚠️ Socket Closed: ${e.code}`);
            if (e.code === 1009) alert("Packet too big! Check server config.");
            else setTimeout(startPublishing, 2000); // Retry
        };

    } catch (err) {
        console.error("Camera Error:", err);
        alert("Camera Failed: " + err.message);
    }
}

// Audio Context Hack: Keeps the tab running at full speed when minimized
function preventBackgroundThrottling() {
    const AudioContext = window.AudioContext || window.webkitAudioContext;
    if (!AudioContext) return;
    const ctx = new AudioContext();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    gain.gain.value = 0; // Silent
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
            console.log("🔌 WATCHER: Connecting...");
            await startWatching();
        } catch (err) {
            console.error("❌ WATCHER ERROR:", err);
        }
        console.log("♻️ Reconnecting Watcher in 2s...");
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const controller = new AbortController();
    const response = await fetch(`/watch/${streamId}?quality=HIGH`, { signal: controller.signal });

    if (!response.ok) throw new Error(`Server Status: ${response.status}`);

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    console.log("✅ WATCHER: Stream Started");

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

    // Refresh Watchdog
    lastPacketTime[frameDTO.pId] = Date.now();

    // Create Tile if new
    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false);
    }

    // Decode Base64 -> Uint8Array
    const binaryString = window.atob(frameDTO.dataBase64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }

    // Queue Data
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
        console.error("Append Error:", e);

        // Handle QuotaExceeded (Buffer Full)
        if (e.name === 'QuotaExceededError') {
            const video = document.getElementById(`video-${pId}`);
            if (sb.buffered.length > 0) {
                const removeEnd = video.currentTime - 3;
                if (removeEnd > 0) {
                    try { sb.remove(0, removeEnd); } catch(ex) {}
                }
            }
        } else {
            // For invalid chunks, just DROP them. Do not loop.
            console.warn("🗑️ Dropping corrupt chunk");
        }
    }
}

// ==========================================
// 7. UI & GRID
// ==========================================
function createVideoTile(pId, isLocal) {
    if (activePresenters.size >= 4) return;
    activePresenters.add(pId);

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';

    // SOUND FIX: Added "Unmute" button because autoplay videos are always muted by default
    card.innerHTML = `
        <div style="position: relative; width: 100%; height: 100%;">
            <video id="video-${pId}" autoplay playsinline muted style="width:100%; height:100%; object-fit: cover;"></video>
            ${!isLocal ? `<button id="btn-${pId}" style="position: absolute; top: 10px; right: 10px; z-index: 10; padding: 5px 10px; cursor: pointer;">🔇 Unmute</button>` : ''}
        </div>
        <div class="label">${isLocal ? "Me" : pId}</div>
    `;
    grid.appendChild(card);
    updateGridLayout();

    // Logic for Remote Videos
    if (!isLocal) {
        const video = card.querySelector('video');
        const btn = card.querySelector(`#btn-${pId}`);

        // Click to Unmute
        if (btn) {
            btn.onclick = () => {
                video.muted = !video.muted;
                btn.innerText = video.muted ? "🔇 Unmute" : "🔊 On";
            };
        }

        const ms = new MediaSource();
        video.src = URL.createObjectURL(ms);
        mediaSources[pId] = ms;

        ms.onsourceopen = () => {
            if (MediaSource.isTypeSupported(CONFIG.codec)) {
                const sb = ms.addSourceBuffer(CONFIG.codec);
                sb.mode = 'sequence';
                mediaBuffers[pId] = sb;
                mediaSourceReady[pId] = true;

                sb.addEventListener('updateend', () => processQueue(pId));

                // Process any early data
                if (mediaQueues[pId] && mediaQueues[pId].length > 0) {
                    processQueue(pId);
                }
            } else {
                console.error("Browser does not support codec:", CONFIG.codec);
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