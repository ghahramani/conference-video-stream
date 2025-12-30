// ==========================================
// 1. GLOBAL CONFIGURATION
// ==========================================
const CONFIG = {
    chunkInterval: 100,
    watchdogTimeout: 5000,
    maxLatency: 2.0,         // Increased slightly to prevent jitter loops
};

let streamId = "";
let myName = "User";
let myPId = "user-" + Math.floor(Math.random() * 100000);
let isPresenter = false;
let isWatching = false;
let sessionStartTime = Date.now();

// NEW: Track Socket State
let isSocketConnected = false;

const activePresenters = new Set();
const lastPacketTime = {};
const stuckMonitors = {};
const mediaBuffers = {};
const mediaSources = {};
const mediaQueues = {};
const initialSyncMap = {}; // NEW: Track if we have done the initial jump for a user

// ==========================================
// 2. WATCHDOG & STATS (THE FIX IS HERE)
// ==========================================
setInterval(() => {
    const now = Date.now();
    activePresenters.forEach(pId => {
        if (pId === myPId) return;
        const lastSeen = lastPacketTime[pId] || 0;
        if (now - lastSeen > CONFIG.watchdogTimeout) {
            console.warn(`❌ User ${pId} TIMED OUT.`);
            removePresenter(pId);
        }
    });
}, 2000);

// Stats Loop - Runs every 500ms
setInterval(() => {
    const uptime = Math.floor((Date.now() - sessionStartTime) / 1000);
    const uptimeStr = new Date(uptime * 1000).toISOString().substr(11, 8);

    activePresenters.forEach(pId => {
        const video = document.getElementById(`video-${pId}`);
        const statsEl = document.getElementById(`stats-${pId}`);
        if (!video) return;

        // --- LOCAL USER ---
        if (pId === myPId) {
            if (statsEl) {
                if (isSocketConnected) {
                    statsEl.innerHTML = `⏱️ ${uptimeStr} (Live)`;
                    statsEl.style.color = "#0f0";
                } else {
                    statsEl.innerHTML = `⚠️ Reconnecting...`;
                    statsEl.style.color = "orange";
                }
            }
            return;
        }

        // --- REMOTE USER ---
        const sb = mediaBuffers[pId];
        if (!sb || sb.buffered.length === 0) return;

        // 1. Calculate Lag (Safely)
        const bufferedEnd = sb.buffered.end(sb.buffered.length - 1);
        let lag = bufferedEnd - video.currentTime;

        // FIX: Prevent negative lag display (UI Only)
        if (lag < 0) lag = 0;

        // 2. INITIAL SYNC (Fixes the Glitch/Crash)
        // We wait until the loop runs to jump, rather than jumping inside the append event.
        // This ensures the buffer is stable before we seek.
        if (!initialSyncMap[pId] && sb.buffered.length > 0) {
            console.log(`🚀 Initial Sync for ${pId}`);
            video.currentTime = bufferedEnd - 0.1;
            initialSyncMap[pId] = true; // Mark as synced
            return;
        }

        // 3. Update UI
        if (statsEl) {
            statsEl.innerHTML = `
                ⏱️ ${uptimeStr}<br>
                📉 Lag: <span style="color: ${lag > 1.0 ? 'red' : '#0f0'}">${lag.toFixed(2)}s</span>
            `;
        }

        // 4. Auto-Jump (Anti-Lag)
        if (lag > CONFIG.maxLatency) {
            console.log(`⏩ JUMPING ${pId} (Lag: ${lag.toFixed(2)}s)`);
            video.currentTime = bufferedEnd - 0.1;
        }

        // 5. Frozen Check (Heartbeat)
        if (video.paused) return;
        const lastTime = stuckMonitors[pId] || 0;
        if (Math.abs(video.currentTime - lastTime) < 0.05) {
            // Only nudge if we actually have data to play
            if (sb.buffered.length > 0) {
                video.currentTime = bufferedEnd - 0.1;
            }
        }
        stuckMonitors[pId] = video.currentTime;
    });
}, 500);

function removePresenter(pId) {
    if (!activePresenters.has(pId)) return;
    activePresenters.delete(pId);
    document.getElementById(`video-${pId}`)?.closest('.video-card')?.remove();
    delete mediaBuffers[pId];
    delete mediaQueues[pId];
    delete mediaSources[pId];
    delete lastPacketTime[pId];
    delete stuckMonitors[pId];
    delete initialSyncMap[pId];
    updateGridLayout();
}

function removeAllRemotePresenters() {
    activePresenters.forEach(pId => {
        if (pId !== myPId) removePresenter(pId);
    });
}

// ==========================================
// 3. JOIN LOGIC
// ==========================================
function joinAsWatcher() {
    const roomInput = document.getElementById('streamInput').value;
    const nameInput = document.getElementById('usernameInput').value;
    if (!roomInput || !nameInput) return alert("Enter Room and Name");

    streamId = roomInput;
    myName = nameInput;

    document.getElementById('login-overlay').style.display = 'none';
    sessionStartTime = Date.now();
    monitorConnection();
}

async function joinAsPresenter() {
    const roomInput = document.getElementById('streamInput').value;
    const nameInput = document.getElementById('usernameInput').value;
    if (!roomInput || !nameInput) return alert("Enter Room and Name");

    streamId = roomInput;
    myName = nameInput;
    isPresenter = true;

    document.getElementById('login-overlay').style.display = 'none';
    sessionStartTime = Date.now();

    await startPublishing();
    monitorConnection();
}

// ==========================================
// 4. PRESENTER
// ==========================================
async function startPublishing() {
    preventBackgroundThrottling();

    try {
        const constraints = {
            audio: true,
            video: {
                width: {ideal: 640, max: 1280},
                height: {ideal: 480, max: 720},
                frameRate: {ideal: 24, max: 30}
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        if (!document.getElementById(`video-${myPId}`)) {
            createVideoTile(myPId, true, myName);
            const localVideo = document.getElementById(`video-${myPId}`);
            localVideo.srcObject = stream;
            localVideo.muted = true;
        }

        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const encodedName = encodeURIComponent(myName);
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH/${encodedName}`;

        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("🎥 PRESENTER: Connected");
            isSocketConnected = true;

            const options = {videoBitsPerSecond: 1000000};
            const recorder = new MediaRecorder(stream, options);
            console.log(`🎙️ Codec: ${recorder.mimeType}`);

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {
                    if (socket.bufferedAmount > 64 * 1024) return;
                    socket.send(await event.data.arrayBuffer());
                }
            };

            recorder.start(CONFIG.chunkInterval);

            setInterval(() => {
                if (recorder.state === "recording") recorder.requestData();
            }, 1000);
        };

        socket.onclose = () => {
            console.warn("⚠️ Socket closed. Retrying in 2s...");
            isSocketConnected = false;
            setTimeout(startPublishing, 2000);
        };

        socket.onerror = (err) => {
            console.error("❌ Socket Error:", err);
            isSocketConnected = false;
            socket.close();
        };

    } catch (err) {
        alert("Camera Failed: " + err.message);
    }
}

function preventBackgroundThrottling() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.start();
    } catch (e) {
    }
}

// ==========================================
// 5. WATCHER
// ==========================================
async function monitorConnection() {
    if (isWatching) return;
    isWatching = true;

    while (isWatching) {
        try {
            await startWatching();
        } catch (err) {
            console.error("⚠️ Connection Lost:", err);
            removeAllRemotePresenters();
        }
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const response = await fetch(`/watch/${streamId}?quality=HIGH`);
    if (!response.ok) throw new Error("Server Down");

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
    lastPacketTime[frameDTO.pId] = Date.now();

    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false, frameDTO.name || frameDTO.pId);
    }

    const binaryString = window.atob(frameDTO.dataBase64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }

    if (!mediaBuffers[frameDTO.pId]) {
        setupPlayerBruteForce(frameDTO.pId, bytes);
    }

    if (!mediaQueues[frameDTO.pId]) mediaQueues[frameDTO.pId] = [];
    mediaQueues[frameDTO.pId].push(bytes);

    processQueue(frameDTO.pId);
}

// ==========================================
// 6. CODEC (FIXED: NO IMMEDIATE JUMP)
// ==========================================
function setupPlayerBruteForce(pId, firstChunk) {
    const ms = mediaSources[pId];
    if (!ms || ms.readyState !== 'open') return;

    const candidates = [
        'video/webm; codecs="vp8, opus"',
        'video/mp4; codecs="avc1.42E01E, mp4a.40.2"',
        'video/webm',
        'video/mp4'
    ];

    let sb = null;
    for (const mime of candidates) {
        if (MediaSource.isTypeSupported(mime)) {
            try {
                sb = ms.addSourceBuffer(mime);
                console.log(`✅ ${pId} using ${mime}`);
                break;
            } catch (e) {
            }
        }
    }

    if (sb) {
        sb.mode = 'sequence';
        mediaBuffers[pId] = sb;
        // FIX: Removed the "updateend" listener that forced the immediate jump.
        // We now rely on the "initialSyncMap" in the stats loop to handle the first jump safely.
        sb.addEventListener('updateend', () => processQueue(pId));
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
function createVideoTile(pId, isLocal, displayName) {
    if (document.getElementById(`video-${pId}`)) return;

    if (activePresenters.size >= 4) return;
    activePresenters.add(pId);

    const nameToShow = displayName || (isLocal ? myName : "Unknown");

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';

    card.innerHTML = `
        <div class="video-wrapper">
            <video id="video-${pId}" autoplay playsinline muted></video>
            ${!isLocal ? `<button id="btn-${pId}" class="unmute-btn">🔇 Unmute</button>` : ''}
            <div id="stats-${pId}" class="stats-overlay">
                Connecting...
            </div>
        </div>
        <div class="label">${isLocal ? "Me (" + nameToShow + ")" : nameToShow}</div>
    `;
    grid.appendChild(card);
    updateGridLayout();

    if (!isLocal) {
        const video = card.querySelector('video');
        const btn = card.querySelector(`#btn-${pId}`);
        if (btn) btn.onclick = () => {
            video.muted = !video.muted;
            btn.innerText = video.muted ? "🔇 Unmute" : "🔊 On";
        };

        const ms = new MediaSource();
        video.src = URL.createObjectURL(ms);
        mediaSources[pId] = ms;
    }
}

function updateGridLayout() {
    const count = activePresenters.size;
    const grid = document.getElementById('video-grid');
    grid.className = '';
    grid.classList.add(`grid-${Math.min(count, 4)}`);
}