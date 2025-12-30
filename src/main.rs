use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use base64::{engine::general_purpose, Engine as _};
use dashmap::DashMap;
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

// ==========================================
// 1. STATE MANAGEMENT
// ==========================================

// A "Room" holds the broadcast channel for a specific stream.
// Capacity 1024 means it can buffer 1024 video chunks before lagging watchers drop packets.
struct Room {
    tx: broadcast::Sender<VideoFrame>,
    last_frame: Arc<VideoFrame>, // Cache the last frame (header) for new joiners
    last_heartbeat: Instant,
}

// The shared state of the application
struct AppState {
    // Map<StreamId, Room>
    rooms: DashMap<String, Room>,
}

// The data packet sent to Watchers
#[derive(Clone, Serialize, Debug)]
struct VideoFrame {
    #[serde(rename = "pId")]
    p_id: String,
    #[serde(rename = "dataBase64")]
    data_base64: String,
}

// ==========================================
// 2. MAIN SERVER SETUP
// ==========================================

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Shared state across all connections
    let state = Arc::new(AppState {
        rooms: DashMap::new(),
    });

    // Spawn a cleanup task (The "Garbage Collector")
    let cleaner_state = state.clone();
    tokio::spawn(async move {
        cleanup_loop(cleaner_state).await;
    });

    // Define Routes
    let app = Router::new()
        .route(
            "/publish/:stream_id/:user_id/:quality",
            get(ws_publish_handler),
        )
        .route("/watch/:stream_id", get(watch_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("🚀 Rust Streaming Server listening on 0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}

// ==========================================
// 3. PRESENTER HANDLER (INGEST)
// ==========================================

async fn ws_publish_handler(
    ws: WebSocketUpgrade,
    Path((stream_id, user_id, _quality)): Path<(String, String, String)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_publisher(socket, stream_id, user_id, state))
}

async fn handle_publisher(
    mut socket: WebSocket,
    stream_id: String,
    user_id: String,
    state: Arc<AppState>,
) {
    tracing::info!("🎥 Presenter joined: {}/{}", stream_id, user_id);

    // Create or Get the Room
    // We use a block just to release the DashMap lock quickly
    let tx = {
        let mut room_entry = state.rooms.entry(stream_id.clone()).or_insert_with(|| {
            let (tx, _rx) = broadcast::channel(1024); // 1024 packet buffer
            Room {
                tx,
                last_frame: Arc::new(VideoFrame {
                    p_id: "".to_string(),
                    data_base64: "".to_string(),
                }),
                last_heartbeat: Instant::now(),
            }
        });
        room_entry.last_heartbeat = Instant::now();
        room_entry.tx.clone()
    };

    // Loop: Read from WebSocket -> Broadcast to Channel
    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Binary(data) = msg {
            // Update Heartbeat
            if let Some(mut room) = state.rooms.get_mut(&stream_id) {
                room.last_heartbeat = Instant::now();
            }

            // Encode to Base64 (to match your existing JS logic)
            let b64_data = general_purpose::STANDARD.encode(&data);

            let frame = VideoFrame {
                p_id: user_id.clone(),
                data_base64: b64_data,
            };

            // Save "Header" frame logic (simplistic: save latest frame as potential header)
            // Ideally, you'd check a flag from the client, but keeping last frame works for most codecs
            if let Some(mut room) = state.rooms.get_mut(&stream_id) {
                room.last_frame = Arc::new(frame.clone());
            }

            // Broadcast!
            // 'send' returns error if no active watchers exist, which we ignore.
            let _ = tx.send(frame);
        } else if let Message::Close(_) = msg {
            break;
        }
    }

    tracing::info!("❌ Presenter left: {}", user_id);
}

// ==========================================
// 4. WATCHER HANDLER (EGRESS)
// ==========================================

async fn watch_handler(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // We return a "Response" that is actually a long-running HTTP stream (NDJSON)
    // This matches your /watch/{id} endpoint logic perfectly.

    use axum::body::Body;
    use axum::response::Response;

    // 1. Get the broadcast receiver for this room
    let (mut rx, last_frame) = match state.rooms.get(&stream_id) {
        Some(room) => (room.tx.subscribe(), room.last_frame.clone()),
        None => {
            return Response::builder()
                .status(404)
                .body(Body::from("Stream not found"))
                .unwrap()
        }
    };

    // 2. Create an async stream
    let stream = async_stream::stream! {
        // A. Send the Last Frame immediately (Header/Keyframe)
        if !last_frame.p_id.is_empty() {
             let json = serde_json::to_string(&*last_frame).unwrap();
             yield Ok::<_, std::io::Error>(format!("data:{}\n\n", json));
        }

        // B. Loop over live messages
        while let Ok(frame) = rx.recv().await {
            // Serialize to JSON
            let json = serde_json::to_string(&frame).unwrap();

            // Send in SSE / NDJSON format
            // "data: {json}\n\n" is standard for event streams
            yield Ok(format!("data:{}\n\n", json));
        }
    };

    // 3. Wrap in Body
    Response::builder()
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .header("Access-Control-Allow-Origin", "*") // CORS
        .body(Body::from_stream(stream))
        .unwrap()
}

// ==========================================
// 5. CLEANUP TASK (Garbage Collector)
// ==========================================

async fn cleanup_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let now = Instant::now();
        // Remove rooms that haven't received data in 30 seconds
        state.rooms.retain(|id, room| {
            let active = now.duration_since(room.last_heartbeat) < Duration::from_secs(30);
            if !active {
                tracing::info!("🗑️ Cleaning up inactive room: {}", id);
            }
            active
        });
    }
}
