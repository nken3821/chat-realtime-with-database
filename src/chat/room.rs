use std::collections::{HashMap, VecDeque};
use tokio::sync::{broadcast, mpsc};
use crate::models::message::ChatMessage;

pub struct Client {
    pub username: String,
    pub sender: mpsc::UnboundedSender<ChatMessage>
}

pub struct Room {
    pub sender: broadcast::Sender<ChatMessage>,
    pub clients: HashMap<String, Client>,
    pub history: VecDeque<ChatMessage>
}

pub struct RoomManager {
    pub rooms: HashMap<String, Room>,
}

impl RoomManager  {
    pub fn new() -> Self {
        Self {
            rooms: HashMap::new(),
        }
    }

    pub fn get_or_create_room(&mut self, room_id: &str) -> &mut Room {
        self.rooms.entry(room_id.to_string()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(100);
            Room {
                sender,
                clients: HashMap::new(),
                history: VecDeque::new()
            }
        })
    }
}