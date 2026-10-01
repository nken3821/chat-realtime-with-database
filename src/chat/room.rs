use crate::models::message::ChatMessage;
use std::collections::{ HashMap, HashSet, VecDeque };
use tokio::sync::{ broadcast, mpsc };

pub struct Client {
    pub username: String,
    pub sender: mpsc::UnboundedSender<ChatMessage>,
}

pub struct Room {
    pub sender: broadcast::Sender<ChatMessage>,
    pub clients: HashMap<String, Client>,
    pub history: VecDeque<ChatMessage>,
}

pub struct RoomManager {
    pub rooms: HashMap<String, Room>,
}

impl RoomManager {
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
                history: VecDeque::new(),
            }
        })
    }

    pub fn add_history(&mut self, room_id: &str, message: ChatMessage) {
        if let Some(room) = self.rooms.get_mut(room_id) {
            room.history.push_back(message);

            if room.history.len() > 50 {
                room.history.pop_front();
            }
        }
    }

    pub fn get_online_users(&self, room_id: &str) -> Vec<String> {
        self.rooms
            .get(room_id)
            .map(|room| {
                room.clients
                    .values()
                    .map(|client| client.username.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn add_client(
        &mut self,
        room_id: &str,
        connection_id: String,
        username: String,
        sender: mpsc::UnboundedSender<ChatMessage>
    ) {
        let room = self.get_or_create_room(&room_id);
        room.clients.insert(connection_id, Client {
            username: username,
            sender: sender,
        });
    }

    pub fn remove_client(&mut self, room_id: &str, connection_id: &str) -> Vec<String> {
        if let Some(room) = self.rooms.get_mut(room_id) {
            room.clients.remove(connection_id);
            return room.clients
                .values()
                .map(|client| client.username.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
        }
        Vec::new()
    }

    pub fn get_history(&mut self, room_id: &str) -> Vec<ChatMessage> {
        self.rooms
            .get(room_id)
            .map(|room| room.history.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_client_sender(
        &self,
        room_id: &str,
        username: &str
    ) -> Vec<mpsc::UnboundedSender<ChatMessage>> {
        self.rooms
            .get(room_id)
            .map(|room| {
                room.clients
                    .values()
                    .filter(|client| client.username == username)
                    .map(|client| client.sender.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn remove_client_and_cleanup(&mut self, room_id: &str, connection_id: &str) -> Vec<String> {
        let should_remove_room = {
            if let Some(room) = self.rooms.get_mut(room_id) {
                println!("[Room: {}] Client {} removed", room_id, connection_id);
                room.clients.remove(connection_id);
                if room.clients.is_empty() {
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if should_remove_room {
            println!("[Room: {}] Empty room removed", room_id);
            self.rooms.remove(room_id);
            Vec::new()
        } else {
            self.get_online_users(room_id)
        }
    }
}
