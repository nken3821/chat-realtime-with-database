mod chat;
mod models;
mod websocket;
mod auth;
mod db;

use rocket::{launch, routes};
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;
use db::create_pool;

use crate::chat::RoomManager;
use crate::websocket::websocket as websocket_function;

#[launch]
fn rocket() -> _ {

    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    let pool = create_pool(&database_url);


    let manager = Arc::new(RwLock::new(RoomManager::new()));

    rocket::build()
        .manage(manager)
        .manage(pool)
        .mount("/", routes![websocket_function])
}


