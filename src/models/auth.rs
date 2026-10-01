use serde::{Deserialize, Serialize};


#[derive(Debug, Deserialize)]
pub struct AuthenticationMessage {
    pub token: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: i32,
    pub exp: usize
}