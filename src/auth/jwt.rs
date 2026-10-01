use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};

use crate::models::Claims;


pub fn verify_token(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let key = DecodingKey::from_secret(secret.as_bytes());
    let validation = Validation::default();
    let token_data = decode::<Claims>(
        token,
        &key,
        &validation
    )?;
    Ok(token_data.claims)
}
