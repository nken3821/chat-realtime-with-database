use diesel::{MysqlConnection, r2d2::{ConnectionManager, Pool}};



pub type DbPool = Pool<ConnectionManager<MysqlConnection>>;

pub fn create_pool(database_url: &str) -> DbPool {
    let manager = ConnectionManager::<MysqlConnection>::new(database_url);

    Pool::builder()
        .build(manager)
        .expect("Failed to create database pool")
}
