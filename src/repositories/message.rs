use crate::models::message::{Message, MessageWithUsername, NewMessage};
use crate::schema::{messages, users};
use diesel::deserialize::QueryableByName;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Unsigned};
use diesel::{MysqlConnection, QueryResult};

#[derive(QueryableByName)]
struct LastInsertId {
    #[diesel(sql_type = Unsigned<BigInt>)]
    id: u64,
}

pub fn create(
    connection: &mut MysqlConnection,
    room_id: &str,
    user_id: i32,
    content: &str,
) -> QueryResult<Message> {
    let new_message = NewMessage {
        room_id,
        user_id,
        content,
    };

    diesel::insert_into(messages::table)
        .values(&new_message)
        .execute(connection)?;

    let result = diesel::sql_query("SELECT LAST_INSERT_ID() AS id")
        .get_result::<LastInsertId>(connection)?;

    messages::table
        .find(result.id as i32)
        .select(Message::as_select())
        .first(connection)
}

pub fn get_history(
    connection: &mut MysqlConnection,
    room_id: &str,
) -> QueryResult<Vec<MessageWithUsername>> {
    messages::table
        .inner_join(users::table.on(users::id.eq(messages::user_id)))
        .filter(messages::room_id.eq(room_id))
        .select((
            messages::id,
            messages::room_id,
            messages::user_id,
            users::username,
            messages::content,
            messages::created_at,
        ))
        .order(messages::id.asc())
        .load::<MessageWithUsername>(connection)
}
