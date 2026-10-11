use crate::models::message::{Message, MessageWithUsername, NewMessage};
use crate::schema::{messages, users};
use crate::models::HistoryMessage;
use diesel::deserialize::QueryableByName;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Unsigned};
use diesel::{MysqlConnection, QueryResult};

pub struct HistoryPage {
    pub messages: Vec<HistoryMessage>,
    pub has_more: bool,
    pub oldest_id: Option<i32>,
}

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
) -> QueryResult<HistoryPage> {
    let mut rows = messages::table
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
        .order(messages::id.desc())
        .limit(51)
        .load::<MessageWithUsername>(connection)?;

    let has_more = rows.len() > 50;
    rows.truncate(50);
    rows.reverse();

    let messages: Vec<HistoryMessage> = rows
        .into_iter()
        .map(|message| HistoryMessage {
            id: message.id,
            username: message.username,
            content: message.content,
            created_at: message.created_at.to_string(),
        })
        .collect();

    let oldest_id = messages.first().map(|message| message.id);

    Ok(HistoryPage {
        messages,
        has_more,
        oldest_id,
    })
}

pub fn get_history_before(
    connection: &mut MysqlConnection,
    room_id: &str,
    before_id: i32,
) -> QueryResult<HistoryPage> {
    let mut rows = messages::table
        .inner_join(users::table.on(users::id.eq(messages::user_id)))
        .filter(messages::room_id.eq(room_id))
        .filter(messages::id.lt(before_id))
        .select((
            messages::id,
            messages::room_id,
            messages::user_id,
            users::username,
            messages::content,
            messages::created_at,
        ))
        .order(messages::id.desc())
        .limit(51)
        .load::<MessageWithUsername>(connection)?;

    // Có 51 bản ghi nghĩa là vẫn còn tin cũ hơn.
    let has_more = rows.len() > 50;

    // Chỉ trả tối đa 50 tin nhắn.
    rows.truncate(50);

    // DB trả mới nhất trước; đảo lại để hiển thị cũ đến mới.
    rows.reverse();

    let messages: Vec<HistoryMessage> = rows
        .into_iter()
        .map(|message| HistoryMessage {
            id: message.id,
            username: message.username,
            content: message.content,
            created_at: message.created_at.to_string(),
        })
        .collect();

    let oldest_id = messages.first().map(|message| message.id);

    Ok(HistoryPage {
        messages,
        has_more,
        oldest_id,
    })
}