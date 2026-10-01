use diesel::{MysqlConnection, QueryResult, RunQueryDsl, deserialize::QueryableByName};


#[derive(Debug, QueryableByName)]
pub struct UserIdentity {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub id: i32,

    #[diesel(sql_type = diesel::sql_types::VarChar)]
    pub username: String
}

pub fn find_user_identity(user_id: i32, connection: &mut MysqlConnection) -> QueryResult<UserIdentity> {
        diesel::sql_query(
            "SELECT id, username FROM users WHERE id = ? LIMIT 1"
        ).bind::<diesel::sql_types::Integer, _>(user_id)
        .load::<UserIdentity>(connection)
        .and_then(|mut users| {
            users
                .pop()
                .ok_or(diesel::result::Error::NotFound)  
        })
    }