diesel::table! {
    messages (id) {
        id -> Integer,
        #[max_length = 100]
        room_id -> Varchar,
        user_id -> Integer,
        content -> Text,
        created_at -> Datetime,
    }
}

diesel::table! {
    users (id) {
        id -> Integer,
        #[max_length = 25]
        username -> Varchar,
        #[max_length = 255]
        password_hash -> Varchar,
        #[max_length = 30]
        email -> Varchar,
        created_at -> Datetime,
        updated_at -> Datetime,
    }
}

diesel::allow_tables_to_appear_in_same_query!(messages, users,);
