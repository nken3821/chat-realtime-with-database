CREATE TABLE messages (
    id Int auto_increment PRIMARY KEY,
    room_id varchar(100) not null,
    user_id int not null,
    content text not null,
    created_at datetime not null DEFAULT CURRENT_TIMESTAMP
)
