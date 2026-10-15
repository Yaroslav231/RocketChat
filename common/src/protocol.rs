use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Data {
    Message(Message),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub sender_id: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestActiveUsers {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Login {
    pub request_id: String,
    pub user_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPublicKey {
    pub request_id: String,
    pub public_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestPublicKey {
    pub request_id: String,
    pub recipient_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Logout {
    pub request_id: String,
    pub user_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendEncryptedData {
    pub request_id: String,
    pub sender_id: i64,
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestUserById {
    pub request_id: String,
    pub user_id: i64
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    RequestActiveUsers(RequestActiveUsers),
    Login(Login),
    RegisterPublicKey(RegisterPublicKey),
    RequestPublicKey(RequestPublicKey),
    Logout(Logout),
    SendEncryptedData(SendEncryptedData),
    RequestUserById(RequestUserById),
}

impl Request {
    pub const REQUEST_ACTIVE_USERS_CODE: i32 = 1;
    pub const LOGIN_CODE: i32 = 2;
    pub const REGISTER_PUBLIC_KEY_CODE: i32 = 3;
    pub const REQUEST_PUBLIC_KEY_CODE: i32 = 4;
    pub const LOGOUT_CODE: i32 = 5;
    pub const SEND_ENCRYPTED_DATA_CODE: i32 = 6;
    pub const REQUEST_USER_BY_ID_CODE: i32 = 7;

    pub fn request_id(&self) -> &str {
        match self {
            Self::RequestActiveUsers(v) => v.request_id.as_str(),
            Self::Login(v) => v.request_id.as_str(),
            Self::RegisterPublicKey(v) => v.request_id.as_str(),
            Self::RequestPublicKey(v) => v.request_id.as_str(),
            Self::Logout(v) => v.request_id.as_str(),
            Self::SendEncryptedData(v) => v.request_id.as_str(),
            Self::RequestUserById(v) => v.request_id.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Confirm {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Error {
    pub request_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedDataDelivered {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicKey {
    pub request_id: String,
    pub public_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginConfirm {
    pub request_id: String,
    pub user: User,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveUsers {
    pub request_id: String,
    pub active_users: Vec<User>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutConfirm {
    pub request_id: String,
    pub user_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserById {
    pub request_id: String,
    pub user_name: String
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Answer {
    Confirm(Confirm),
    Error(Error),
    EncryptedDataDelivered(EncryptedDataDelivered),
    PublicKey(PublicKey),
    LoginConfirm(LoginConfirm),
    ActiveUsers(ActiveUsers),
    LogoutConfirm(LogoutConfirm),
    UserById(UserById),
}

impl Answer {
    pub const CONFIRM_CODE: i32 = 7;
    pub const ERROR_CODE: i32 = 8;
    pub const ENCRYPTED_DATA_DELIVERED_CODE: i32 = 9;
    pub const PUBLIC_KEY_CODE: i32 = 10;
    pub const LOGIN_CONFIRM_CODE: i32 = 11;
    pub const ACTIVE_USERS_CODE: i32 = 12;
    pub const USER_BY_ID_CODE: i32 = 14;
    pub const LOGOUT_CONFIRM_CODE: i32 = 13;

    pub fn request_id(&self) -> &str {
        match self {
            Self::Confirm(v) => v.request_id.as_str(),
            Self::Error(v) => v.request_id.as_str(),
            Self::EncryptedDataDelivered(v) => v.request_id.as_str(),
            Self::PublicKey(v) => v.request_id.as_str(),
            Self::LoginConfirm(v) => v.request_id.as_str(),
            Self::ActiveUsers(v) => v.request_id.as_str(),
            Self::LogoutConfirm(v) => v.request_id.as_str(),
            Self::UserById(v) => v.request_id.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data")]
pub enum Packet {
    Request(Request),
    Answer(Answer),
}

#[derive(Debug, Clone)]
pub struct PacketHeader {
    pub code: i32,
    pub recipient_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct DataPacket {
    pub data: Vec<u8>,
    pub header: PacketHeader,
}


pub fn is_routable(code: i32) -> bool {
    code == Request::SEND_ENCRYPTED_DATA_CODE
        || code == Answer::ENCRYPTED_DATA_DELIVERED_CODE
}

pub const MAX_PACKET_SIZE: usize = 64 * 1024 * 1024;