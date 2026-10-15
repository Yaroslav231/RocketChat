use serde::{
    de::DeserializeOwned,
    Serialize,
};

pub struct SerializationService;
impl SerializationService {
    pub fn serialize<T: Serialize>(
        value: &T
    ) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(value)
    }

    pub fn deserialize<T: DeserializeOwned>(
        data: &[u8]
    ) -> Result<T, serde_json::Error> {
        serde_json::from_slice(data)
    }
}