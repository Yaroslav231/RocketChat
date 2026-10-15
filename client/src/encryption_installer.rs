use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use common::protocol::{
    Packet,
    Request,
    RegisterPublicKey,
    RequestPublicKey,
};
use tokio::sync::RwLock;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::chat_api::ChatApi;
use crate::cryptographer::{Cryptographer, EncryptedData};

pub struct EncryptionInstaller {
    private_key: StaticSecret,
    public_key: PublicKey,
    peer_public_keys: RwLock<HashMap<i64, PublicKey>>,
    session_keys: RwLock<HashMap<i64, [u8; 32]>>,
}

impl EncryptionInstaller {
    pub fn new() -> Self {
        let (private_key, public_key) =
            Cryptographer::generate_key_pair();

        Self {
            private_key,
            public_key,
            peer_public_keys: RwLock::new(HashMap::new()),
            session_keys: RwLock::new(HashMap::new()),
        }
    }
    pub fn public_key_bytes(&self) -> Vec<u8> {
        Cryptographer::public_key_to_bytes(&self.public_key)
    }
    pub async fn send_public_key(
        &self,
        api: &ChatApi,
    ) -> Result<()> {
        api.request(
            |request_id| {
                Packet::Request(
                    Request::RegisterPublicKey(
                        RegisterPublicKey {
                            request_id,
                            public_key: self.public_key_bytes(),
                        },
                    ),
                )
            },
            Request::REGISTER_PUBLIC_KEY_CODE,
            "Confirm",
        )
            .await
            .context(
                "Не вдалось зареєструвати відкритий ключ на сервері",
            )?;

        Ok(())
    }
    pub async fn get_session_key(
        &self,
        user_id: i64,
        api: &ChatApi,
    ) -> Result<[u8; 32]> {
        if let Some(key) =
            self.session_keys.read().await.get(&user_id).copied()
        {
            return Ok(key);
        }

        let recipient_public_key =
            self.get_public_key(user_id, api).await?;

        let shared_secret = Cryptographer::generate_shared_secret(
            &self.private_key,
            &recipient_public_key,
        )?;

        let session_key =
            Cryptographer::derive_session_key(&shared_secret)?;

        self.session_keys
            .write()
            .await
            .insert(user_id, session_key);

        Ok(session_key)
    }
    pub async fn get_public_key(
        &self,
        user_id: i64,
        api: &ChatApi,
    ) -> Result<PublicKey> {
        if let Some(key) =
            self.peer_public_keys.read().await.get(&user_id).cloned()
        {
            return Ok(key);
        }

        let answer = api
            .request(
                |request_id| {
                    Packet::Request(
                        Request::RequestPublicKey(
                            RequestPublicKey {
                                request_id,
                                recipient_id: user_id,
                            },
                        ),
                    )
                },
                Request::REQUEST_PUBLIC_KEY_CODE,
                "PublicKey",
            )
            .await
            .with_context(|| {
                format!(
                    "Не вдалось отримати відкритий ключ {user_id}"
                )
            })?;

        let key_value = answer
            .pointer("/data/public_key")
            .or_else(|| answer.pointer("/data/publicKey"))
            .or_else(|| answer.pointer("/data/key"))
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "У відповіді PublicKey відсутнє поле public_key"
                )
            })?;

        let encoded: Vec<u8> = serde_json::from_value(key_value)
            .context(
                "Поле public_key має бути массивом байтів",
            )?;

        let public_key = Cryptographer::public_key_from_bytes(&encoded)?;

        self.peer_public_keys
            .write()
            .await
            .insert(user_id, public_key);

        Ok(public_key)
    }
    pub async fn decrypt_from_user(
        &self,
        user_id: i64,
        nonce: &[u8],
        ciphertext: &[u8],
        api: &ChatApi,
    ) -> Result<Vec<u8>> {
        let session_key =
            self.get_session_key(user_id, api).await?;

        let nonce: [u8; 12] = nonce
            .try_into()
            .map_err(|_| {
                anyhow!(
                    "Nonce AES-GCM має містити 12 байтів"
                )
            })?;

        Cryptographer::decrypt(
            &session_key,
            &EncryptedData {
                nonce,
                ciphertext: ciphertext.to_vec(),
            },
            None,
        )
    }
    pub async fn encrypt_for_user(
        &self,
        user_id: i64,
        plaintext: &[u8],
        api: &ChatApi,
    ) -> Result<EncryptedData> {
        let session_key =
            self.get_session_key(user_id, api).await?;

        Cryptographer::encrypt(
            &session_key,
            plaintext,
            None,
        )
    }
    pub async fn clear_session_keys(&self) {
        self.session_keys.write().await.clear();
        self.peer_public_keys.write().await.clear();
    }
}

impl Default for EncryptionInstaller {
    fn default() -> Self {
        Self::new()
    }
}