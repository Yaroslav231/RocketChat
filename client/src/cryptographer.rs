use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use anyhow::{Result, anyhow, bail};
use hkdf::Hkdf;
use rand_core::OsRng;
use sha2::Sha256;
use x25519_dalek::{PublicKey, SharedSecret, StaticSecret};

const AES_KEY_SIZE: usize = 32;
const GCM_NONCE_SIZE: usize = 12;
const X25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x03, 0x21, 0x00,
];

#[derive(Debug, Clone)]
pub struct EncryptedData {
    pub nonce: [u8; GCM_NONCE_SIZE],
    pub ciphertext: Vec<u8>,
}
pub struct Cryptographer;

impl Cryptographer {
    pub fn generate_key_pair() -> (StaticSecret, PublicKey) {
        let private_key = StaticSecret::random_from_rng(OsRng);
        let public_key = PublicKey::from(&private_key);
        (private_key, public_key)
    }
    pub fn public_key_to_bytes(public_key: &PublicKey) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(X25519_SPKI_PREFIX.len() + 32);
        encoded.extend_from_slice(&X25519_SPKI_PREFIX);
        encoded.extend_from_slice(public_key.as_bytes());
        encoded
    }
    pub fn public_key_from_bytes(encoded: &[u8]) -> Result<PublicKey> {
        let raw = if encoded.len() == 32 {
            encoded
        } else if encoded.len() == X25519_SPKI_PREFIX.len() + 32
            && encoded.starts_with(&X25519_SPKI_PREFIX)
        {
            &encoded[X25519_SPKI_PREFIX.len()..]
        } else {
            bail!(
                "Невідомий формат X25519 public key: очікувалося 32 байти raw або 44 байти X.509"
            );
        };

        let bytes: [u8; 32] = raw
            .try_into()
            .map_err(|_| anyhow!("Некоректна довжина X25519 public key"))?;
        Ok(PublicKey::from(bytes))
    }
    pub fn generate_shared_secret(
        private_key: &StaticSecret,
        public_key: &PublicKey,
    ) -> Result<[u8; 32]> {
        let shared: SharedSecret = private_key.diffie_hellman(public_key);

        if shared.as_bytes().iter().all(|byte| *byte == 0) {
            bail!("X25519 produced an all-zero shared secret");
        }

        Ok(*shared.as_bytes())
    }
    pub fn derive_aes_key(
        shared_secret: &[u8],
        salt: &[u8],
        info: &[u8],
    ) -> Result<[u8; AES_KEY_SIZE]> {
        let zero_salt = [0u8; 32];
        let actual_salt = if salt.is_empty() { &zero_salt[..] } else { salt };

        let hkdf = Hkdf::<Sha256>::new(Some(actual_salt), shared_secret);
        let mut key = [0u8; AES_KEY_SIZE];
        hkdf.expand(info, &mut key)
            .map_err(|_| anyhow!("Не вдалось отримати AES-256 ключ через HKDF"))?;
        Ok(key)
    }
    pub fn derive_session_key(shared_secret: &[u8]) -> Result<[u8; AES_KEY_SIZE]> {
        Self::derive_aes_key(shared_secret, &[], b"RocketChat")
    }
    pub fn encrypt(
        key: &[u8; AES_KEY_SIZE],
        plaintext: &[u8],
        associated_data: Option<&[u8]>,
    ) -> Result<EncryptedData> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|_| anyhow!("AES-256-GCM requires a 32-byte key"))?;

        let mut nonce = [0u8; GCM_NONCE_SIZE];
        use rand_core::RngCore;
        let mut rng = OsRng;
        rng.fill_bytes(&mut nonce);

        let payload = Payload {
            msg: plaintext,
            aad: associated_data.unwrap_or(&[]),
        };
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), payload)
            .map_err(|_| anyhow!("Не вдалось зашифрувати данні AES-GCM"))?;

        Ok(EncryptedData { nonce, ciphertext })
    }

    pub fn decrypt(
        key: &[u8; AES_KEY_SIZE],
        encrypted: &EncryptedData,
        associated_data: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|_| anyhow!("AES-256-GCM requires a 32-byte key"))?;

        let payload = Payload {
            msg: encrypted.ciphertext.as_ref(),
            aad: associated_data.unwrap_or(&[]),
        };

        cipher
            .decrypt(Nonce::from_slice(&encrypted.nonce), payload)
            .map_err(|_| anyhow!("Не вдалось розшифрувати данні: невірний ключ або пошкоджений пакет"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x25519_keys_derive_same_session_key() {
        let (alice_private, alice_public) = Cryptographer::generate_key_pair();
        let (bob_private, bob_public) = Cryptographer::generate_key_pair();

        let alice_shared = Cryptographer::generate_shared_secret(&alice_private, &bob_public)
            .expect("Alice should derive a shared secret");
        let bob_shared = Cryptographer::generate_shared_secret(&bob_private, &alice_public)
            .expect("Bob should derive a shared secret");

        let alice_key = Cryptographer::derive_session_key(&alice_shared).unwrap();
        let bob_key = Cryptographer::derive_session_key(&bob_shared).unwrap();
        assert_eq!(alice_key, bob_key);
    }

    #[test]
    fn x509_public_key_round_trip() {
        let (_, public_key) = Cryptographer::generate_key_pair();
        let encoded = Cryptographer::public_key_to_bytes(&public_key);
        assert_eq!(encoded.len(), 44);
        let decoded = Cryptographer::public_key_from_bytes(&encoded).unwrap();
        assert_eq!(decoded.as_bytes(), public_key.as_bytes());
    }

    #[test]
    fn aes_gcm_encrypts_and_decrypts() {
        let key = [7u8; AES_KEY_SIZE];
        let encrypted = Cryptographer::encrypt(&key, b"hello", None).unwrap();
        let plaintext = Cryptographer::decrypt(&key, &encrypted, None).unwrap();
        assert_eq!(plaintext, b"hello");
    }
}
