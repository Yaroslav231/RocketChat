use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use serde_json::Value;
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct ReceivedResponse {
    pub code: i32,
    pub packet: Value,
}

type PendingSender = oneshot::Sender<std::result::Result<ReceivedResponse, String>>;
pub type PendingReceiver = oneshot::Receiver<std::result::Result<ReceivedResponse, String>>;

pub struct RequestManager {
    pending: Mutex<HashMap<String, PendingSender>>,
    response_timeout_ms: AtomicU64,
}

impl RequestManager {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            response_timeout_ms: AtomicU64::new(100_000),
        }
    }

    pub fn set_response_timeout(&self, timeout_ms: u64) {
        self.response_timeout_ms.store(timeout_ms, Ordering::Relaxed);
    }

    pub async fn create_request(&self, request_id: impl Into<String>) -> Result<PendingReceiver> {
        let request_id = request_id.into();
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.pending.lock().await;

        if pending.contains_key(&request_id) {
            return Err(anyhow!("Запит з request_id={request_id} вже зареєстрований"));
        }

        pending.insert(request_id, sender);
        Ok(receiver)
    }
    
    pub async fn complete(&self, request_id: &str, response: ReceivedResponse) -> bool {
        let sender = self.pending.lock().await.remove(request_id);
        match sender {
            Some(sender) => sender.send(Ok(response)).is_ok(),
            None => false,
        }
    }

    pub async fn cancel(&self, request_id: &str) {
        self.pending.lock().await.remove(request_id);
    }

    pub async fn fail_all(&self, message: impl Into<String>) {
        let message = message.into();
        let mut guard = self.pending.lock().await;
        let pending = std::mem::take(&mut *guard);
        drop(guard);
        for (_, sender) in pending {
            let _ = sender.send(Err(message.clone()));
        }
    }
    pub async fn wait_and_get_packet(
        &self,
        request_id: &str,
        receiver: PendingReceiver,
        expected_answer: &str,
    ) -> Result<Value> {
        let duration = Duration::from_millis(
            self.response_timeout_ms.load(Ordering::Relaxed),
        );

        let waited = timeout(duration, receiver).await;
        self.cancel(request_id).await;

        let response = match waited {
            Err(_) => return Err(anyhow!("Час очікування відповіді від сервера вичерпано")),
            Ok(Err(_)) => return Err(anyhow!("Канал очікування відповіді закрито")),
            Ok(Ok(Err(message))) => return Err(anyhow!(message)),
            Ok(Ok(Ok(response))) => response,
        };

        let answer_type = response
            .packet
            .pointer("/data/type")
            .and_then(Value::as_str)
            .unwrap_or_default();

        if response.code == 8 || answer_type == "Error" {
            let message = response
                .packet
                .pointer("/data/message")
                .or_else(|| response.packet.pointer("/data/error"))
                .and_then(Value::as_str)
                .unwrap_or("Сервер повернув невідому помилку");
            return Err(anyhow!(message.to_owned()));
        }

        if answer_type != expected_answer {
            return Err(anyhow!(
                "Неочікуваний тип відповіді: отримано '{answer_type}', очікувався '{expected_answer}'"
            ));
        }

        Ok(response.packet)
    }
}

impl Default for RequestManager {
    fn default() -> Self {
        Self::new()
    }
}
