use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use common::connection::ConnectionHandler;

use crate::object_transport::ObjectTransport;

pub struct ClientConnection {
    pub connection_id: u64,

    pub connection_handler:
        Arc<ConnectionHandler>,

    pub object_transport:
        ObjectTransport,

    user_id: AtomicI64,
}

impl ClientConnection {
    pub fn new(
        connection_id: u64,
        connection_handler: Arc<ConnectionHandler>,
    ) -> Self {
        let object_transport =
            ObjectTransport::new(
                Arc::clone(&connection_handler)
            );

        Self {
            connection_id,
            connection_handler,
            object_transport,
            user_id: AtomicI64::new(0),
        }
    }

    pub fn user_id(&self) -> Option<i64> {
        let id = self.user_id.load(Ordering::Acquire);

        if id > 0 {
            Some(id)
        } else {
            None
        }
    }

    pub fn set_user_id(&self, id: Option<i64>) {
        self.user_id.store(
            id.unwrap_or(0),
            Ordering::Release,
        );
    }

    pub fn take_user_id(&self) -> Option<i64> {
        let id = self.user_id.swap(
            0,
            Ordering::AcqRel,
        );

        if id > 0 {
            Some(id)
        } else {
            None
        }
    }
}