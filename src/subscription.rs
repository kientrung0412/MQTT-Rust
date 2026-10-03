//! Quản lý subscription MQTT v5 theo tên.
//!
//! Mỗi subscription được đăng ký với một `name` duy nhất và được gán một
//! Subscription Identifier (MQTT v5). Broker gắn identifier này vào mỗi message
//! gửi về, nên manager biết chính xác message thuộc subscription nào và gọi đúng
//! handler — kể cả khi các topic filter chồng lấn nhau (vd. `a/+/b` và `a/#`).

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Publish, SubscribeProperties};
use rumqttc::v5::{AsyncClient, ClientError};

/// Handler nhận (tên subscription, topic, payload).
pub type Handler = Arc<dyn Fn(&str, &str, &[u8]) + Send + Sync>;

#[derive(Debug)]
pub enum SubscriptionError {
    /// Filter đã được dùng bởi subscription có tên khác.
    FilterInUse { filter: String, owner: String },
    Client(ClientError),
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FilterInUse { filter, owner } => {
                write!(f, "filter '{filter}' đã được dùng bởi subscription '{owner}'")
            }
            Self::Client(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SubscriptionError {}

impl From<ClientError> for SubscriptionError {
    fn from(e: ClientError) -> Self {
        Self::Client(e)
    }
}

struct Subscription {
    id: usize,
    filter: String,
    qos: QoS,
    handler: Handler,
}

#[derive(Default)]
struct Registry {
    by_name: HashMap<String, Subscription>,
    name_by_id: HashMap<usize, String>,
    next_id: usize,
}

#[derive(Clone)]
pub struct SubscriptionManager {
    client: AsyncClient,
    registry: Arc<Mutex<Registry>>,
}

impl SubscriptionManager {
    pub fn new(client: AsyncClient) -> Self {
        Self {
            client,
            registry: Arc::new(Mutex::new(Registry {
                next_id: 1, // Subscription Identifier phải >= 1
                ..Default::default()
            })),
        }
    }

    /// Đăng ký subscription với tên `name`. Nếu tên đã tồn tại thì thay thế.
    pub async fn subscribe<F>(
        &self,
        name: &str,
        filter: &str,
        qos: QoS,
        handler: F,
    ) -> Result<(), SubscriptionError>
    where
        F: Fn(&str, &str, &[u8]) + Send + Sync + 'static,
    {
        let (id, old_filter) = {
            let mut reg = self.registry.lock().unwrap();

            // Broker chỉ giữ một subscription cho mỗi filter, nên mỗi filter chỉ thuộc một tên
            if let Some((owner, _)) = reg.by_name.iter().find(|(n, s)| *n != name && s.filter == filter) {
                return Err(SubscriptionError::FilterInUse {
                    filter: filter.to_string(),
                    owner: owner.clone(),
                });
            }

            let id = reg.next_id;
            reg.next_id += 1;
            reg.name_by_id.insert(id, name.to_string());
            let old = reg.by_name.insert(
                name.to_string(),
                Subscription {
                    id,
                    filter: filter.to_string(),
                    qos,
                    handler: Arc::new(handler),
                },
            );
            let old_filter = old.map(|s| {
                reg.name_by_id.remove(&s.id);
                s.filter
            });
            (id, old_filter)
        };

        if let Some(old) = old_filter.filter(|old| old != filter) {
            self.client.unsubscribe(old).await?;
        }
        self.send_subscribe(filter, qos, id).await?;
        Ok(())
    }

    /// Hủy subscription theo tên. Trả về `false` nếu không tìm thấy tên.
    pub async fn unsubscribe(&self, name: &str) -> Result<bool, ClientError> {
        let removed = {
            let mut reg = self.registry.lock().unwrap();
            let removed = reg.by_name.remove(name);
            if let Some(sub) = &removed {
                reg.name_by_id.remove(&sub.id);
            }
            removed
        };
        match removed {
            Some(sub) => {
                self.client.unsubscribe(sub.filter).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Danh sách tên các subscription đang hoạt động.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.registry.lock().unwrap().by_name.keys().cloned().collect();
        names.sort();
        names
    }

    /// Gửi message đến handler của subscription có identifier đi kèm message.
    pub fn dispatch(&self, publish: &Publish) {
        let topic = String::from_utf8_lossy(&publish.topic);
        let ids = publish
            .properties
            .as_ref()
            .map(|p| p.subscription_identifiers.as_slice())
            .unwrap_or_default();

        // Clone handler ra ngoài để không giữ lock khi chạy handler
        let matched: Vec<(String, Handler)> = {
            let reg = self.registry.lock().unwrap();
            ids.iter()
                .filter_map(|id| reg.name_by_id.get(id))
                .filter_map(|name| reg.by_name.get(name).map(|s| (name.clone(), s.handler.clone())))
                .collect()
        };

        for (name, handler) in matched {
            handler(&name, &topic, &publish.payload);
        }
    }

    /// Subscribe lại toàn bộ (dùng sau khi reconnect với clean session).
    pub async fn resubscribe_all(&self) -> Result<(), ClientError> {
        let subs: Vec<(String, QoS, usize)> = self
            .registry
            .lock()
            .unwrap()
            .by_name
            .values()
            .map(|s| (s.filter.clone(), s.qos, s.id))
            .collect();
        for (filter, qos, id) in subs {
            self.send_subscribe(&filter, qos, id).await?;
        }
        Ok(())
    }

    async fn send_subscribe(&self, filter: &str, qos: QoS, id: usize) -> Result<(), ClientError> {
        let props = SubscribeProperties {
            id: Some(id),
            user_properties: Vec::new(),
        };
        self.client.subscribe_with_properties(filter, qos, props).await
    }
}
