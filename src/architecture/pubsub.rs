use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PubSubMessageCore {
    pub topic: String,
    pub payload: Vec<u8>,
    pub offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PubSubSubscriptionCore {
    pub name: String,
    pub topic: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PubSubDeliveryCore {
    pub topic: String,
    pub offset: u64,
    pub delivered_to: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SubscriberState {
    topic: String,
    next_offset: u64,
}

#[derive(Clone, Debug)]
pub struct InMemoryPubSubCore {
    records: VecDeque<PubSubMessageCore>,
    subscribers: BTreeMap<String, SubscriberState>,
    retained_messages: usize,
    next_offset: u64,
    next_subscription_id: u64,
}

impl InMemoryPubSubCore {
    pub fn new(retained_messages: usize) -> Result<Self, String> {
        if retained_messages == 0 {
            return Err("retained_messages must be positive".to_string());
        }
        Ok(Self {
            records: VecDeque::new(),
            subscribers: BTreeMap::new(),
            retained_messages,
            next_offset: 0,
            next_subscription_id: 1,
        })
    }

    pub fn retained_messages(&self) -> usize {
        self.retained_messages
    }

    pub fn message_count(&self) -> usize {
        self.records.len()
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    pub fn subscribe(
        &mut self,
        topic: String,
        name: Option<String>,
        replay_from_beginning: bool,
    ) -> Result<PubSubSubscriptionCore, String> {
        validate_topic(&topic)?;
        let subscription_name = match name {
            Some(name) => {
                validate_subscription_name(&name)?;
                name
            }
            None => self.next_subscription_name(),
        };
        if self.subscribers.contains_key(&subscription_name) {
            return Err(format!("duplicate subscription name: {subscription_name}"));
        }
        let next_offset = if replay_from_beginning {
            self.records
                .front()
                .map_or(self.next_offset, |record| record.offset)
        } else {
            self.next_offset
        };
        self.subscribers.insert(
            subscription_name.clone(),
            SubscriberState {
                topic: topic.clone(),
                next_offset,
            },
        );
        Ok(PubSubSubscriptionCore {
            name: subscription_name,
            topic,
        })
    }

    pub fn unsubscribe(&mut self, name: &str) -> Result<bool, String> {
        validate_subscription_name(name)?;
        Ok(self.subscribers.remove(name).is_some())
    }

    pub fn publish(
        &mut self,
        topic: String,
        payload: Vec<u8>,
    ) -> Result<PubSubDeliveryCore, String> {
        validate_topic(&topic)?;
        if payload.is_empty() {
            return Err("pubsub payload must be non-empty bytes".to_string());
        }
        let offset = self.next_offset;
        self.next_offset += 1;
        self.records.push_back(PubSubMessageCore {
            topic: topic.clone(),
            payload,
            offset,
        });
        while self.records.len() > self.retained_messages {
            self.records.pop_front();
        }
        Ok(PubSubDeliveryCore {
            delivered_to: self
                .subscribers
                .iter()
                .filter(|(_, subscriber)| topic_matches(&subscriber.topic, &topic))
                .map(|(name, _)| name.clone())
                .collect(),
            topic,
            offset,
        })
    }

    pub fn poll(
        &mut self,
        subscription: &str,
        max_messages: Option<usize>,
    ) -> Result<Vec<PubSubMessageCore>, String> {
        validate_subscription_name(subscription)?;
        let Some(subscriber) = self.subscribers.get_mut(subscription) else {
            return Err(format!("unknown subscription: {subscription}"));
        };
        if matches!(max_messages, Some(0)) {
            return Err("max_messages must be positive when provided".to_string());
        }
        let limit = max_messages.unwrap_or(usize::MAX);
        let mut messages = Vec::new();
        for record in &self.records {
            if record.offset < subscriber.next_offset {
                continue;
            }
            if !topic_matches(&subscriber.topic, &record.topic) {
                continue;
            }
            messages.push(record.clone());
            subscriber.next_offset = record.offset + 1;
            if messages.len() >= limit {
                break;
            }
        }
        Ok(messages)
    }

    pub fn latest(&self, topic: Option<&str>) -> Result<Option<PubSubMessageCore>, String> {
        if let Some(topic) = topic {
            validate_topic(topic)?;
        }
        Ok(self
            .records
            .iter()
            .rev()
            .find(|record| topic.is_none_or(|topic| topic_matches(topic, &record.topic)))
            .cloned())
    }

    pub fn topic_offsets(&self) -> BTreeMap<String, u64> {
        let mut offsets = BTreeMap::new();
        for record in &self.records {
            offsets.insert(record.topic.clone(), record.offset);
        }
        offsets
    }

    fn next_subscription_name(&mut self) -> String {
        let name = format!("subscription-{}", self.next_subscription_id);
        self.next_subscription_id += 1;
        name
    }
}

fn topic_matches(subscription_topic: &str, message_topic: &str) -> bool {
    subscription_topic == "*" || subscription_topic == message_topic
}

fn validate_topic(topic: &str) -> Result<(), String> {
    if topic.trim().is_empty() {
        return Err("pubsub topic must be a non-empty string".to_string());
    }
    Ok(())
}

fn validate_subscription_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err("pubsub subscription name must be a non-empty string".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_and_poll_named_subscription() {
        let mut pubsub = InMemoryPubSubCore::new(8).expect("valid pubsub");
        let subscription = pubsub
            .subscribe(
                "orders.created".to_string(),
                Some("orders".to_string()),
                false,
            )
            .expect("valid subscription");

        let delivery = pubsub
            .publish("orders.created".to_string(), b"order-123".to_vec())
            .expect("valid publish");
        let messages = pubsub
            .poll(&subscription.name, None)
            .expect("subscription can poll");

        assert_eq!(delivery.delivered_to, vec!["orders".to_string()]);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, b"order-123");
        assert_eq!(messages[0].offset, 0);
    }

    #[test]
    fn retained_replay_starts_at_oldest_retained_message() {
        let mut pubsub = InMemoryPubSubCore::new(2).expect("valid pubsub");
        pubsub
            .publish("events".to_string(), b"first".to_vec())
            .expect("valid publish");
        pubsub
            .publish("events".to_string(), b"second".to_vec())
            .expect("valid publish");
        pubsub
            .publish("events".to_string(), b"third".to_vec())
            .expect("valid publish");
        pubsub
            .subscribe("events".to_string(), Some("replay".to_string()), true)
            .expect("valid replay subscription");

        let messages = pubsub.poll("replay", None).expect("subscription can poll");

        assert_eq!(
            messages
                .iter()
                .map(|message| message.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"second".as_slice(), b"third".as_slice()],
        );
        assert_eq!(
            messages
                .iter()
                .map(|message| message.offset)
                .collect::<Vec<_>>(),
            vec![1, 2],
        );
    }
}
