//! In-memory conversation store for multi-turn RAG chat with bounded history and TTL-based eviction.

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A single message in a conversation.
#[derive(Debug, Clone)]
pub struct ConversationMessage {
    pub role: String,
    pub content: String,
}

/// Stored state for one conversation.
struct ConversationState {
    messages: VecDeque<ConversationMessage>,
    last_access: Instant,
}

/// Shared, thread-safe in-memory conversation store.
///
/// - `max_messages`: maximum messages retained per conversation (oldest are evicted).
/// - `ttl`: conversations not accessed within this duration are considered expired and
///   are pruned on the next `get_or_create` call.
pub struct ConversationStore {
    conversations: DashMap<String, ConversationState>,
    max_messages: usize,
    ttl: Duration,
}

impl ConversationStore {
    pub fn new(max_messages: usize, ttl: Duration) -> Self {
        // Keep a pair-aligned cap when possible so trimming does not split turns.
        let max_messages = pair_aligned_cap(max_messages);
        Self {
            conversations: DashMap::new(),
            max_messages,
            ttl,
        }
    }

    /// Create a new `ConversationStore` wrapped in `Arc` with production defaults.
    ///
    /// Defaults: 10 messages per conversation, 1-hour TTL.
    pub fn default_shared() -> Arc<Self> {
        Arc::new(Self::new(10, Duration::from_secs(3600)))
    }

    /// Return the current bounded history for `conversation_id`, or an empty vec if absent/expired.
    pub fn get_history(&self, conversation_id: &str) -> Vec<ConversationMessage> {
        if let Some(mut entry) = self.conversations.get_mut(conversation_id) {
            if entry.last_access.elapsed() > self.ttl {
                drop(entry);
                self.conversations.remove(conversation_id);
                return vec![];
            }
            entry.last_access = Instant::now();
            entry.messages.iter().cloned().collect()
        } else {
            vec![]
        }
    }

    /// Append a user turn and assistant reply to the conversation.
    ///
    /// Creates the conversation if it does not exist. Enforces the `max_messages` cap
    /// by dropping the oldest messages (always in pairs to keep turn integrity when possible).
    pub fn append_exchange(
        &self,
        conversation_id: &str,
        user_message: String,
        assistant_message: String,
    ) {
        let mut entry = self
            .conversations
            .entry(conversation_id.to_string())
            .or_insert_with(|| ConversationState {
                messages: VecDeque::new(),
                last_access: Instant::now(),
            });
        entry.last_access = Instant::now();
        entry.messages.push_back(ConversationMessage {
            role: "user".to_string(),
            content: user_message,
        });
        entry.messages.push_back(ConversationMessage {
            role: "assistant".to_string(),
            content: assistant_message,
        });
        // Evict oldest messages, always in pairs to preserve turn structure.
        while entry.messages.len() > self.max_messages {
            entry.messages.pop_front();
            if entry.messages.len() > self.max_messages {
                entry.messages.pop_front();
            }
        }
    }

    /// Evict all conversations that have exceeded the TTL.
    ///
    /// Can be called periodically to reclaim memory without blocking active requests.
    pub fn evict_expired(&self) {
        let ttl = self.ttl;
        self.conversations
            .retain(|_, state| state.last_access.elapsed() <= ttl);
    }
}

fn pair_aligned_cap(max_messages: usize) -> usize {
    if max_messages > 1 && max_messages % 2 == 1 {
        max_messages - 1
    } else {
        max_messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_history_returns_empty_for_unknown_conversation() {
        let store = ConversationStore::new(10, Duration::from_secs(60));
        assert!(store.get_history("nonexistent").is_empty());
    }

    #[test]
    fn append_exchange_stores_messages_in_order() {
        let store = ConversationStore::new(10, Duration::from_secs(60));
        store.append_exchange("conv1", "hello".to_string(), "hi there".to_string());
        let history = store.get_history("conv1");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].role, "user");
        assert_eq!(history[0].content, "hello");
        assert_eq!(history[1].role, "assistant");
        assert_eq!(history[1].content, "hi there");
    }

    #[test]
    fn append_exchange_caps_at_max_messages() {
        // max_messages = 4 means 2 exchange pairs maximum.
        let store = ConversationStore::new(4, Duration::from_secs(60));
        store.append_exchange("c", "q1".to_string(), "a1".to_string());
        store.append_exchange("c", "q2".to_string(), "a2".to_string());
        store.append_exchange("c", "q3".to_string(), "a3".to_string());
        let history = store.get_history("c");
        // Should retain only the most recent 4 messages (q2/a2/q3/a3).
        assert_eq!(history.len(), 4, "should cap at max_messages");
        assert_eq!(history[0].content, "q2");
        assert_eq!(history[3].content, "a3");
    }

    /// Verify that an odd `max_messages` cap is rounded down to the nearest even number so that eviction never splits a user/assistant pair.
    #[test]
    fn append_exchange_odd_cap_preserves_turn_pairs() {
        // max_messages = 5 cannot hold whole pairs; keep the most recent complete pairs only.
        let store = ConversationStore::new(5, Duration::from_secs(60));
        store.append_exchange("c", "q1".to_string(), "a1".to_string());
        store.append_exchange("c", "q2".to_string(), "a2".to_string());
        store.append_exchange("c", "q3".to_string(), "a3".to_string());

        let history = store.get_history("c");
        assert_eq!(
            history.len(),
            4,
            "odd message cap should not split a user/assistant pair"
        );
        assert_eq!(history[0].role, "user");
        assert_eq!(history[0].content, "q2");
        assert_eq!(history[3].role, "assistant");
        assert_eq!(history[3].content, "a3");
    }

    #[test]
    fn get_history_returns_empty_after_ttl_expired() {
        let store = ConversationStore::new(10, Duration::from_millis(1));
        store.append_exchange("c", "q".to_string(), "a".to_string());
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            store.get_history("c").is_empty(),
            "expired conversation should return empty"
        );
    }

    #[test]
    fn evict_expired_removes_stale_entries() {
        let store = ConversationStore::new(10, Duration::from_millis(1));
        store.append_exchange("old", "q".to_string(), "a".to_string());
        std::thread::sleep(Duration::from_millis(5));
        store.evict_expired();
        assert!(
            store.conversations.is_empty(),
            "stale entry should be removed"
        );
    }

    #[test]
    fn pair_aligned_cap_rounds_down_only_when_possible() {
        assert_eq!(pair_aligned_cap(0), 0);
        assert_eq!(pair_aligned_cap(1), 1);
        assert_eq!(pair_aligned_cap(4), 4);
        assert_eq!(pair_aligned_cap(5), 4);
    }
}
