//! API key storage, persistence, and cryptographic operations.
use chrono::Utc;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use crate::admin_key_persistence::{persist_admin_key_file, PermissionFailureMode};

use super::{ApiKey, KeyApiResponse, VALID_ACLS};

const ADMIN_KEY_DESCRIPTION: &str = "Admin API Key";
const DEFAULT_SEARCH_KEY_DESCRIPTION: &str = "Default Search API Key";

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyStoreData {
    pub keys: Vec<ApiKey>,
    #[serde(default)]
    pub deleted_keys: Vec<ApiKey>,
}

pub struct KeyStore {
    pub(super) data: RwLock<KeyStoreData>,
    file_path: PathBuf,
    admin_key_value: RwLock<String>,
}

impl KeyStore {
    /// Load keys from `keys.json` in `data_dir`, or create default keys if the file is missing or corrupt.
    ///
    /// Automatically re-hashes the admin entry if `admin_key` differs from the stored hash, enabling key rotation via the `FLAPJACK_ADMIN_KEY` env var. Persists the result to disk on return.
    pub fn load_or_create(data_dir: &Path, admin_key: &str) -> Self {
        let file_path = data_dir.join("keys.json");
        let mut data = Self::load_key_store_data_or_default(&file_path, admin_key);
        Self::rotate_admin_entry_if_needed(&mut data, admin_key);

        let store = Self {
            data: RwLock::new(data),
            file_path,
            admin_key_value: RwLock::new(admin_key.to_string()),
        };
        store.save();
        store
    }

    fn load_key_store_data_or_default(file_path: &Path, admin_key: &str) -> KeyStoreData {
        if !file_path.exists() {
            return Self::create_default_keys(admin_key);
        }

        match Self::read_key_store_data(file_path) {
            Ok(data) => data,
            Err(error) => {
                tracing::warn!("{error}, recreating");
                Self::create_default_keys(admin_key)
            }
        }
    }

    fn read_key_store_data(file_path: &Path) -> Result<KeyStoreData, String> {
        let contents = std::fs::read_to_string(file_path)
            .map_err(|error| format!("Failed to read keys.json: {error}"))?;
        serde_json::from_str(&contents)
            .map_err(|error| format!("Failed to parse keys.json: {error}"))
    }

    fn rotate_admin_entry_if_needed(data: &mut KeyStoreData, admin_key: &str) {
        let Some(admin_entry) = admin_key_entry_mut(&mut data.keys) else {
            return;
        };

        if verify_key(admin_key, &admin_entry.hash, &admin_entry.salt) {
            return;
        }

        let new_salt = generate_salt();
        admin_entry.hash = hash_key(admin_key, &new_salt);
        admin_entry.salt = new_salt;
        tracing::info!("Admin key rotated");
    }

    /// Build the initial `KeyStoreData` with an admin key (all ACLs, no hmac_key) and a default search-only key (with hmac_key for secured key generation).
    fn create_default_keys(admin_key: &str) -> KeyStoreData {
        let now = Utc::now().timestamp_millis();
        let all_acls = VALID_ACLS.iter().map(|acl| (*acl).to_string()).collect();

        let admin_salt = generate_salt();
        let admin_hash = hash_key(admin_key, &admin_salt);

        let admin = ApiKey {
            hash: admin_hash,
            salt: admin_salt,
            hmac_key: None, // Admin keys should not be used for secured key generation
            created_at: now,
            acl: all_acls,
            description: ADMIN_KEY_DESCRIPTION.into(),
            indexes: vec![],
            max_hits_per_query: 0,
            max_queries_per_ip_per_hour: 0,
            query_parameters: String::new(),
            referers: vec![],
            restrict_sources: None,
            validity: 0,
        };

        let search_key_value = format!("fj_search_{}", generate_hex_key());
        let search_salt = generate_salt();
        let search_hash = hash_key(&search_key_value, &search_salt);

        let search_key = ApiKey {
            hash: search_hash,
            salt: search_salt,
            hmac_key: Some(search_key_value.clone()), // Store for HMAC verification of secured keys
            created_at: now,
            acl: vec!["search".into()],
            description: DEFAULT_SEARCH_KEY_DESCRIPTION.into(),
            indexes: vec![],
            max_hits_per_query: 0,
            max_queries_per_ip_per_hour: 0,
            query_parameters: String::new(),
            referers: vec![],
            restrict_sources: None,
            validity: 0,
        };

        KeyStoreData {
            keys: vec![admin, search_key],
            deleted_keys: vec![],
        }
    }

    /// Persist the current key data to `keys.json` with pretty-printed JSON and set file permissions to `0600` on Unix.
    fn save(&self) {
        let data = self.data.read().unwrap();
        if let Err(error) = persist_key_store_data(&self.file_path, &data) {
            tracing::warn!("Failed to save keys.json: {}", error);
        }
    }

    pub fn is_admin(&self, key_value: &str) -> bool {
        use subtle::ConstantTimeEq;
        let admin_key = self.admin_key_value.read().unwrap();
        let a = key_value.as_bytes();
        let b = admin_key.as_bytes();
        // Constant-time comparison to prevent timing side-channel attacks.
        // Length mismatch leaks length info but not content — acceptable tradeoff.
        a.len() == b.len() && a.ct_eq(b).into()
    }

    pub fn lookup(&self, key_value: &str) -> Option<ApiKey> {
        let data = self.data.read().unwrap();
        data.keys
            .iter()
            .find(|k| verify_key(key_value, &k.hash, &k.salt))
            .cloned()
    }

    pub fn list_all(&self) -> Vec<ApiKey> {
        let data = self.data.read().unwrap();
        data.keys.clone()
    }

    /// List all keys as safe API response DTOs with `value` populated.
    /// Admin key: value from `admin_key_value`. Non-admin keys: value from `hmac_key`.
    pub fn list_all_as_dto(&self) -> Vec<KeyApiResponse> {
        let admin_key = self.admin_key_value.read().unwrap();
        let data = self.data.read().unwrap();
        data.keys
            .iter()
            .map(|key| KeyApiResponse::from_api_key(key, dto_key_value(key, &admin_key)))
            .collect()
    }

    /// Look up a key and return it as a safe API response DTO.
    /// The key_value path param is used as the `value` field.
    pub fn lookup_as_dto(&self, key_value: &str) -> Option<KeyApiResponse> {
        self.lookup(key_value)
            .map(|key| KeyApiResponse::from_api_key(&key, key_value.to_string()))
    }

    /// Creates a new key and returns the plaintext value (only time it's visible)
    /// The key is hashed before storage
    pub fn create_key(&self, mut key: ApiKey) -> (ApiKey, String) {
        let plaintext_value = format!("fj_search_{}", generate_hex_key());
        let salt = generate_salt();
        let hash = hash_key(&plaintext_value, &salt);

        key.hash = hash;
        key.salt = salt;
        key.created_at = Utc::now().timestamp_millis();
        // Store hmac_key for secured key support (except for admin-like keys)
        key.hmac_key = Some(plaintext_value.clone());

        let mut data = self.data.write().unwrap();
        data.keys.push(key.clone());
        drop(data);
        self.save();

        (key, plaintext_value)
    }

    /// Update a key's mutable fields (ACL, description, indexes, etc.) while preserving its hash, salt, hmac_key, and creation timestamp.
    ///
    /// Returns the updated key on success, or `None` if no key matches `key_value`.
    pub fn update_key(&self, key_value: &str, mut updated: ApiKey) -> Option<ApiKey> {
        let mut data = self.data.write().unwrap();
        if let Some(existing) = data
            .keys
            .iter_mut()
            .find(|k| verify_key(key_value, &k.hash, &k.salt))
        {
            // Preserve hash, salt, hmac_key, and creation time
            updated.hash = existing.hash.clone();
            updated.salt = existing.salt.clone();
            updated.hmac_key = existing.hmac_key.clone();
            updated.created_at = existing.created_at;
            *existing = updated.clone();
            drop(data);
            self.save();
            Some(updated)
        } else {
            None
        }
    }

    /// Soft-delete a key by moving it from the active set to `deleted_keys`.
    ///
    /// Refuses to delete the admin key. Returns `true` if a key was deleted, `false` if the key was not found or is the admin key.
    pub fn delete_key(&self, key_value: &str) -> bool {
        let mut data = self.data.write().unwrap();

        // Check if this is the admin key and prevent deletion
        if let Some(admin) = admin_key_entry(&data.keys) {
            if verify_key(key_value, &admin.hash, &admin.salt) {
                return false;
            }
        }

        // Find and delete the key
        if let Some(pos) = data
            .keys
            .iter()
            .position(|k| verify_key(key_value, &k.hash, &k.salt))
        {
            let removed = data.keys.remove(pos);
            data.deleted_keys.push(removed);
            drop(data);
            self.save();
            true
        } else {
            false
        }
    }

    /// Move a previously deleted key from `deleted_keys` back into the active key set and persist the change.
    ///
    /// Returns the restored `ApiKey` on success, or `None` if no matching deleted key is found.
    pub fn restore_key(&self, key_value: &str) -> Option<ApiKey> {
        let mut data = self.data.write().unwrap();
        if let Some(pos) = data
            .deleted_keys
            .iter()
            .position(|k| verify_key(key_value, &k.hash, &k.salt))
        {
            let restored = data.deleted_keys.remove(pos);
            data.keys.push(restored.clone());
            drop(data);
            self.save();
            Some(restored)
        } else {
            None
        }
    }

    pub fn admin_key_value(&self) -> String {
        self.admin_key_value.read().unwrap().clone()
    }

    /// Generate a new admin key, update in-memory state (admin_key_value + keys.json hash),
    /// and persist to both `keys.json` and `.admin_key` on disk. Returns the new plaintext key.
    ///
    /// Disk writes happen before in-memory updates so that an I/O failure leaves the
    /// running process in its original consistent state (no admin lockout).
    pub fn rotate_admin_key(&self) -> Result<String, String> {
        let new_key = generate_admin_key();
        let new_salt = generate_salt();
        let new_hash = hash_key(&new_key, &new_salt);

        // Persist .admin_key FIRST — if this fails, nothing has changed.
        let data_dir = self
            .file_path
            .parent()
            .ok_or("Cannot determine data directory from keys.json path")?;
        persist_admin_key_file(
            &data_dir.join(".admin_key"),
            &new_key,
            PermissionFailureMode::WarnAndContinue,
        )?;

        // Update in-memory state atomically (admin_key first, then data — matches
        // list_all_as_dto() lock ordering to prevent reader inconsistency).
        {
            let mut admin_key = self.admin_key_value.write().unwrap();
            let mut data = self.data.write().unwrap();
            let admin_entry =
                admin_key_entry_mut(&mut data.keys).ok_or("No admin key found in key store")?;
            admin_entry.hash = new_hash;
            admin_entry.salt = new_salt;
            *admin_key = new_key.clone();
        }

        // Persist keys.json — if this fails (save() only warns), the new .admin_key
        // and in-memory state are consistent; rotate_admin_entry_if_needed() will
        // reconcile keys.json on next restart.
        self.save();

        tracing::info!("Admin key rotated at runtime");
        Ok(new_key)
    }
}

pub fn generate_hex_key() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 16] = rng.gen();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Generate a random salt for key hashing
fn generate_salt() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 32] = rng.gen();
    hex::encode(bytes)
}

/// Hash a key value with a salt using SHA-256
pub(crate) fn hash_key(key_value: &str, salt: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(key_value.as_bytes());
    hex::encode(hasher.finalize())
}

/// Verify a key value against a stored hash and salt using constant-time comparison
pub(crate) fn verify_key(key_value: &str, stored_hash: &str, salt: &str) -> bool {
    let computed_hash = hash_key(key_value, salt);
    // Constant-time comparison to prevent timing attacks
    if computed_hash.len() != stored_hash.len() {
        return false;
    }
    let mut result = 0u8;
    for (a, b) in computed_hash.bytes().zip(stored_hash.bytes()) {
        result |= a ^ b;
    }
    result == 0
}

/// Generate a prefixed admin key (fj_admin_ + 32 hex chars).
pub fn generate_admin_key() -> String {
    format!("fj_admin_{}", generate_hex_key())
}

/// Read the admin key from an existing keys.json, if one exists.
/// NOTE: With hashed keys, this can no longer return the plaintext value.
/// This function is deprecated and always returns None.
/// The admin key must be provided via FLAPJACK_ADMIN_KEY env var.
#[deprecated(note = "Admin keys are now hashed at rest. Use FLAPJACK_ADMIN_KEY env var.")]
pub fn read_existing_admin_key(_data_dir: &Path) -> Option<String> {
    None
}

/// Generate a new admin key and update both .admin_key file and keys.json. Returns the new key.
pub fn reset_admin_key(data_dir: &Path) -> Result<String, String> {
    let file_path = data_dir.join("keys.json");
    if !file_path.exists() {
        return Err("No keys.json found. Start the server first to initialize.".into());
    }

    let contents = std::fs::read_to_string(&file_path)
        .map_err(|e| format!("Failed to read keys.json: {}", e))?;
    let mut data: KeyStoreData =
        serde_json::from_str(&contents).map_err(|e| format!("Failed to parse keys.json: {}", e))?;

    let new_key = generate_admin_key();
    let new_salt = generate_salt();
    let new_hash = hash_key(&new_key, &new_salt);

    if let Some(admin) = admin_key_entry_mut(&mut data.keys) {
        admin.hash = new_hash;
        admin.salt = new_salt;
    } else {
        return Err("No admin key found in keys.json.".into());
    }

    persist_key_store_data(&file_path, &data)?;

    persist_admin_key_file(
        &data_dir.join(".admin_key"),
        &new_key,
        PermissionFailureMode::WarnAndContinue,
    )?;

    Ok(new_key)
}

fn admin_key_entry(keys: &[ApiKey]) -> Option<&ApiKey> {
    keys.iter().find(|key| is_admin_key_entry(key))
}

fn admin_key_entry_mut(keys: &mut [ApiKey]) -> Option<&mut ApiKey> {
    keys.iter_mut().find(|key| is_admin_key_entry(key))
}

fn dto_key_value(key: &ApiKey, admin_key: &str) -> String {
    if verify_key(admin_key, &key.hash, &key.salt) {
        admin_key.to_string()
    } else {
        key.hmac_key.clone().unwrap_or_default()
    }
}

fn is_admin_key_entry(key: &ApiKey) -> bool {
    key.description == ADMIN_KEY_DESCRIPTION
}

fn persist_key_store_data(file_path: &Path, data: &KeyStoreData) -> Result<(), String> {
    let json = serde_json::to_string_pretty(data)
        .map_err(|error| format!("Failed to serialize keys.json: {error}"))?;

    if let Some(parent) = file_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    std::fs::write(file_path, &json)
        .map_err(|error| format!("Failed to write keys.json: {error}"))?;
    ensure_keys_file_permissions(file_path);
    Ok(())
}

fn ensure_keys_file_permissions(file_path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Err(error) =
            std::fs::set_permissions(file_path, std::fs::Permissions::from_mode(0o600))
        {
            tracing::warn!("Failed to set keys.json permissions: {}", error);
        }
    }
}
