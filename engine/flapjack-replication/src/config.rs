use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub bind_addr: String,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub node_id: String,
    pub addr: String, // e.g., "http://10.0.1.2:7700" or "http://node-b:7700"
}

impl NodeConfig {
    /// Load node configuration from {data_dir}/node.json or return standalone default
    pub fn load_or_default(data_dir: &Path) -> Self {
        let node_json = data_dir.join("node.json");
        if let Some(config) = Self::load_from_file(&node_json) {
            return config;
        }

        let config = Self {
            node_id: Self::default_node_id(),
            bind_addr: Self::default_bind_addr(),
            peers: Self::parse_env_peers(),
        };
        Self::log_default_source(&config);
        config
    }

    /// Read `{data_dir}/node.json` if it exists, returning `None` on read or parse errors.
    fn load_from_file(node_json: &Path) -> Option<Self> {
        if !node_json.exists() {
            return None;
        }

        let content = match std::fs::read_to_string(node_json) {
            Ok(content) => content,
            Err(error) => {
                tracing::error!("Failed to read node.json: {}, using defaults", error);
                return None;
            }
        };

        match serde_json::from_str::<NodeConfig>(&content) {
            Ok(mut config) => {
                config.peers = config
                    .peers
                    .into_iter()
                    .filter_map(|peer| {
                        Self::normalize_peer_addr(&peer.addr).map(|normalized_addr| PeerConfig {
                            node_id: peer.node_id.trim().to_string(),
                            addr: normalized_addr,
                        })
                    })
                    .filter(|peer| !peer.node_id.is_empty())
                    .collect();
                tracing::info!(
                    "Loaded node config: node_id={}, peers={}",
                    config.node_id,
                    config.peers.len()
                );
                Some(config)
            }
            Err(error) => {
                tracing::error!("Failed to parse node.json: {}, using defaults", error);
                None
            }
        }
    }

    fn default_node_id() -> String {
        std::env::var("FLAPJACK_NODE_ID").unwrap_or_else(|_| {
            hostname::get()
                .ok()
                .and_then(|hostname| hostname.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string())
        })
    }

    fn default_bind_addr() -> String {
        std::env::var("FLAPJACK_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:7700".to_string())
    }

    fn parse_env_peers() -> Vec<PeerConfig> {
        std::env::var("FLAPJACK_PEERS")
            .unwrap_or_default()
            .split(',')
            .filter_map(Self::parse_peer_entry)
            .collect()
    }

    /// Parse one `FLAPJACK_PEERS` entry in `node_id=addr` form, ignoring malformed values.
    fn parse_peer_entry(peer: &str) -> Option<PeerConfig> {
        if peer.trim().is_empty() {
            return None;
        }

        let mut parts = peer.splitn(2, '=');
        let peer_id = parts.next()?.trim().to_string();
        let addr = parts.next()?.trim();
        if peer_id.is_empty() || addr.is_empty() {
            return None;
        }
        let normalized_addr = Self::normalize_peer_addr(addr)?;

        Some(PeerConfig {
            node_id: peer_id,
            addr: normalized_addr,
        })
    }

    fn normalize_peer_addr(addr: &str) -> Option<String> {
        let parsed = reqwest::Url::parse(addr).ok()?;
        match parsed.scheme() {
            "http" | "https" => {}
            _ => return None,
        }

        let host = parsed.host_str()?;
        if host.eq_ignore_ascii_case("localhost") {
            return None;
        }
        // Literal-IP hosts are checked directly. Non-IP hosts (real hostnames AND
        // non-canonical numeric forms like `2130706433` that the IP parser rejects
        // but the OS resolver maps to loopback/link-local) are resolved and every
        // resolved address is checked. Operator-configured replication peers may
        // legitimately live on RFC1918/ULA private networks, so only local-host /
        // metadata-style destinations are blocked here.
        if let Ok(ip) = host.parse::<IpAddr>() {
            if Self::is_unsafe_peer_ip(&ip) {
                return None;
            }
        } else if Self::first_blocked_resolved_ip(host, parsed.port_or_known_default()).is_some() {
            return None;
        }

        Some(parsed.as_str().trim_end_matches('/').to_string())
    }

    /// Resolve a non-literal peer host and return the first resolved address that
    /// is unsafe for peer fan-out, or None. Resolution failure returns None
    /// (peer addresses commonly use docker/k8s service names that only resolve at
    /// connect time), so config intake must not require live DNS.
    fn first_blocked_resolved_ip(host: &str, port: Option<u16>) -> Option<IpAddr> {
        use std::net::ToSocketAddrs;
        (host, port.unwrap_or(0))
            .to_socket_addrs()
            .ok()?
            .map(|sa| sa.ip())
            .find(Self::is_unsafe_peer_ip)
    }

    fn is_unsafe_peer_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback() || v4.is_link_local() || v4.is_broadcast() || v4.is_unspecified()
            }
            IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_unicast_link_local()
                    || v6.to_ipv4_mapped().is_some_and(|v4| {
                        v4.is_loopback()
                            || v4.is_link_local()
                            || v4.is_broadcast()
                            || v4.is_unspecified()
                    })
            }
        }
    }

    fn log_default_source(config: &NodeConfig) {
        if config.peers.is_empty() {
            tracing::info!(
                "No node.json found, running in standalone mode: node_id={}",
                config.node_id
            );
        } else {
            tracing::info!(
                "No node.json found, loaded {} peer(s) from FLAPJACK_PEERS: node_id={}",
                config.peers.len(),
                config.node_id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Tests that mutate global env vars must not run in parallel — they share
    // process-wide state. Serialize them with this mutex instead of adding a
    // new `serial_test` dev-dependency.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_load_or_default_no_file() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();

        // Ensure no env var pollution from other tests
        std::env::remove_var("FLAPJACK_PEERS");
        std::env::remove_var("FLAPJACK_NODE_ID");

        let config = NodeConfig::load_or_default(temp_dir.path());

        // Should use defaults
        assert_eq!(config.peers.len(), 0);
        assert!(!config.node_id.is_empty());
    }

    /// Verify that a well-formed `node.json` file is parsed and its node ID, bind address, and peer list are returned verbatim.
    #[test]
    fn test_load_or_default_valid_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let node_json_path = temp_dir.path().join("node.json");

        let config_str = r#"{
            "node_id": "test-node",
            "bind_addr": "0.0.0.0:7700",
            "peers": [
                {"node_id": "peer-1", "addr": "http://peer1:7700"}
            ]
        }"#;

        let mut file = std::fs::File::create(&node_json_path).unwrap();
        file.write_all(config_str.as_bytes()).unwrap();

        let config = NodeConfig::load_or_default(temp_dir.path());

        assert_eq!(config.node_id, "test-node");
        assert_eq!(config.bind_addr, "0.0.0.0:7700");
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].node_id, "peer-1");
        assert_eq!(config.peers[0].addr, "http://peer1:7700");
    }

    /// Verify that a malformed `node.json` file is gracefully ignored, falling back to standalone defaults with no peers.
    #[test]
    fn test_load_or_default_invalid_json() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let node_json_path = temp_dir.path().join("node.json");

        // Ensure no env var pollution from other tests
        std::env::remove_var("FLAPJACK_PEERS");
        std::env::remove_var("FLAPJACK_NODE_ID");

        let mut file = std::fs::File::create(&node_json_path).unwrap();
        file.write_all(b"invalid json").unwrap();

        let config = NodeConfig::load_or_default(temp_dir.path());

        // Should fall back to defaults
        assert_eq!(config.peers.len(), 0);
    }

    /// Verify that `FLAPJACK_NODE_ID` and `FLAPJACK_PEERS` environment variables are used to construct the config when no `node.json` exists, including correct parsing of multiple comma-separated `id=addr` pairs.
    #[test]
    fn test_load_or_default_flapjack_peers_env_var() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();

        std::env::set_var("FLAPJACK_NODE_ID", "test-node-env");
        std::env::set_var(
            "FLAPJACK_PEERS",
            "node-b=http://node-b:7700,node-c=http://node-c:7701",
        );

        let config = NodeConfig::load_or_default(temp_dir.path());

        std::env::remove_var("FLAPJACK_NODE_ID");
        std::env::remove_var("FLAPJACK_PEERS");

        assert_eq!(config.node_id, "test-node-env");
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0].node_id, "node-b");
        assert_eq!(config.peers[0].addr, "http://node-b:7700");
        assert_eq!(config.peers[1].node_id, "node-c");
        assert_eq!(config.peers[1].addr, "http://node-c:7701");
    }

    /// Verify that a single-entry `FLAPJACK_PEERS` value (no trailing comma) is parsed into exactly one peer with the expected node ID and IP-based address.
    #[test]
    fn test_load_or_default_single_peer_env() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();

        std::env::set_var("FLAPJACK_NODE_ID", "node-a-single");
        std::env::set_var("FLAPJACK_PEERS", "node-b=http://node-b.example.com:7700");

        let config = NodeConfig::load_or_default(temp_dir.path());

        std::env::remove_var("FLAPJACK_NODE_ID");
        std::env::remove_var("FLAPJACK_PEERS");

        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].node_id, "node-b");
        assert_eq!(config.peers[0].addr, "http://node-b.example.com:7700");
    }

    #[test]
    fn test_load_or_default_empty_peers_env() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();

        std::env::set_var("FLAPJACK_PEERS", "");

        let config = NodeConfig::load_or_default(temp_dir.path());

        std::env::remove_var("FLAPJACK_PEERS");

        assert_eq!(config.peers.len(), 0);
    }

    /// Verify that a valid `node.json` file takes precedence over `FLAPJACK_NODE_ID` and `FLAPJACK_PEERS` environment variables set at the same time.
    #[test]
    fn test_node_json_takes_precedence_over_env() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let node_json_path = temp_dir.path().join("node.json");

        let config_str = r#"{
            "node_id": "from-json",
            "bind_addr": "0.0.0.0:7700",
            "peers": [
                {"node_id": "peer-json", "addr": "http://peer-json:7700"}
            ]
        }"#;

        let mut file = std::fs::File::create(&node_json_path).unwrap();
        file.write_all(config_str.as_bytes()).unwrap();

        std::env::set_var("FLAPJACK_NODE_ID", "from-env");
        std::env::set_var("FLAPJACK_PEERS", "peer-env=http://peer-env:7700");

        let config = NodeConfig::load_or_default(temp_dir.path());

        std::env::remove_var("FLAPJACK_NODE_ID");
        std::env::remove_var("FLAPJACK_PEERS");

        // node.json takes precedence
        assert_eq!(config.node_id, "from-json");
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].node_id, "peer-json");
    }

    #[test]
    fn a10_env_peer_parser_rejects_unsafe_or_malformed_peer_addresses() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();

        std::env::set_var("FLAPJACK_NODE_ID", "node-a-a10");
        // Covers every blocked class. RFC1918 peers remain allowed because
        // replication/analytics fan-out is explicitly configured for internal
        // cluster destinations; only metadata/local-host style targets are unsafe.
        // Classes blocked here:
        //   - link-local metadata, loopback
        //   - non-http scheme
        //   - numeric-form `2130706433` (url crate canonicalizes to 127.0.0.1,
        //     caught by the literal-IP check; asserts canonicalization holds)
        //   - `localhost.` trailing-dot FQDN (kept as a registered name by the url
        //     crate, slips past literal + "localhost" checks; only resolve-and-check
        //     catches it — the hostname-resolution SSRF bypass the fix closes)
        std::env::set_var(
            "FLAPJACK_PEERS",
            "meta=http://169.254.169.254,loop=http://127.0.0.1:7700,priv=http://10.0.0.1:7700,bad=file:///tmp/x,numeric=http://2130706433:7700,lhdot=http://localhost.:7700,ok=https://peer-a.example.com:7700",
        );

        let config = NodeConfig::load_or_default(temp_dir.path());

        std::env::remove_var("FLAPJACK_NODE_ID");
        std::env::remove_var("FLAPJACK_PEERS");

        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0].node_id, "priv");
        assert_eq!(config.peers[0].addr, "http://10.0.0.1:7700");
        assert_eq!(config.peers[1].node_id, "ok");
        assert_eq!(config.peers[1].addr, "https://peer-a.example.com:7700");
    }

    #[test]
    fn a10_node_json_filters_unsafe_peer_addresses() {
        let _guard = ENV_MUTEX.lock().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let node_json_path = temp_dir.path().join("node.json");

        let config_str = r#"{
                "node_id": "json-node",
                "bind_addr": "0.0.0.0:7700",
                "peers": [
                    {"node_id": "unsafe-meta", "addr": "http://169.254.169.254"},
                    {"node_id": "unsafe-scheme", "addr": "file:///tmp/peer"},
                    {"node_id": "private-ok", "addr": "http://10.0.0.2:7700"},
                    {"node_id": "safe", "addr": "http://peer-safe.example.com:7700"}
                ]
            }"#;

        let mut file = std::fs::File::create(&node_json_path).unwrap();
        file.write_all(config_str.as_bytes()).unwrap();

        let config = NodeConfig::load_or_default(temp_dir.path());
        assert_eq!(config.node_id, "json-node");
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0].node_id, "private-ok");
        assert_eq!(config.peers[0].addr, "http://10.0.0.2:7700");
        assert_eq!(config.peers[1].node_id, "safe");
        assert_eq!(config.peers[1].addr, "http://peer-safe.example.com:7700");
    }
}
