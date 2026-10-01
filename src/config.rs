use serde::{Deserialize, Serialize};

/// Viewer tuning values, read from `config.json` → `module_specific`. Every
/// field is defaulted so a missing key falls back to the shipped value; the
/// loader writes any missing keys back into `config.json` so they are always
/// present and editable in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// How often the poll task refreshes the timeline + module list (seconds).
    pub refresh_interval_secs: u64,
    /// Deadline for a db_query / timeline_query send AND response wait (seconds).
    pub query_timeout_secs: u64,
    /// Cap on the live engine/module log buffer.
    pub live_log_cap: usize,
    /// `LIMIT` for the archival log query.
    pub log_limit: i64,
    /// `LIMIT` for the messages query.
    pub messages_limit: i64,
    /// `LIMIT` for the errors query.
    pub errors_limit: i64,
    /// `LIMIT` for the audit (held) query.
    pub audit_limit: i64,
    /// Initial reconnect backoff (seconds).
    pub reconnect_base_secs: u64,
    /// Reconnect backoff cap (seconds).
    pub reconnect_max_secs: u64,
    /// Capacity of the engine result broadcast channel.
    pub broadcast_cap: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            refresh_interval_secs: 3,
            query_timeout_secs: 3,
            live_log_cap: 500,
            log_limit: 40,
            messages_limit: 60,
            errors_limit: 40,
            audit_limit: 40,
            reconnect_base_secs: 1,
            reconnect_max_secs: 30,
            broadcast_cap: 256,
        }
    }
}

impl Config {
    pub fn load_or_default() -> Self {
        Self::load_or_default_from("config.json")
    }

    pub fn load_or_default_from<P: AsRef<std::path::Path>>(path: P) -> Self {
        let mut cfg = Self::default();
        if let Ok(s) = std::fs::read_to_string(&path) {
            if let Ok(root) = serde_json::from_str::<serde_json::Value>(&s) {
                match root.get("module_specific") {
                    Some(ms) if ms.is_object() => {
                        cfg = serde_json::from_value(ms.clone()).unwrap_or(cfg);
                    }
                    _ => {
                        cfg = serde_json::from_str::<Config>(&s).unwrap_or(cfg);
                    }
                }
            }
        }
        // Backfill missing keys into config.json (module_specific object),
        // preserving any existing top-level fields such as ip/port/pin.
        let mut root: serde_json::Value = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        root["module_specific"] = serde_json::to_value(&cfg).unwrap_or(serde_json::Value::Null);
        if let Ok(pretty) = serde_json::to_string_pretty(&root) {
            let _ = std::fs::write(&path, pretty);
        }
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_yields_defaults_and_backfills() {
        let dir = std::env::temp_dir().join(format!("audit_config_test_{}", uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        let cfg = Config::load_or_default_from(&path);
        assert_eq!(cfg.refresh_interval_secs, 3);
        assert_eq!(cfg.query_timeout_secs, 3);
        assert_eq!(cfg.live_log_cap, 500);
        assert_eq!(cfg.log_limit, 40);
        assert_eq!(cfg.messages_limit, 60);
        assert_eq!(cfg.errors_limit, 40);
        assert_eq!(cfg.audit_limit, 40);
        assert_eq!(cfg.reconnect_base_secs, 1);
        assert_eq!(cfg.reconnect_max_secs, 30);
        assert_eq!(cfg.broadcast_cap, 256);
        // Backfill wrote module_specific into the file.
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["module_specific"]["refresh_interval_secs"], 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_module_specific_fills_missing_with_defaults() {
        let dir = std::env::temp_dir().join(format!("audit_config_test2_{}", uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        std::fs::write(
            &path,
            r#"{"ip":"127.0.0.1","module_specific":{"refresh_interval_secs":7}}"#,
        )
        .unwrap();
        let cfg = Config::load_or_default_from(&path);
        assert_eq!(cfg.refresh_interval_secs, 7);
        assert_eq!(cfg.query_timeout_secs, 3);
        // Top-level fields are preserved on backfill.
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["ip"], "127.0.0.1");
        assert_eq!(written["module_specific"]["refresh_interval_secs"], 7);
        assert_eq!(written["module_specific"]["query_timeout_secs"], 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn uuid() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}