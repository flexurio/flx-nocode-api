//! Files compiled into the binary so a fresh install never needs the network:
//! the two core entities (`flx_users`, `flx_roles`), a minimal `rules.json`
//! and the `.env` template written by `flx-nocode-api init`.

pub const FLX_USERS_JSON: &str = include_str!("flx_users.json");
pub const FLX_ROLES_JSON: &str = include_str!("flx_roles.json");
pub const RULES_JSON: &str = include_str!("rules.json");
pub const ENV_TEMPLATE: &str = include_str!("env.example");
pub const ROUTES_JSON: &str = "{\n  \"routes\": [\"flx_users\", \"flx_roles\"],\n  \"route_publics\": []\n}\n";

/// Write `flx_users.json` / `flx_roles.json` into `<conf>/entity` when missing.
/// Returns the list of files created.
pub fn ensure_core_config(conf: &str) -> std::io::Result<Vec<String>> {
    let entity_dir = std::path::Path::new(conf).join("entity");
    std::fs::create_dir_all(&entity_dir)?;
    let mut created = Vec::new();
    for (name, body) in [("flx_users.json", FLX_USERS_JSON), ("flx_roles.json", FLX_ROLES_JSON)] {
        let p = entity_dir.join(name);
        if !p.exists() {
            std::fs::write(&p, body)?;
            created.push(p.display().to_string());
        }
    }
    let routes = std::path::Path::new(conf).join("routes.json");
    if !routes.exists() {
        std::fs::write(&routes, ROUTES_JSON)?;
        created.push(routes.display().to_string());
    }
    let rules = std::path::Path::new(conf).join("rules.json");
    if !rules.exists() {
        std::fs::write(&rules, RULES_JSON)?;
        created.push(rules.display().to_string());
    }
    Ok(created)
}

/// Random URL-safe secret of `len` characters.
pub fn random_secret(len: usize) -> String {
    use rand::RngExt;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..len).map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_core_entities_are_valid_schemas() {
        for body in [FLX_USERS_JSON, FLX_ROLES_JSON] {
            let s: crate::model::TableSchema = serde_json::from_str(body).unwrap();
            assert!(!s.table.is_empty());
        }
        let v: serde_json::Value = serde_json::from_str(RULES_JSON).unwrap();
        assert!(v["rule"].is_array());
    }

    #[test]
    fn ensure_core_config_is_idempotent() {
        let dir = std::env::temp_dir().join(format!("flx-embedded-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let created = ensure_core_config(dir.to_str().unwrap()).unwrap();
        assert_eq!(created.len(), 4);
        let again = ensure_core_config(dir.to_str().unwrap()).unwrap();
        assert!(again.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
