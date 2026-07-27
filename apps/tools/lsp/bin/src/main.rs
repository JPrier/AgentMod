//! LSP host composition root.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use agentmod_lsp_host_data::LspData;
use agentmod_lsp_host_dependency::{
    AuthorizationConfig, LspDependencyConfig, NativeLspDependency, ServerDefinition,
};
use agentmod_lsp_host_logic::LspLogic;
use agentmod_lsp_host_service::LspService;
use serde_json::Value;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let servers = std::env::var("AGENTMOD_LSP_SERVERS_JSON")
        .ok()
        .map(|value| parse_servers(&value))
        .transpose()?
        .unwrap_or_default();
    let mut config = LspDependencyConfig::new(
        std::env::current_dir()?,
        servers,
        8 * 1024 * 1024,
        Duration::from_secs(20),
    )?;
    if let (Ok(key), Ok(owner), Ok(session)) = (
        std::env::var("AGENTMOD_LSP_AUTH_KEY_HEX"),
        std::env::var("AGENTMOD_LSP_AUTH_OWNER"),
        std::env::var("AGENTMOD_LSP_AUTH_SESSION"),
    ) {
        config = config.with_authorization(AuthorizationConfig {
            owner,
            session,
            key: parse_key(&key)?,
            maximum_lifetime: Duration::from_secs(300),
        });
    }
    let dependency = NativeLspDependency::new(config);
    let data = LspData::new(dependency);
    let logic = LspLogic::new(data);
    let service = LspService::new(logic);
    service.run_jsonl(std::io::stdin().lock(), std::io::stdout().lock())?;
    Ok(())
}

fn parse_servers(input: &str) -> Result<Vec<ServerDefinition>, String> {
    let values = serde_json::from_str::<Vec<Value>>(input).map_err(|error| error.to_string())?;
    values
        .into_iter()
        .map(|value| {
            let required = |key: &str| {
                value
                    .get(key)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| format!("server field {key} is required"))
            };
            let arguments = string_array(value.get("arguments"))?;
            let extensions = string_array(value.get("extensions"))?
                .into_iter()
                .map(|extension| extension.to_ascii_lowercase())
                .collect::<BTreeSet<_>>();
            let environment = value
                .get("environment")
                .and_then(Value::as_object)
                .map(|items| {
                    items
                        .iter()
                        .map(|(key, value)| {
                            value
                                .as_str()
                                .map(|value| (key.clone(), value.to_owned()))
                                .ok_or_else(|| "environment values must be strings".to_owned())
                        })
                        .collect::<Result<BTreeMap<_, _>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            Ok(ServerDefinition {
                id: required("id")?,
                command: PathBuf::from(required("command")?),
                arguments,
                extensions,
                language_id: required("language_id")?,
                environment,
            })
        })
        .collect()
}

fn string_array(value: Option<&Value>) -> Result<Vec<String>, String> {
    value.and_then(Value::as_array).map_or_else(
        || Ok(Vec::new()),
        |values| {
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| "array values must be strings".to_owned())
                })
                .collect()
        },
    )
}

fn parse_key(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 {
        return Err("authorization key must be exactly 64 hexadecimal characters".into());
    }
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "authorization key contains non-hexadecimal characters")?;
    }
    Ok(key)
}
