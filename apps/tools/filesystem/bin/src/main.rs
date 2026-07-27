//! Filesystem-host composition root.

use std::{
    io::{BufRead, BufReader, Write},
    sync::Arc,
};

use agentmod_filesystem_host_data::FilesystemData;
use agentmod_filesystem_host_dependency::{
    DEFAULT_MAX_FILE_BYTES, FilesystemAuthorizationConfig, FilesystemDependencyConfig,
    NativeFilesystem,
};
use agentmod_filesystem_host_logic::FilesystemLogic;
use agentmod_filesystem_host_service::FilesystemService;
use agentmod_protocol_support::authorization::AuthorizationKey;
use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = std::env::current_dir()?;
    let key = std::env::var("AGENTMOD_FILESYSTEM_AUTH_KEY_HEX")
        .map_err(|_| "AGENTMOD_FILESYSTEM_AUTH_KEY_HEX is required")?;
    let owner = std::env::var("AGENTMOD_FILESYSTEM_AUTH_OWNER")
        .map_err(|_| "AGENTMOD_FILESYSTEM_AUTH_OWNER is required")?;
    let session = std::env::var("AGENTMOD_FILESYSTEM_AUTH_SESSION")
        .map_err(|_| "AGENTMOD_FILESYSTEM_AUTH_SESSION is required")?;
    let config =
        FilesystemDependencyConfig::new(vec![workspace], Vec::new(), DEFAULT_MAX_FILE_BYTES)?
            .with_authorization(FilesystemAuthorizationConfig {
                owner,
                session,
                key: Arc::new(AuthorizationKey::from_hex(&key)?),
            });
    let dependency = NativeFilesystem::new(config);
    let data = FilesystemData::new(dependency);
    let logic = FilesystemLogic::new(data);
    let service = FilesystemService::new(logic);
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    let mut frame = Vec::new();
    loop {
        frame.clear();
        let bytes = reader.read_until(b'\n', &mut frame)?;
        if bytes == 0 {
            break;
        }
        if frame.len() > MAX_FRAME_BYTES {
            write_event(
                &mut writer,
                &ToolHostEvent::Failed {
                    call_id: String::new(),
                    code: "frame_too_large".into(),
                    message: "tool command exceeds the fixed frame limit".into(),
                    retryable: false,
                },
            )?;
            continue;
        }
        while matches!(frame.last(), Some(b'\n' | b'\r')) {
            frame.pop();
        }
        let Ok(command) = serde_json::from_slice::<ToolHostCommand>(&frame) else {
            write_event(
                &mut writer,
                &ToolHostEvent::Failed {
                    call_id: String::new(),
                    code: "invalid_frame".into(),
                    message: "tool command is not valid protocol JSON".into(),
                    retryable: false,
                },
            )?;
            continue;
        };
        for event in service.handle_wire(command) {
            write_event(&mut writer, &event)?;
        }
    }
    Ok(())
}

fn write_event(
    writer: &mut impl Write,
    event: &ToolHostEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    serde_json::to_writer(&mut *writer, event)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}
