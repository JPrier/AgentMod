//! Host-process fixture that substitutes an unknown response correlation.

use std::io::{BufRead, Write};

use agentmod_plugin_protocol::{PluginResponse, PluginResponseFrame, decode_bounded_request_frame};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = std::io::stdin();
    let mut lines = input.lock().lines();
    for _ in 0..2 {
        let line = lines.next().ok_or("missing request")??;
        decode_bounded_request_frame(line.as_bytes())?;
    }

    let frame = PluginResponseFrame {
        correlation_id: String::from("substituted-correlation"),
        response: PluginResponse::Failed {
            code: String::from("substituted_correlation"),
            message: String::from("fixture response"),
            retryable: false,
        },
    };
    frame.validate_contract()?;
    let output = std::io::stdout();
    let mut output = output.lock();
    serde_json::to_writer(&mut output, &frame)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}
