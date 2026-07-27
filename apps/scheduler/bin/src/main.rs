//! Scheduler composition root and bounded JSONL protocol transport.

use std::{
    error::Error,
    io::{self, BufRead, Write},
    path::PathBuf,
};

use agentmod_scheduler_data::SchedulerData;
use agentmod_scheduler_dependency::FileSchedulerDependency;
use agentmod_scheduler_logic::SchedulerLogic;
use agentmod_scheduler_protocol::{SchedulerCommand, SchedulerResponse};
use agentmod_scheduler_service::SchedulerService;

const MAX_FRAME_BYTES: usize = 1024 * 1024;

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::var_os("AGENTMOD_SCHEDULER_ROOT")
        .map_or_else(|| PathBuf::from("scheduler"), PathBuf::from);
    let dependency = FileSchedulerDependency::new(root)?;
    let data = SchedulerData::new(dependency);
    let logic = SchedulerLogic::new(data);
    let authentication_token = std::env::var("AGENTMOD_SCHEDULER_AUTH_TOKEN")?;
    let service = SchedulerService::new(logic, authentication_token)?;
    serve(&service, io::stdin().lock(), io::stdout().lock())
}

fn serve<L: agentmod_scheduler_logic::SchedulerLogicPort>(
    service: &SchedulerService<L>,
    input: impl BufRead,
    mut output: impl Write,
) -> Result<(), Box<dyn Error>> {
    for line in input.lines() {
        let line = line?;
        let response = if line.len() > MAX_FRAME_BYTES {
            SchedulerResponse::Error {
                code: "frame_too_large".to_owned(),
                message: "scheduler command exceeded the transport bound".to_owned(),
            }
        } else {
            match serde_json::from_str::<SchedulerCommand>(&line) {
                Ok(command) => service.handle(command),
                Err(_) => SchedulerResponse::Error {
                    code: "invalid_json".to_owned(),
                    message: "scheduler command was malformed".to_owned(),
                },
            }
        };
        serde_json::to_writer(&mut output, &response)?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}
