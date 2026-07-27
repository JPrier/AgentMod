use reqwest::Client;

pub type LogicRequest = runtime_service::ServiceRequest;
pub use runtime_data::DataRecord;

pub fn wire_value(_: fixture_runtime_protocol::WireRequest) {
    let _ = std::fs::read("forbidden");
    let _ = std::mem::size_of::<Client>();
}
