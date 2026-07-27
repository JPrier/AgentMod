use std::{
    env,
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
};

fn main() {
    let ready = env::args().nth(1).expect("ready file");
    let log = env::args().nth(2).expect("request log");
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
    let endpoint = format!("http://{}/", listener.local_addr().expect("address"));
    fs::write(&ready, endpoint).expect("ready");
    for stream in listener.incoming() {
        let mut stream = stream.expect("accept");
        let (method, path) = read_request(&mut stream);
        append_log(&log, &format!("{method} {path}\n"));
        let value = route(&method, &path);
        let body = format!(r#"{{"value":{value}}}"#);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .expect("response");
        if method == "DELETE" && path == "/session/fixture-session" {
            break;
        }
    }
}

fn read_request(stream: &mut TcpStream) -> (String, String) {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let count = stream.read(&mut buffer).expect("read");
        request.extend_from_slice(&buffer[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("header")
        + 4;
    let headers = String::from_utf8_lossy(&request[..end]).into_owned();
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while request.len() < end + length {
        let count = stream.read(&mut buffer).expect("body");
        request.extend_from_slice(&buffer[..count]);
    }
    let mut parts = headers
        .lines()
        .next()
        .expect("request line")
        .split_whitespace();
    (
        parts.next().expect("method").to_owned(),
        parts.next().expect("path").to_owned(),
    )
}

fn route(method: &str, path: &str) -> &'static str {
    match (method, path) {
        ("POST", "/session") => {
            r#"{"sessionId":"fixture-session","capabilities":{"browserName":"fixture"}}"#
        }
        ("GET", "/session/fixture-session/url") => r#""http://127.0.0.1/page""#,
        ("GET", "/session/fixture-session/title") => r#""Fixture rendered page""#,
        ("GET", "/session/fixture-session/source") => {
            r#""<html><body><button id=\"button\">Go</button><form><input id=\"input\"></form><script>document.body.dataset.rendered='true'</script></body></html>""#
        }
        ("GET", "/session/fixture-session/screenshot") => r#""iVBORw0KGgo=""#,
        ("POST", "/session/fixture-session/element") => {
            r#"{"element-6066-11e4-a52e-4f735466cecf":"element-1"}"#
        }
        ("POST", "/session/fixture-session/execute/async") => {
            r#"{"url":"http://127.0.0.1/file","mime":"text/plain","base64":"ZG93bmxvYWQtdGhyb3VnaC1icm93c2Vy"}"#
        }
        _ => "null",
    }
}

fn append_log(path: impl AsRef<Path>, line: &str) {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| file.write_all(line.as_bytes()))
        .expect("log");
}
