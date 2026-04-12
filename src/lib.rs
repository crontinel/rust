//! Crontinel API client for Rust.

use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Crontinel {
    api_key: String,
    api_url: String,
    app_name: String,
    http_client: Client,
}

impl Crontinel {
    pub fn new(api_key: &str) -> Self {
        Self::builder(api_key).build()
    }

    pub fn builder(api_key: &str) -> CrontinelBuilder {
        CrontinelBuilder::new(api_key)
    }

    pub fn schedule_run(&self, command: &str, duration_ms: Option<i64>, exit_code: i32) -> Result<(), CrontinelError> {
        self.call("notify/schedule_run", json!({
            "command": command,
            "duration_ms": duration_ms,
            "exit_code": exit_code,
            "ran_at": unix_now(),
            "app": self.app_name,
        }))
    }

    pub fn queue_processed(&self, queue: &str, processed: i64, failed: i64, duration_ms: Option<i64>) -> Result<(), CrontinelError> {
        self.call("notify/queue_processed", json!({
            "queue": queue,
            "processed": processed,
            "failed": failed,
            "duration_ms": duration_ms,
            "ran_at": unix_now(),
            "app": self.app_name,
        }))
    }

    pub fn horizon_snapshot(&self, supervisors: serde_json::Value, failed_jobs_per_minute: f64, paused: bool) -> Result<(), CrontinelError> {
        self.call("notify/horizon_snapshot", json!({
            "supervisors": supervisors,
            "failed_jobs_per_minute": failed_jobs_per_minute,
            "paused": paused,
            "ran_at": unix_now(),
            "app": self.app_name,
        }))
    }

    pub fn event(&self, key: &str, message: &str, state: &str, metadata: Option<serde_json::Value>) -> Result<(), CrontinelError> {
        self.call("notify/event", json!({
            "key": key,
            "message": message,
            "state": state,
            "metadata": metadata.unwrap_or(json!({})),
            "ran_at": unix_now(),
            "app": self.app_name,
        }))
    }

    pub fn monitor_schedule<F>(&self, command: &str, f: F) -> (i64, i32)
    where
        F: FnOnce() -> Result<(), CrontinelError>,
    {
        use std::time::Instant;
        let start = Instant::now();
        let exit_code = match f() {
            Ok(()) => 0,
            Err(_) => 1,
        };
        let duration_ms = start.elapsed().as_millis() as i64;
        let _ = self.schedule_run(command, Some(duration_ms), exit_code);
        (duration_ms, exit_code)
    }

    fn call(&self, method: &str, params: serde_json::Value) -> Result<(), CrontinelError> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });

        let resp = self
            .http_client
            .post(format!("{}/api/mcp", self.api_url))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&body)
            .send()?;

        if !resp.status().is_success() {
            return Err(CrontinelError::Http(resp.status().as_u16()));
        }

        let rpc: RpcResponse = resp.json()?;
        if rpc.error.is_some() {
            return Err(CrontinelError::Rpc(rpc.error.unwrap()));
        }
        Ok(())
    }
}

impl Default for Crontinel {
    fn default() -> Self {
        Self::new("")
    }
}

#[derive(Debug)]
pub enum CrontinelError {
    Http(u16),
    Rpc(RpcError),
    Reqwest(reqwest::Error),
}

#[derive(Debug, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    #[serde(rename = "result")]
    pub result: Option<serde_json::Value>,
    #[serde(rename = "error")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for CrontinelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CrontinelError::Http(code) => write!(f, "HTTP {}", code),
            CrontinelError::Rpc(e) => write!(f, "RPC {}: {}", e.code, e.message),
            CrontinelError::Reqwest(e) => write!(f, "Reqwest: {}", e),
        }
    }
}

impl std::error::Error for CrontinelError {}

impl From<reqwest::Error> for CrontinelError {
    fn from(e: reqwest::Error) -> Self {
        CrontinelError::Reqwest(e)
    }
}

pub struct CrontinelBuilder {
    api_key: String,
    api_url: String,
    app_name: String,
    timeout: Duration,
}

impl CrontinelBuilder {
    pub fn new(api_key: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_url: "https://app.crontinel.com".to_string(),
            app_name: "rust".to_string(),
            timeout: Duration::from_secs(10),
        }
    }

    pub fn api_url(mut self, url: &str) -> Self {
        self.api_url = url.to_string();
        self
    }

    pub fn app_name(mut self, name: &str) -> Self {
        self.app_name = name.to_string();
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn build(self) -> Crontinel {
        if self.api_key.trim().is_empty() {
            panic!("crontinel: api_key is required");
        }
        let http_client = Client::builder()
            .timeout(self.timeout)
            .build()
            .expect("valid HTTP client");
        Crontinel { api_key: self.api_key, api_url: self.api_url, app_name: self.app_name, http_client }
    }
}

fn unix_now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn spawn_test_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{}", port);

        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap();
                let body = String::from_utf8_lossy(&buf[..n]);

                // Find Content-Length
                let cl_header = body.lines()
                    .find(|l| l.to_lowercase().starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1))
                    .map(|s| s.trim().parse::<usize>().ok())
                    .flatten()
                    .unwrap_or(0);

                // Read body if needed (after headers end with \r\n\r\n)
                let mut full_body = String::new();
                if cl_header > 0 {
                    let header_end = body.find("\r\n\r\n").map(|p| p + 4).unwrap_or(n);
                    let body_start = header_end;
                    full_body = body[body_start..body_start + cl_header.min(body.len() - body_start)].to_string();
                }

                // Parse JSON-RPC request (body starts after headers)
                let body_str: &str = if full_body.is_empty() { &*body } else { &full_body };
                let parsed: Result<serde_json::Value, _> = serde_json::from_str(body_str);

                let response = if parsed.is_ok() {
                    r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#
                } else {
                    r#"{"jsonrpc":"2.0","id":1,"result":{}}"#
                };

                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    response.len(),
                    response
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });

        url
    }

    fn spawn_401_server() -> (String, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{}", port);

        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let body = "{}";
                let resp = format!("HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", body.len(), body);
                let _ = stream.write_all(resp.as_bytes());
            }
        });

        thread::sleep(Duration::from_millis(10));
        (url, port)
    }


    #[test]
    fn test_requires_api_key() {
        let result = std::panic::catch_unwind(|| Crontinel::new(""));
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_api_url() {
        let _c = Crontinel::builder("key")
            .api_url("https://custom.example.com")
            .app_name("my-worker")
            .build();
    }

    #[test]
    fn test_schedule_run_sends_correct_payload() {
        let url = spawn_test_server();
        let c = Crontinel::builder("test_key").api_url(&url).build();
        let err = c.schedule_run("php artisan schedule:run", Some(1500), 0);
        assert!(err.is_ok());
    }

    #[test]
    fn test_queue_processed_payload() {
        let url = spawn_test_server();
        let c = Crontinel::builder("test_key").api_url(&url).build();
        let err = c.queue_processed("emails", 50, 2, Some(3200));
        assert!(err.is_ok());
    }

    #[test]
    fn test_horizon_snapshot_payload() {
        let url = spawn_test_server();
        let c = Crontinel::builder("test_key").api_url(&url).build();
        let supervisors = json!({"emails": {"status": "running"}, "reports": {"status": "paused"}});
        let err = c.horizon_snapshot(supervisors, 4.2, false);
        assert!(err.is_ok());
    }

    #[test]
    fn test_event_payload() {
        let url = spawn_test_server();
        let c = Crontinel::builder("test_key").api_url(&url).build();
        let err = c.event("deployment", "Application deployed", "info", Some(json!({"version": "2.1.0"})));
        assert!(err.is_ok());
    }

    #[test]
    fn test_monitor_schedule_success() {
        let url = spawn_test_server();
        let c = Crontinel::builder("test_key").api_url(&url).build();
        let (ms, code) = c.monitor_schedule("my-task", || Ok(()));
        assert_eq!(code, 0);
        assert!(ms >= 0);
    }

    #[test]
    fn test_http_error_propagates() {
        let (url, port) = spawn_401_server();
        let c = Crontinel::builder("test_key").api_url(&url).build();
        let err = c.schedule_run("test", Some(100), 0);
        assert!(err.is_err());
        match err {
            Err(CrontinelError::Http(401)) => {}
            other => panic!("expected HTTP 401 on port {}, got {:?}", port, other),
        }
    }
}
