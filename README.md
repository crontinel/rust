# crontinel - Crontinel API Client for Rust

Report cron and background job runs to [Crontinel Cloud](https://app.crontinel.com).

## Quickstart

```toml
[dependencies]
crontinel = "0.1"
```

```rust
use crontinel::Crontinel;

let client = Crontinel::new("your_api_key");
client.set_base_url("https://app.crontinel.com/api/v1");

// Report a cron run
client.schedule_run("php artisan schedule:run", Some(1500), 0).unwrap();
```

Get your API key at [app.crontinel.com](https://app.crontinel.com).

## Usage

```rust

// Report a scheduled command
client.schedule_run("php artisan schedule:run", Some(1500), 0).unwrap();

// Report queue processing
client.queue_processed("emails", 50, 2, Some(3200)).unwrap();

// Send a custom event
client.event("deployment", "Application deployed", "info", None).unwrap();

// Monitor a closure
let (ms, code) = client.monitor_schedule("my-task", || Ok(()));
assert_eq!(code, 0);
```

## Features

- `schedule_run` — report scheduled command outcome
- `queue_processed` — report queue worker activity
- `horizon_snapshot` — report Laravel Horizon supervisor status
- `event` — send custom events and alerts
- `monitor_schedule` — run a closure and auto-report outcome

## Builder

```rust
let client = Crontinel::builder("key")
    .api_url("https://custom.example.com")
    .app_name("my-worker")
    .timeout(Duration::from_secs(30))
    .build();
```

## Laravel Integration

For Laravel applications, use the official [`crontinel/laravel`](https://github.com/crontinel/crontinel) package which integrates with the scheduler and queue worker out of the box.
