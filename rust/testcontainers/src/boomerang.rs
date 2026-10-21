
use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

const DEFAULT_IMAGE: &str = "boomerang:local";

/// Container port exposed by the Boomerang gRPC host (h2c).
pub const BOOMERANG_PORT: ContainerPort = ContainerPort::Tcp(50051);

#[derive(Debug, Clone)]
pub struct Boomerang {
    name: String,
    tag: String,
    env_vars: Vec<(&'static str, String)>,
    mounts: Vec<Mount>,
}

/// `BOOMERANG_IMAGE` overrides the image the harness starts. `boomerang:local` is the default.
/// `name:tag` splits on the last colon. A registry port (`localhost:5000/img:tag`) keeps the host.
fn configured_image() -> (String, String, String) {
    let raw = std::env::var("BOOMERANG_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_string());
    let raw = raw.trim();
    let (name, tag) = if raw.is_empty() {
        ("boomerang".to_string(), "local".to_string())
    } else if let Some((name, tag)) = raw.rsplit_once(':') {
        if !name.is_empty() && !tag.is_empty() && !tag.contains('/') {
            (name.to_string(), tag.to_string())
        } else {
            (raw.to_string(), "latest".to_string())
        }
    } else {
        (raw.to_string(), "latest".to_string())
    };
    let image_ref = format!("{name}:{tag}");
    (name, tag, image_ref)
}

/// Optional host override forwarded into the container as `BOOMERANG_SEGMENT_MAX_BYTES`.
/// Unset keeps the image default (256 MiB). Do not set `ESB_SEGMENT_SIZE_BYTES` for this —
/// that knob also retunes Axon/tephra.
fn segment_max_bytes_override() -> Option<String> {
    std::env::var("BOOMERANG_SEGMENT_MAX_BYTES")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Optional host override forwarded into the container as
/// `BOOMERANG_DELIVERY_LINGER_MS` (delivery linger window in ms; `0` disables).
/// Unset keeps the image default (on at 1 ms on the bench host since `a57f091`).
fn delivery_linger_override() -> Option<String> {
    std::env::var("BOOMERANG_DELIVERY_LINGER_MS")
        .ok()
        .filter(|s| !s.is_empty())
}

impl Boomerang {
    pub fn new(data_dir: Option<String>, durability: &str) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/data"),
            None => Mount::volume_mount("", "/data"),
        };
        let (name, tag, _image_ref) = configured_image();
        let mut env_vars = vec![
            ("BOOMERANG_DATA_DIR", "/data".to_string()),
            ("BOOMERANG_DURABILITY", durability.to_string()),
            ("GRPC_PORT", "50051".to_string()),
        ];
        if let Some(bytes) = segment_max_bytes_override() {
            println!("BOOMERANG_SEGMENT_MAX_BYTES={bytes}");
            env_vars.push(("BOOMERANG_SEGMENT_MAX_BYTES", bytes));
        }
        if let Some(ms) = delivery_linger_override() {
            println!("BOOMERANG_DELIVERY_LINGER_MS={ms}");
            env_vars.push(("BOOMERANG_DELIVERY_LINGER_MS", ms));
        }
        Self {
            name,
            tag,
            env_vars,
            mounts: vec![mount],
        }
    }

    /// Runtime posture recorded in each run's `run_manifest.json`.
    pub fn describe(durability: &str) -> serde_json::Value {
        let (.., image_ref) = configured_image();
        serde_json::json!({
            "image": image_ref,
            "durability": durability,
            "git_sha": image_git_sha(&image_ref),
            "segment_max_bytes": segment_max_bytes_override(),
            "delivery_linger_ms": delivery_linger_override(),
        })
    }
}

/// Reads the `git.sha` label from the image the harness will start.
fn image_git_sha(image_ref: &str) -> Option<String> {
    let output = std::process::Command::new("docker")
        .args([
            "inspect",
            "--format",
            r#"{{index .Config.Labels "git.sha"}}"#,
            image_ref,
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if sha.is_empty() || sha == "<no value>" {
        None
    } else {
        Some(sha)
    }
}

impl Image for Boomerang {
    fn name(&self) -> &str {
        &self.name
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("Boomerang started")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<
        Item = (
            impl Into<std::borrow::Cow<'_, str>>,
            impl Into<std::borrow::Cow<'_, str>>,
        ),
    > {
        self.env_vars.iter().map(|(k, v)| (*k, v.clone()))
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[BOOMERANG_PORT]
    }
}


