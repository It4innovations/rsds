
use serde::{Deserialize, Serialize};


/*
{b'status': b'OK',
 b'time': 1568021833.7372987,
 b'heartbeat-interval': 0.5,
 b'worker-plugins': []}
*/

#[derive(Serialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct HeartbeatResponse {
    pub status: &'static str,
    pub time: f64,
    pub heartbeat_interval: f64,
    pub worker_plugins: Vec<()>, // type of plugins??
}

#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum WorkerMessage {
    KeepAlive,
}
