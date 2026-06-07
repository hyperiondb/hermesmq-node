use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hermesmq_proto as proto;
use hermesmq_proto::{request, response, Request, Response};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

type OnMessage = ThreadsafeFunction<DeliveredMessage, Promise<()>, DeliveredMessage, Status, false>;

#[napi(object)]
pub struct NodeAddr {
    pub id: u32,
    pub client_addr: String,
    pub peer_addr: String,
}

#[napi(object)]
pub struct RateLimit {
    pub rate_per_sec: f64,
    pub burst: u32,
}

#[napi(object)]
pub struct Retention {
    pub max_messages: u32,
    pub max_age_ms: u32,
}

#[napi(object)]
pub struct CreateTopicOptions {
    pub topic: String,
    pub rate_limit: Option<RateLimit>,
    pub retention: Option<Retention>,
}

#[napi(object)]
pub struct ProduceOptions {
    pub topic: String,
    pub body: Buffer,
    pub priority: Option<u32>,
}

#[napi(object)]
pub struct PollOptions {
    pub topic: String,
    pub group: String,
    pub max: Option<u32>,
    pub visibility_ms: Option<u32>,
    pub wait_ms: Option<u32>,
}

#[napi(object)]
pub struct SubscribeOptions {
    pub topic: String,
    pub group: String,
    pub prefetch: Option<u32>,
    pub visibility_ms: Option<u32>,
    pub ack_mode: Option<String>,
}

#[napi(object)]
pub struct LeaseRef {
    pub topic: String,
    pub group: String,
    pub lease_id: String,
}

#[napi(object)]
pub struct DeliveredMessage {
    pub lease_id: String,
    pub offset: String,
    pub priority: u32,
    pub content_type: u32,
    pub payload: Buffer,
    pub ts_ms: String,
}

#[napi(object)]
pub struct ClusterStats {
    pub last_applied: String,
    pub current_leader: u32,
}

struct NodeRec {
    id: u64,
    client_addr: String,
    peer_addr: String,
}

struct Inner {
    nodes: Vec<NodeRec>,
    leader: Mutex<usize>,
}

#[napi]
pub struct Client {
    inner: Arc<Inner>,
}

async fn try_call(addr: &str, req: &Request) -> std::io::Result<Response> {
    let mut stream = TcpStream::connect(addr).await?;
    let bytes = req.encode_to_vec();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;

    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await?;
    Response::decode(buf.as_slice())
        .map_err(|e| std::io::Error::other(e.to_string()))
}

async fn call(inner: &Inner, req: &Request) -> Result<Response> {
    let n = inner.nodes.len();
    if n == 0 {
        return Err(Error::from_reason("hermesmq: no nodes configured"));
    }
    let mut idx = *inner.leader.lock().await;
    let attempts = (n * 3).max(3);
    let mut last = String::new();
    for _ in 0..attempts {
        let node = &inner.nodes[idx % n];
        match try_call(&node.client_addr, req).await {
            Ok(resp) => {
                if let Some(response::Kind::Error(e)) = &resp.kind {
                    if e.code == "not_leader" {
                        last = format!("not_leader from {}", node.client_addr);
                        idx += 1;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
                *inner.leader.lock().await = idx % n;
                return Ok(resp);
            }
            Err(e) => {
                last = e.to_string();
                idx += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    Err(Error::from_reason(format!(
        "hermesmq: no reachable leader after {attempts} attempts: {last}"
    )))
}

fn err_response(e: proto::Error) -> Error {
    Error::from_reason(format!("{}: {}", e.code, e.message))
}

#[napi]
pub struct Subscription {
    stop: Arc<AtomicBool>,
    abort: tokio::task::AbortHandle,
}

#[napi]
impl Subscription {
    #[napi]
    pub fn unsubscribe(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.abort.abort();
    }
}

fn dispatch(
    d: proto::Delivered,
    topic: &str,
    group: &str,
    auto: bool,
    on_message: &Arc<OnMessage>,
    tx: &UnboundedSender<Request>,
) {
    let lease_id = d.lease_id;
    let msg = DeliveredMessage {
        lease_id: d.lease_id.to_string(),
        offset: d.offset.to_string(),
        priority: d.priority,
        content_type: d.content_type,
        payload: Buffer::from(d.payload),
        ts_ms: d.ts_ms.to_string(),
    };
    if auto {
        on_message.call(msg, ThreadsafeFunctionCallMode::NonBlocking);
        return;
    }
    let on_message = on_message.clone();
    let tx = tx.clone();
    let topic = topic.to_string();
    let group = group.to_string();
    tokio::spawn(async move {
        let ok = match on_message.call_async_catch(msg).await {
            Ok(promise) => promise.await.is_ok(),
            Err(_) => false,
        };
        let req = if ok {
            Request {
                kind: Some(request::Kind::Ack(proto::Ack {
                    topic,
                    group,
                    lease_id,
                })),
            }
        } else {
            Request {
                kind: Some(request::Kind::Nack(proto::Nack {
                    topic,
                    group,
                    lease_id,
                })),
            }
        };
        let _ = tx.send(req);
    });
}

async fn read_push_loop(
    read_half: &mut OwnedReadHalf,
    topic: &str,
    group: &str,
    auto: bool,
    on_message: &Arc<OnMessage>,
    tx: &UnboundedSender<Request>,
    stop: &Arc<AtomicBool>,
) {
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let mut len = [0u8; 4];
        if read_half.read_exact(&mut len).await.is_err() {
            return;
        }
        let n = u32::from_be_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        if read_half.read_exact(&mut buf).await.is_err() {
            return;
        }
        let Ok(resp) = Response::decode(buf.as_slice()) else {
            continue;
        };
        match resp.kind {
            Some(response::Kind::Polled(p)) => {
                for d in p.items {
                    dispatch(d, topic, group, auto, on_message, tx);
                }
            }
            Some(response::Kind::Error(_)) => return,
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_subscription(
    inner: Arc<Inner>,
    topic: String,
    group: String,
    prefetch: u32,
    visibility_ms: u64,
    auto: bool,
    on_message: Arc<OnMessage>,
    stop: Arc<AtomicBool>,
) {
    let n = inner.nodes.len();
    if n == 0 {
        return;
    }
    let ack_mode = if auto { "auto" } else { "manual" };
    let mut idx = *inner.leader.lock().await;
    while !stop.load(Ordering::SeqCst) {
        let addr = inner.nodes[idx % n].client_addr.clone();
        let stream = match TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(_) => {
                idx += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let (mut read_half, mut write_half) = stream.into_split();
        let sub = Request {
            kind: Some(request::Kind::Subscribe(proto::Subscribe {
                topic: topic.clone(),
                group: group.clone(),
                prefetch,
                visibility_timeout_ms: visibility_ms,
                ack_mode: ack_mode.to_string(),
            })),
        };
        let bytes = sub.encode_to_vec();
        if write_half
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .is_err()
            || write_half.write_all(&bytes).await.is_err()
            || write_half.flush().await.is_err()
        {
            idx += 1;
            continue;
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
        let writer = tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let b = req.encode_to_vec();
                if write_half
                    .write_all(&(b.len() as u32).to_be_bytes())
                    .await
                    .is_err()
                    || write_half.write_all(&b).await.is_err()
                    || write_half.flush().await.is_err()
                {
                    break;
                }
            }
        });

        *inner.leader.lock().await = idx % n;
        read_push_loop(&mut read_half, &topic, &group, auto, &on_message, &tx, &stop).await;
        drop(tx);
        writer.abort();

        if stop.load(Ordering::SeqCst) {
            return;
        }
        idx += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn expect_ok(resp: Response) -> Result<()> {
    match resp.kind {
        Some(response::Kind::Ok(_)) => Ok(()),
        Some(response::Kind::Error(e)) => Err(err_response(e)),
        other => Err(Error::from_reason(format!("unexpected response: {other:?}"))),
    }
}

#[napi]
pub async fn connect(nodes: Vec<NodeAddr>) -> Result<Client> {
    if nodes.is_empty() {
        return Err(Error::from_reason("connect: `nodes` must not be empty"));
    }
    let recs = nodes
        .into_iter()
        .map(|n| NodeRec {
            id: n.id as u64,
            client_addr: n.client_addr,
            peer_addr: n.peer_addr,
        })
        .collect();
    let client = Client {
        inner: Arc::new(Inner {
            nodes: recs,
            leader: Mutex::new(0),
        }),
    };
    client.bootstrap().await?;
    Ok(client)
}

#[napi]
impl Client {
    #[napi]
    pub async fn bootstrap(&self) -> Result<()> {
        let inner = self.inner.clone();
        let nodes = inner
            .nodes
            .iter()
            .map(|r| proto::Node {
                id: r.id,
                peer_addr: r.peer_addr.clone(),
            })
            .collect();
        let req = Request {
            kind: Some(request::Kind::Bootstrap(proto::Bootstrap { nodes })),
        };
        expect_ok(call(&inner, &req).await?)
    }

    #[napi]
    pub async fn create_topic(&self, options: CreateTopicOptions) -> Result<()> {
        let inner = self.inner.clone();
        let req = Request {
            kind: Some(request::Kind::CreateTopic(proto::CreateTopic {
                topic: options.topic.clone(),
            })),
        };
        expect_ok(call(&inner, &req).await?)?;
        if let Some(rate_limit) = options.rate_limit {
            let req = Request {
                kind: Some(request::Kind::SetRateLimit(proto::SetRateLimit {
                    topic: options.topic.clone(),
                    rate_per_sec: rate_limit.rate_per_sec,
                    burst: rate_limit.burst,
                })),
            };
            expect_ok(call(&inner, &req).await?)?;
        }
        if let Some(retention) = options.retention {
            let req = Request {
                kind: Some(request::Kind::SetRetention(proto::SetRetention {
                    topic: options.topic,
                    max_messages: retention.max_messages as u64,
                    max_age_ms: retention.max_age_ms as u64,
                })),
            };
            expect_ok(call(&inner, &req).await?)?;
        }
        Ok(())
    }

    #[napi]
    pub async fn produce(&self, options: ProduceOptions) -> Result<String> {
        let inner = self.inner.clone();
        let req = Request {
            kind: Some(request::Kind::Produce(proto::Produce {
                topic: options.topic,
                priority: options.priority.unwrap_or(0),
                content_type: 0,
                payload: options.body.to_vec(),
                producer_id: String::new(),
                seq: 0,
            })),
        };
        match call(&inner, &req).await?.kind {
            Some(response::Kind::Produced(p)) => Ok(p.offset.to_string()),
            Some(response::Kind::Error(e)) => Err(err_response(e)),
            other => Err(Error::from_reason(format!("unexpected response: {other:?}"))),
        }
    }

    #[napi]
    pub async fn poll(&self, options: PollOptions) -> Result<Vec<DeliveredMessage>> {
        let inner = self.inner.clone();
        let req = Request {
            kind: Some(request::Kind::Poll(proto::Poll {
                topic: options.topic,
                group: options.group,
                max: options.max.unwrap_or(16),
                visibility_timeout_ms: options.visibility_ms.unwrap_or(30_000) as u64,
                ack_mode: "manual".to_string(),
                wait_ms: options.wait_ms.unwrap_or(0) as u64,
            })),
        };
        match call(&inner, &req).await?.kind {
            Some(response::Kind::Polled(p)) => Ok(p
                .items
                .into_iter()
                .map(|d| DeliveredMessage {
                    lease_id: d.lease_id.to_string(),
                    offset: d.offset.to_string(),
                    priority: d.priority,
                    content_type: d.content_type,
                    payload: Buffer::from(d.payload),
                    ts_ms: d.ts_ms.to_string(),
                })
                .collect()),
            Some(response::Kind::Error(e)) => Err(err_response(e)),
            other => Err(Error::from_reason(format!("unexpected response: {other:?}"))),
        }
    }

    #[napi(ts_args_type = "options: SubscribeOptions, onMessage: (msg: DeliveredMessage) => void | Promise<void>")]
    pub async fn subscribe(&self, options: SubscribeOptions, on_message: OnMessage) -> Result<Subscription> {
        let inner = self.inner.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let auto = options.ack_mode.as_deref() == Some("auto");
        let prefetch = options.prefetch.unwrap_or(16);
        let visibility_ms = options.visibility_ms.unwrap_or(30_000) as u64;
        let stop_task = stop.clone();
        let handle = tokio::spawn(run_subscription(
            inner,
            options.topic,
            options.group,
            prefetch,
            visibility_ms,
            auto,
            Arc::new(on_message),
            stop_task,
        ));
        Ok(Subscription {
            stop,
            abort: handle.abort_handle(),
        })
    }

    #[napi]
    pub async fn ack(&self, lease: LeaseRef) -> Result<()> {
        let inner = self.inner.clone();
        let lease_id = lease
            .lease_id
            .parse::<u64>()
            .map_err(|_| Error::from_reason("ack: invalid leaseId"))?;
        let req = Request {
            kind: Some(request::Kind::Ack(proto::Ack {
                topic: lease.topic,
                group: lease.group,
                lease_id,
            })),
        };
        expect_ok(call(&inner, &req).await?)
    }

    #[napi]
    pub async fn nack(&self, lease: LeaseRef) -> Result<()> {
        let inner = self.inner.clone();
        let lease_id = lease
            .lease_id
            .parse::<u64>()
            .map_err(|_| Error::from_reason("nack: invalid leaseId"))?;
        let req = Request {
            kind: Some(request::Kind::Nack(proto::Nack {
                topic: lease.topic,
                group: lease.group,
                lease_id,
            })),
        };
        expect_ok(call(&inner, &req).await?)
    }

    #[napi]
    pub async fn stats(&self) -> Result<ClusterStats> {
        let inner = self.inner.clone();
        let req = Request {
            kind: Some(request::Kind::Stats(proto::Stats {})),
        };
        match call(&inner, &req).await?.kind {
            Some(response::Kind::Stats(s)) => Ok(ClusterStats {
                last_applied: s.last_applied.to_string(),
                current_leader: s.current_leader as u32,
            }),
            Some(response::Kind::Error(e)) => Err(err_response(e)),
            other => Err(Error::from_reason(format!("unexpected response: {other:?}"))),
        }
    }
}
