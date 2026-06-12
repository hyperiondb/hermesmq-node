use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hermesmq_proto as proto;
use hermesmq_proto::{request, response, Request, Response};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeFunction, ThreadsafeFunctionCallMode, UnknownReturnValue,
};
use napi_derive::napi;
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};

const MAX_FRAME: usize = 64 * 1024 * 1024;
const MAX_IDLE_PER_NODE: usize = 4;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const BASE_READ_TIMEOUT: Duration = Duration::from_secs(10);
const CALL_BACKOFF_CAP_MS: u64 = 500;
const SUBSCRIBE_BACKOFF_CAP_MS: u64 = 5_000;
const PIPELINE_DEPTH: usize = 32;
const PIPELINE_QUEUE: usize = 1024;

type OnMessage =
    ThreadsafeFunction<DeliveredMessage, MaybePromise, DeliveredMessage, Status, false>;
type OnError = ThreadsafeFunction<String, UnknownReturnValue, String, Status, false>;

pub struct MaybePromise(Option<Promise<()>>);

impl TypeName for MaybePromise {
    fn type_name() -> &'static str {
        "MaybePromise"
    }

    fn value_type() -> ValueType {
        ValueType::Unknown
    }
}

impl ValidateNapiValue for MaybePromise {
    unsafe fn validate(
        _env: napi::sys::napi_env,
        _napi_val: napi::sys::napi_value,
    ) -> Result<napi::sys::napi_value> {
        Ok(std::ptr::null_mut())
    }
}

impl FromNapiValue for MaybePromise {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        napi_val: napi::sys::napi_value,
    ) -> Result<Self> {
        let mut is_promise = false;
        let status = unsafe { napi::sys::napi_is_promise(env, napi_val, &mut is_promise) };
        if status != napi::sys::Status::napi_ok {
            return Err(Error::new(Status::GenericFailure, "napi_is_promise failed"));
        }
        if is_promise {
            Ok(MaybePromise(Some(unsafe {
                Promise::from_napi_value(env, napi_val)?
            })))
        } else {
            Ok(MaybePromise(None))
        }
    }
}

#[napi(object)]
pub struct NodeAddr {
    pub id: u32,
    pub client_addr: String,
    pub peer_addr: String,
}

#[napi(object)]
pub struct ConnectOptions {
    pub bootstrap: Option<bool>,
}

#[napi(object)]
pub struct RateLimit {
    pub rate_per_sec: f64,
    pub burst: u32,
}

#[napi(object)]
pub struct Retention {
    pub max_messages: i64,
    pub max_age_ms: i64,
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
    pub producer_id: Option<String>,
    pub seq: Option<i64>,
}

#[napi(object)]
pub struct ProduceManyResult {
    pub offset: Option<String>,
    pub error: Option<String>,
}

#[napi(object)]
pub struct PollOptions {
    pub topic: String,
    pub group: String,
    pub max: Option<u32>,
    pub visibility_ms: Option<i64>,
    pub wait_ms: Option<i64>,
}

#[napi(object)]
pub struct SubscribeOptions {
    pub topic: String,
    pub group: String,
    pub prefetch: Option<u32>,
    pub visibility_ms: Option<i64>,
    #[napi(ts_type = "'auto' | 'manual'")]
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
    pub current_term: String,
    pub last_log_index: String,
    pub is_leader: bool,
    pub topics: f64,
    pub messages: f64,
    pub in_flight: f64,
}

struct NodeRec {
    id: u64,
    client_addr: String,
    peer_addr: String,
}

struct Inner {
    nodes: Vec<NodeRec>,
    leader: AtomicUsize,
    pools: Vec<Mutex<Vec<TcpStream>>>,
    pipes: Vec<Mutex<Option<PipelineHandle>>>,
}

struct PipelineJob {
    req: Request,
    reply: oneshot::Sender<io::Result<Response>>,
}

#[derive(Clone)]
struct PipelineHandle {
    tx: mpsc::Sender<PipelineJob>,
}

impl PipelineHandle {
    async fn request(&self, req: Request, read_timeout: Duration) -> io::Result<Response> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(PipelineJob { req, reply })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "produce pipeline is closed"))?;
        match tokio::time::timeout(read_timeout, rx).await {
            Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "response timed out")),
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "produce pipeline dropped the request",
            )),
            Ok(Ok(result)) => result,
        }
    }
}

async fn run_pipeline(stream: TcpStream, mut jobs: mpsc::Receiver<PipelineJob>) {
    let (mut read_half, mut write_half) = stream.into_split();
    let (pending_tx, mut pending_rx) =
        mpsc::channel::<oneshot::Sender<io::Result<Response>>>(PIPELINE_DEPTH);

    let reader = tokio::spawn(async move {
        while let Some(reply) = pending_rx.recv().await {
            let result = match read_frame(&mut read_half).await {
                Ok(buf) => Response::decode(buf.as_slice()).map_err(io::Error::other),
                Err(e) => Err(e),
            };
            let failed = result.is_err();
            let _ = reply.send(result);
            if failed {
                break;
            }
        }
        while let Ok(reply) = pending_rx.try_recv() {
            let _ = reply.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection lost",
            )));
        }
    });

    while let Some(job) = jobs.recv().await {
        let frame = job.req.encode_to_vec();
        if pending_tx.send(job.reply).await.is_err() {
            break;
        }
        if write_frame(&mut write_half, &frame).await.is_err() {
            break;
        }
    }
    drop(pending_tx);
    drop(write_half);
    let _ = reader.await;
    jobs.close();
    while let Ok(job) = jobs.try_recv() {
        let _ = job.reply.send(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "connection lost",
        )));
    }
}

fn backoff(failures: u32, cap_ms: u64) -> Duration {
    let ms = 50u64.saturating_mul(1 << failures.min(7)).min(cap_ms);
    Duration::from_millis(ms)
}

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(bytes).await?;
    w.flush().await
}

async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds 64 MiB limit",
        ));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn request_on(
    stream: &mut TcpStream,
    req: &Request,
    read_timeout: Duration,
) -> io::Result<Response> {
    write_frame(stream, &req.encode_to_vec()).await?;
    let buf = tokio::time::timeout(read_timeout, read_frame(stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "response timed out"))??;
    Response::decode(buf.as_slice()).map_err(io::Error::other)
}

impl Inner {
    fn peer_index(&self, peer_addr: &str) -> Option<usize> {
        if peer_addr.is_empty() {
            return None;
        }
        self.nodes.iter().position(|n| n.peer_addr == peer_addr)
    }

    fn pooled(&self, idx: usize) -> Option<TcpStream> {
        self.pools[idx].lock().unwrap().pop()
    }

    async fn fresh(&self, idx: usize) -> io::Result<TcpStream> {
        let addr = &self.nodes[idx].client_addr;
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("connect to {addr} timed out"),
                )
            })??;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    fn park(&self, idx: usize, stream: TcpStream) {
        let mut pool = self.pools[idx].lock().unwrap();
        if pool.len() < MAX_IDLE_PER_NODE {
            pool.push(stream);
        }
    }

    async fn pipeline(&self, idx: usize) -> io::Result<PipelineHandle> {
        if let Some(handle) = self.pipes[idx].lock().unwrap().clone() {
            if !handle.tx.is_closed() {
                return Ok(handle);
            }
        }
        let stream = self.fresh(idx).await?;
        let mut slot = self.pipes[idx].lock().unwrap();
        if let Some(handle) = slot.as_ref() {
            if !handle.tx.is_closed() {
                return Ok(handle.clone());
            }
        }
        let (tx, rx) = mpsc::channel(PIPELINE_QUEUE);
        tokio::spawn(run_pipeline(stream, rx));
        let handle = PipelineHandle { tx };
        *slot = Some(handle.clone());
        Ok(handle)
    }

    fn drop_pipeline(&self, idx: usize) {
        self.pipes[idx].lock().unwrap().take();
    }

    async fn call_node(
        &self,
        idx: usize,
        req: &Request,
        read_timeout: Duration,
    ) -> io::Result<Response> {
        if let Some(mut stream) = self.pooled(idx) {
            if let Ok(resp) = request_on(&mut stream, req, read_timeout).await {
                self.park(idx, stream);
                return Ok(resp);
            }
        }
        let mut stream = self.fresh(idx).await?;
        let resp = request_on(&mut stream, req, read_timeout).await?;
        self.park(idx, stream);
        Ok(resp)
    }
}

async fn call(inner: &Inner, req: &Request, read_timeout: Duration) -> Result<Response> {
    let n = inner.nodes.len();
    let attempts = (n * 3).max(3);
    let mut idx = inner.leader.load(Ordering::Relaxed);
    let mut last = String::new();
    for attempt in 0..attempts {
        let i = idx % n;
        match inner.call_node(i, req, read_timeout).await {
            Ok(resp) => {
                if let Some(response::Kind::Error(e)) = &resp.kind {
                    if e.code == "not_leader" {
                        last = format!("not_leader from {}", inner.nodes[i].client_addr);
                        idx = inner.peer_index(&e.leader_addr).unwrap_or(i + 1);
                        if attempt + 1 < attempts {
                            tokio::time::sleep(backoff(attempt as u32, CALL_BACKOFF_CAP_MS)).await;
                        }
                        continue;
                    }
                }
                inner.leader.store(i, Ordering::Relaxed);
                return Ok(resp);
            }
            Err(e) => {
                last = format!("{}: {e}", inner.nodes[i].client_addr);
                idx = i + 1;
                if attempt + 1 < attempts {
                    tokio::time::sleep(backoff(attempt as u32, CALL_BACKOFF_CAP_MS)).await;
                }
            }
        }
    }
    Err(Error::from_reason(format!(
        "hermesmq: no reachable leader after {attempts} attempts; last error: {last}"
    )))
}

async fn call_pipelined(inner: &Inner, req: &Request, read_timeout: Duration) -> Result<Response> {
    let n = inner.nodes.len();
    let attempts = (n * 3).max(3);
    let mut idx = inner.leader.load(Ordering::Relaxed);
    let mut last = String::new();
    for attempt in 0..attempts {
        let i = idx % n;
        let pipe = match inner.pipeline(i).await {
            Ok(p) => p,
            Err(e) => {
                last = format!("{}: {e}", inner.nodes[i].client_addr);
                idx = i + 1;
                if attempt + 1 < attempts {
                    tokio::time::sleep(backoff(attempt as u32, CALL_BACKOFF_CAP_MS)).await;
                }
                continue;
            }
        };
        match pipe.request(req.clone(), read_timeout).await {
            Ok(resp) => {
                if let Some(response::Kind::Error(e)) = &resp.kind {
                    if e.code == "not_leader" {
                        last = format!("not_leader from {}", inner.nodes[i].client_addr);
                        inner.drop_pipeline(i);
                        idx = inner.peer_index(&e.leader_addr).unwrap_or(i + 1);
                        if attempt + 1 < attempts {
                            tokio::time::sleep(backoff(attempt as u32, CALL_BACKOFF_CAP_MS)).await;
                        }
                        continue;
                    }
                }
                inner.leader.store(i, Ordering::Relaxed);
                return Ok(resp);
            }
            Err(e) => {
                last = format!("{}: {e}", inner.nodes[i].client_addr);
                inner.drop_pipeline(i);
                idx = i + 1;
                if attempt + 1 < attempts {
                    tokio::time::sleep(backoff(attempt as u32, CALL_BACKOFF_CAP_MS)).await;
                }
            }
        }
    }
    Err(Error::from_reason(format!(
        "hermesmq: no reachable leader after {attempts} attempts; last error: {last}"
    )))
}

fn err_response(e: proto::Error) -> Error {
    let mut msg = format!("{}: {}", e.code, e.message);
    if e.retry_after_ms > 0 {
        msg.push_str(&format!(" (retry after {}ms)", e.retry_after_ms));
    }
    Error::from_reason(msg)
}

fn expect_ok(resp: Response) -> Result<()> {
    match resp.kind {
        Some(response::Kind::Ok(_)) => Ok(()),
        Some(response::Kind::Error(e)) => Err(err_response(e)),
        other => Err(Error::from_reason(format!(
            "unexpected response: {other:?}"
        ))),
    }
}

fn produce_request(options: ProduceOptions) -> Result<Request> {
    let producer_id = options.producer_id.unwrap_or_default();
    let seq = match options.seq {
        Some(s) if s >= 0 => s as u64,
        Some(_) => return Err(Error::from_reason("produce: seq must be >= 0")),
        None if !producer_id.is_empty() => {
            return Err(Error::from_reason(
                "produce: producerId requires seq (a per-producer monotonic counter)",
            ))
        }
        None => 0,
    };
    Ok(Request {
        kind: Some(request::Kind::Produce(proto::Produce {
            topic: options.topic,
            priority: options.priority.unwrap_or(0),
            content_type: 0,
            payload: options.body.to_vec(),
            producer_id,
            seq,
        })),
    })
}

fn produced_offset(resp: Response) -> Result<String> {
    match resp.kind {
        Some(response::Kind::Produced(p)) => Ok(p.offset.to_string()),
        Some(response::Kind::Error(e)) => Err(err_response(e)),
        other => Err(Error::from_reason(format!(
            "unexpected response: {other:?}"
        ))),
    }
}

fn delivered_message(d: proto::Delivered) -> DeliveredMessage {
    DeliveredMessage {
        lease_id: d.lease_id.to_string(),
        offset: d.offset.to_string(),
        priority: d.priority,
        content_type: d.content_type,
        payload: Buffer::from(d.payload),
        ts_ms: d.ts_ms.to_string(),
    }
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

enum SubEnd {
    NotLeader(Option<usize>),
    Failed(String),
    Stopped,
}

struct SubscriptionTask {
    inner: Arc<Inner>,
    topic: String,
    group: String,
    auto: bool,
    sub_req: Request,
    on_message: Arc<OnMessage>,
    on_error: Option<Arc<OnError>>,
    stop: Arc<AtomicBool>,
}

impl SubscriptionTask {
    fn dispatch(&self, d: proto::Delivered, tx: &UnboundedSender<Request>) {
        let lease_id = d.lease_id;
        let msg = delivered_message(d);
        if self.auto {
            self.on_message
                .call(msg, ThreadsafeFunctionCallMode::NonBlocking);
            return;
        }
        let on_message = self.on_message.clone();
        let tx = tx.clone();
        let topic = self.topic.clone();
        let group = self.group.clone();
        tokio::spawn(async move {
            let ok = match on_message.call_async_catch(msg).await {
                Ok(MaybePromise(Some(promise))) => promise.await.is_ok(),
                Ok(MaybePromise(None)) => true,
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

    async fn once(&self, i: usize, failures: &mut u32) -> SubEnd {
        let addr = self.inner.nodes[i].client_addr.clone();
        let stream = match self.inner.fresh(i).await {
            Ok(s) => s,
            Err(e) => return SubEnd::Failed(format!("{addr}: {e}")),
        };
        let (mut read_half, mut write_half) = stream.into_split();
        if let Err(e) = write_frame(&mut write_half, &self.sub_req.encode_to_vec()).await {
            return SubEnd::Failed(format!("{addr}: {e}"));
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                if write_frame(&mut write_half, &req.encode_to_vec())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        loop {
            let buf = match read_frame(&mut read_half).await {
                Ok(b) => b,
                Err(e) => {
                    return if self.stop.load(Ordering::SeqCst) {
                        SubEnd::Stopped
                    } else {
                        SubEnd::Failed(format!("{addr}: {e}"))
                    }
                }
            };
            if self.stop.load(Ordering::SeqCst) {
                return SubEnd::Stopped;
            }
            let Ok(resp) = Response::decode(buf.as_slice()) else {
                continue;
            };
            match resp.kind {
                Some(response::Kind::Polled(p)) => {
                    *failures = 0;
                    self.inner.leader.store(i, Ordering::Relaxed);
                    for d in p.items {
                        self.dispatch(d, &tx);
                    }
                }
                Some(response::Kind::Error(e)) if e.code == "not_leader" => {
                    return SubEnd::NotLeader(self.inner.peer_index(&e.leader_addr));
                }
                Some(response::Kind::Error(e)) => {
                    return SubEnd::Failed(format!("{}: {}", e.code, e.message));
                }
                _ => {}
            }
        }
    }

    async fn run(self) {
        let n = self.inner.nodes.len();
        let mut idx = self.inner.leader.load(Ordering::Relaxed);
        let mut failures = 0u32;
        while !self.stop.load(Ordering::SeqCst) {
            let i = idx % n;
            match self.once(i, &mut failures).await {
                SubEnd::Stopped => return,
                SubEnd::NotLeader(hint) => {
                    idx = hint.unwrap_or(i + 1);
                    failures += 1;
                }
                SubEnd::Failed(msg) => {
                    if let Some(cb) = &self.on_error {
                        cb.call(msg, ThreadsafeFunctionCallMode::NonBlocking);
                    }
                    idx = i + 1;
                    failures += 1;
                }
            }
            if self.stop.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(backoff(
                failures.saturating_sub(1),
                SUBSCRIBE_BACKOFF_CAP_MS,
            ))
            .await;
        }
    }
}

#[napi]
pub struct Client {
    inner: Arc<Inner>,
}

#[napi]
pub async fn connect(nodes: Vec<NodeAddr>, options: Option<ConnectOptions>) -> Result<Client> {
    if nodes.is_empty() {
        return Err(Error::from_reason("connect: `nodes` must not be empty"));
    }
    let recs: Vec<NodeRec> = nodes
        .into_iter()
        .map(|n| NodeRec {
            id: n.id as u64,
            client_addr: n.client_addr,
            peer_addr: n.peer_addr,
        })
        .collect();
    let pools = recs.iter().map(|_| Mutex::new(Vec::new())).collect();
    let pipes = recs.iter().map(|_| Mutex::new(None)).collect();
    let client = Client {
        inner: Arc::new(Inner {
            nodes: recs,
            leader: AtomicUsize::new(0),
            pools,
            pipes,
        }),
    };
    if options.and_then(|o| o.bootstrap).unwrap_or(false) {
        client.bootstrap().await?;
    } else {
        client.stats().await?;
    }
    Ok(client)
}

#[napi]
impl Client {
    #[napi]
    pub async fn bootstrap(&self) -> Result<()> {
        let nodes = self
            .inner
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
        expect_ok(call(&self.inner, &req, BASE_READ_TIMEOUT).await?)
    }

    #[napi]
    pub async fn create_topic(&self, options: CreateTopicOptions) -> Result<()> {
        let req = Request {
            kind: Some(request::Kind::CreateTopic(proto::CreateTopic {
                topic: options.topic.clone(),
            })),
        };
        expect_ok(call(&self.inner, &req, BASE_READ_TIMEOUT).await?)?;
        if let Some(rate_limit) = options.rate_limit {
            let req = Request {
                kind: Some(request::Kind::SetRateLimit(proto::SetRateLimit {
                    topic: options.topic.clone(),
                    rate_per_sec: rate_limit.rate_per_sec,
                    burst: rate_limit.burst,
                })),
            };
            expect_ok(call(&self.inner, &req, BASE_READ_TIMEOUT).await?)?;
        }
        if let Some(retention) = options.retention {
            let req = Request {
                kind: Some(request::Kind::SetRetention(proto::SetRetention {
                    topic: options.topic,
                    max_messages: retention.max_messages.max(0) as u64,
                    max_age_ms: retention.max_age_ms.max(0) as u64,
                })),
            };
            expect_ok(call(&self.inner, &req, BASE_READ_TIMEOUT).await?)?;
        }
        Ok(())
    }

    #[napi]
    pub async fn produce(&self, options: ProduceOptions) -> Result<String> {
        let req = produce_request(options)?;
        produced_offset(call_pipelined(&self.inner, &req, BASE_READ_TIMEOUT).await?)
    }

    #[napi]
    pub async fn produce_many(&self, items: Vec<ProduceOptions>) -> Result<Vec<ProduceManyResult>> {
        let mut handles = Vec::with_capacity(items.len());
        for item in items {
            let inner = self.inner.clone();
            handles.push(tokio::spawn(async move {
                let req = produce_request(item)?;
                produced_offset(call_pipelined(&inner, &req, BASE_READ_TIMEOUT).await?)
            }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for handle in handles {
            out.push(match handle.await {
                Ok(Ok(offset)) => ProduceManyResult {
                    offset: Some(offset),
                    error: None,
                },
                Ok(Err(e)) => ProduceManyResult {
                    offset: None,
                    error: Some(e.reason.to_string()),
                },
                Err(e) => ProduceManyResult {
                    offset: None,
                    error: Some(e.to_string()),
                },
            });
        }
        Ok(out)
    }

    #[napi]
    pub async fn poll(&self, options: PollOptions) -> Result<Vec<DeliveredMessage>> {
        let wait_ms = options.wait_ms.unwrap_or(0).max(0) as u64;
        let req = Request {
            kind: Some(request::Kind::Poll(proto::Poll {
                topic: options.topic,
                group: options.group,
                max: options.max.unwrap_or(16),
                visibility_timeout_ms: options.visibility_ms.unwrap_or(30_000).max(0) as u64,
                ack_mode: "manual".to_string(),
                wait_ms,
            })),
        };
        let read_timeout = BASE_READ_TIMEOUT + Duration::from_millis(wait_ms);
        match call(&self.inner, &req, read_timeout).await?.kind {
            Some(response::Kind::Polled(p)) => {
                Ok(p.items.into_iter().map(delivered_message).collect())
            }
            Some(response::Kind::Error(e)) => Err(err_response(e)),
            other => Err(Error::from_reason(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    #[napi(
        ts_args_type = "options: SubscribeOptions, onMessage: (msg: DeliveredMessage) => void | Promise<void>, onError?: (err: string) => void"
    )]
    pub async fn subscribe(
        &self,
        options: SubscribeOptions,
        on_message: OnMessage,
        on_error: Option<OnError>,
    ) -> Result<Subscription> {
        let auto = match options.ack_mode.as_deref() {
            None | Some("manual") => false,
            Some("auto") => true,
            Some(other) => {
                return Err(Error::from_reason(format!(
                    "subscribe: invalid ackMode {other:?}; expected \"auto\" or \"manual\""
                )))
            }
        };
        let stop = Arc::new(AtomicBool::new(false));
        let sub_req = Request {
            kind: Some(request::Kind::Subscribe(proto::Subscribe {
                topic: options.topic.clone(),
                group: options.group.clone(),
                prefetch: options.prefetch.unwrap_or(16),
                visibility_timeout_ms: options.visibility_ms.unwrap_or(30_000).max(0) as u64,
                ack_mode: if auto { "auto" } else { "manual" }.to_string(),
            })),
        };
        let task = SubscriptionTask {
            inner: self.inner.clone(),
            topic: options.topic,
            group: options.group,
            auto,
            sub_req,
            on_message: Arc::new(on_message),
            on_error: on_error.map(Arc::new),
            stop: stop.clone(),
        };
        let handle = tokio::spawn(task.run());
        Ok(Subscription {
            stop,
            abort: handle.abort_handle(),
        })
    }

    #[napi]
    pub async fn ack(&self, lease: LeaseRef) -> Result<()> {
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
        expect_ok(call(&self.inner, &req, BASE_READ_TIMEOUT).await?)
    }

    #[napi]
    pub async fn nack(&self, lease: LeaseRef) -> Result<()> {
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
        expect_ok(call(&self.inner, &req, BASE_READ_TIMEOUT).await?)
    }

    #[napi]
    pub async fn stats(&self) -> Result<ClusterStats> {
        let req = Request {
            kind: Some(request::Kind::Stats(proto::Stats {})),
        };
        match call(&self.inner, &req, BASE_READ_TIMEOUT).await?.kind {
            Some(response::Kind::Stats(s)) => Ok(ClusterStats {
                last_applied: s.last_applied.to_string(),
                current_leader: s.current_leader as u32,
                current_term: s.current_term.to_string(),
                last_log_index: s.last_log_index.to_string(),
                is_leader: s.is_leader,
                topics: s.topics as f64,
                messages: s.messages as f64,
                in_flight: s.in_flight as f64,
            }),
            Some(response::Kind::Error(e)) => Err(err_response(e)),
            other => Err(Error::from_reason(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    #[napi]
    pub fn close(&self) {
        for pool in &self.inner.pools {
            pool.lock().unwrap().clear();
        }
        for pipe in &self.inner.pipes {
            pipe.lock().unwrap().take();
        }
    }
}
