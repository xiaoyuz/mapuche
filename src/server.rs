use crate::{
    Command, Connection, Db, DbDropGuard, MapucheError, Shutdown, P2P_CLIENT, RAFT_CLIENT,
    RING_NODES,
};
use std::collections::HashMap;

use crate::client::Client;
use crate::config::{
    async_gc_worker_number_or_default, config_cluster_or_default, config_infra_or_default,
    config_local_pool_number, config_max_connection, is_auth_enabled, is_auth_matched, LOGGER,
};
use crate::gc::GcMaster;
use crate::metrics::{
    CURRENT_CONNECTION_COUNTER, RAFT_REMOTE_COUNTER, RAFT_REMOTE_DURATION, REQUEST_CMD_COUNTER,
    REQUEST_CMD_ERROR_COUNTER, REQUEST_CMD_FINISH_COUNTER, REQUEST_CMD_HANDLE_TIME,
    REQUEST_CMD_REMOTE_COUNTER, REQUEST_COUNTER, TOTAL_CONNECTION_PROCESSED,
};
use crate::p2p::message::Message;
use crate::rocks::errors::{
    REDIS_AUTH_INVALID_PASSWORD_ERR, REDIS_AUTH_REQUIRED_ERR, REDIS_AUTH_WHEN_DISABLED_ERR,
};
use crate::utils::{now_timestamp_in_millis, resp_err, resp_invalid_arguments, resp_ok};
use local_ip_address::local_ip;
use slog::{debug, error, info};
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

use crate::cmd::CommandType;
use crate::raft::store::RaftResponse;
use crate::raft::RaftRequest;
use tokio::sync::{broadcast, mpsc, Mutex, Semaphore};
use tokio::time::{self, Duration, Instant};
use tokio_util::task::LocalPoolHandle;
use uuid::Uuid;

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// Shared database handle.
    ///
    /// Contains the key / value store as well as the broadcast channels for
    /// pub/sub.
    ///
    /// This holds a wrapper around an `Arc`. The internal `Db` can be
    /// retrieved and passed into the per connection state (`Handler`).
    db_holder: DbDropGuard,

    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,

    limit_connections: Arc<Semaphore>,
    clients: Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`.
#[derive(Debug)]
struct Handler {
    db: Db,
    cur_client: Arc<Mutex<Client>>,
    clients: Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>,
    connection: Connection,
    shutdown: Shutdown,
    authorized: bool,
    _shutdown_complete: mpsc::Sender<()>,
}

/// Run the mapuche server.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
///
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the subscribe() method on the sender is used to create
    // one.
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let db_holder = DbDropGuard::new();

    // Initialize the listener state
    let mut server = Listener {
        listener,
        db_holder: db_holder.clone(),
        limit_connections: Arc::new(Semaphore::new(config_max_connection())),
        clients: Arc::new(Mutex::new(HashMap::new())),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    let mut gc_master = GcMaster::new(async_gc_worker_number_or_default());
    gc_master.start_workers().await;

    tokio::select! {
        res = server.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(LOGGER, "failed to accept, case {}", err.to_string());
            }
        }
        _ = gc_master.run() => {
            error!(LOGGER, "gc master exit");
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!(LOGGER, "shutting down");
        }
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    async fn run(&mut self) -> crate::Result<()> {
        info!(LOGGER, "accepting inbound connections");

        let local_pool_number = config_local_pool_number();
        let local_pool = LocalPoolHandle::new(local_pool_number);

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;
            let (kill_tx, kill_rx) = mpsc::channel(1);
            let client = Client::new(&socket, kill_tx);
            let client_id = client.id();
            let arc_client = Arc::new(Mutex::new(client));
            self.clients
                .lock()
                .await
                .insert(client_id, arc_client.clone());

            // Create the necessary per-connection handler state.
            let mut handler = Handler {
                db: self.db_holder.db(),
                cur_client: arc_client.clone(),
                clients: self.clients.clone(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe(), kill_rx),
                authorized: !is_auth_enabled(),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };
            local_pool.spawn_pinned(|| async move {
                // Process the connection. If an error is encountered, log it.
                CURRENT_CONNECTION_COUNTER.inc();
                TOTAL_CONNECTION_PROCESSED.inc();
                if let Err(err) = handler.run().await {
                    error!(LOGGER, "connection error {:?}", err);
                }
                handler
                    .clients
                    .lock()
                    .await
                    .remove(&handler.cur_client.lock().await.id());
                CURRENT_CONNECTION_COUNTER.dec();
                drop(permit)
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    error!(LOGGER, "Accept Error! {:?}", &err);
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

impl Handler {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    async fn run(&mut self) -> crate::Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Convert the redis frame into a command struct. This returns an
            // error if the frame is not a valid redis command or it is an
            // unsupported command.
            let cmd = Command::from_frame(frame)?;
            let cmd_name = cmd.get_name().to_owned();

            {
                let mut w_client = self.cur_client.lock().await;
                w_client.interact(&cmd_name);
            }

            let start_at = Instant::now();
            REQUEST_COUNTER.inc();
            REQUEST_CMD_COUNTER.with_label_values(&[&cmd_name]).inc();

            debug!(LOGGER, "req, {:?}", cmd);

            match cmd {
                Command::Auth(c) => {
                    if !c.valid() {
                        self.connection
                            .write_frame(&resp_invalid_arguments())
                            .await?;
                    } else if !is_auth_enabled() {
                        // check password and update connection authorized flag
                        self.connection
                            .write_frame(&resp_err(REDIS_AUTH_WHEN_DISABLED_ERR))
                            .await?;
                    } else if is_auth_matched(c.passwd()) {
                        self.connection.write_frame(&resp_ok()).await?;
                        self.authorized = true;
                    } else {
                        self.connection
                            .write_frame(&resp_err(REDIS_AUTH_INVALID_PASSWORD_ERR))
                            .await?;
                    }
                }
                _ => {
                    if !self.authorized {
                        self.connection
                            .write_frame(&resp_err(REDIS_AUTH_REQUIRED_ERR))
                            .await?;
                    } else {
                        let execute_res = if config_cluster_or_default().is_empty() {
                            self.execute_locally(cmd).await
                        } else {
                            self.execute_on_ring(cmd).await
                        };
                        match execute_res {
                            Ok(_) => (),
                            Err(e) => {
                                REQUEST_CMD_ERROR_COUNTER
                                    .with_label_values(&[&cmd_name])
                                    .inc();
                                return Err(e);
                            }
                        }
                    }
                }
            }
            let duration = Instant::now() - start_at;
            REQUEST_CMD_HANDLE_TIME
                .with_label_values(&[&cmd_name])
                .observe(duration_to_sec(duration));
            REQUEST_CMD_FINISH_COUNTER
                .with_label_values(&[&cmd_name])
                .inc();
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn execute_on_ring(&mut self, cmd: Command) -> crate::Result<()> {
        let hash_ring_key = cmd.hash_ring_key()?;
        let local_address = local_ip()?.to_string();
        let message = Message::CmdReqMessage {
            address: local_address.clone(),
            cmd: cmd.clone(),
            ts: now_timestamp_in_millis(),
            req_id: Uuid::new_v4().to_string(),
        };
        unsafe {
            if let Some(hash_ring) = &RING_NODES {
                let remote_node = hash_ring
                    .get_node(hash_ring_key)
                    .ok_or(MapucheError::String("hash ring node not matched"))?;
                let remote_url: String = remote_node.into();
                if local_address == remote_url {
                    self.execute_locally(cmd).await?;
                } else {
                    let cmd_name = cmd.get_name().to_owned();
                    REQUEST_CMD_REMOTE_COUNTER
                        .with_label_values(&[&cmd_name])
                        .inc();
                    self.do_remote_execute(message, &remote_url).await?;
                }
            } else {
                Err(MapucheError::String("hash ring not inited"))?
            }
        }
        Ok(())
    }

    async unsafe fn do_remote_execute(
        &mut self,
        message: Message,
        remote_url: &str,
    ) -> crate::Result<()> {
        if let Some(client) = &P2P_CLIENT {
            let rec = client.subscribe(remote_url).await;
            client.call(remote_url, message).await?;
            let res = rec
                .ok_or(MapucheError::String("p2p client not inited"))?
                .recv()
                .await?;
            if let Message::CmdRespMessage {
                address,
                frame,
                ts: _,
                req_id: _,
            } = res
            {
                debug!(LOGGER, "res from remote address, {:?}, {}", frame, address);
                self.connection.write_frame(&frame).await?;
            }
        } else {
            Err(MapucheError::String("p2p client not inited"))?
        }
        Ok(())
    }

    async fn execute_locally(&mut self, cmd: Command) -> crate::Result<()> {
        if !config_infra_or_default().need_raft() {
            return cmd
                .apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await;
        }
        unsafe {
            if let (Some(client), CommandType::WRITE) = (&RAFT_CLIENT, cmd.cmd_type()) {
                RAFT_REMOTE_COUNTER.inc();
                let start_at = Instant::now();
                let response = client
                    .write(&RaftRequest::CmdLog {
                        id: Uuid::new_v4().to_string(),
                        cmd,
                    })
                    .await?;
                let duration = Instant::now() - start_at;
                RAFT_REMOTE_DURATION.observe(duration_to_sec(duration));
                if let RaftResponse::Frame(frame) = response.data {
                    debug!(LOGGER, "res from raft, {:?}", frame);
                    self.connection.write_frame(&frame).await?;
                }
            } else {
                cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                    .await?;
            }
        }
        Ok(())
    }
}

#[inline]
pub fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}
