mod graph;
#[cfg(test)]
mod tests;

use self::graph::{Graph, Key as GraphKey};
use super::{constants::REQUEST_TIMEOUT, message::Request};
use crate::{
    collections::HashMap,
    crypto::{sign::PublicKey, Hash},
    protocol::{BlockId, MultiBlockPresence},
};
use std::{
    collections::VecDeque,
    iter, mem,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::{select, sync::mpsc, task};
use tokio_stream::StreamExt;
use tokio_util::time::{delay_queue, DelayQueue};
use tracing::instrument;

/// Keeps track of in-flight requests. Falls back on another peer in case the request failed (due to
/// error response, timeout or disconnection). Evenly distributes the requests between the peers
/// and ensures every request is only sent to one peer at a time.
pub(super) struct RequestTracker {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl RequestTracker {
    #[expect(dead_code)]
    pub fn new() -> Self {
        let (this, worker) = build();
        task::spawn(worker.run());
        this
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub fn new_client(&self) -> (RequestTrackerClient, mpsc::UnboundedReceiver<Request>) {
        let client_id = ClientId::next();
        let (request_tx, request_rx) = mpsc::unbounded_channel();

        self.command_tx
            .send(Command::InsertClient {
                client_id,
                request_tx,
            })
            .ok();

        (
            RequestTrackerClient {
                client_id,
                command_tx: self.command_tx.clone(),
            },
            request_rx,
        )
    }
}

pub(super) struct RequestTrackerClient {
    client_id: ClientId,
    command_tx: mpsc::UnboundedSender<Command>,
}

impl RequestTrackerClient {
    /// Handle sending a request that does not follow from any previously received response.
    #[expect(dead_code)]
    pub fn initial(&self, request: Request, block_presence: MultiBlockPresence) {
        self.command_tx
            .send(Command::HandleInitial {
                client_id: self.client_id,
                request,
                block_presence,
            })
            .ok();
    }

    /// Handle sending requests that follow from a received success response.
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn success(&self, request_key: MessageKey, requests: Vec<(Request, MultiBlockPresence)>) {
        self.command_tx
            .send(Command::HandleSuccess {
                client_id: self.client_id,
                request_key,
                requests,
            })
            .ok();
    }

    /// Handle failure response.
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn failure(&self, request_key: MessageKey) {
        self.command_tx
            .send(Command::HandleFailure {
                client_id: self.client_id,
                request_key,
            })
            .ok();
    }
}

impl Drop for RequestTrackerClient {
    fn drop(&mut self) {
        self.command_tx
            .send(Command::RemoveClient {
                client_id: self.client_id,
            })
            .ok();
    }
}

/// Key identifying a request and its corresponding response.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub(super) enum MessageKey {
    RootNode(PublicKey),
    ChildNodes(Hash),
    Block(BlockId),
}

impl<'a> From<&'a Request> for MessageKey {
    fn from(request: &'a Request) -> Self {
        match request {
            Request::RootNode(writer_id, _) => MessageKey::RootNode(*writer_id),
            Request::ChildNodes(hash, _, _) => MessageKey::ChildNodes(*hash),
            Request::Block(block_id, _) => MessageKey::Block(*block_id),
        }
    }
}

fn build() -> (RequestTracker, Worker) {
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    (RequestTracker { command_tx }, Worker::new(command_rx))
}

struct Worker {
    command_rx: mpsc::UnboundedReceiver<Command>,
    clients: HashMap<ClientId, ClientState>,
    requests: Graph<RequestState>,
    timer: DelayQueue<(ClientId, MessageKey)>,
}

impl Worker {
    fn new(command_rx: mpsc::UnboundedReceiver<Command>) -> Self {
        Self {
            command_rx,
            clients: HashMap::default(),
            requests: Graph::new(),
            timer: DelayQueue::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            select! {
                command = self.command_rx.recv() => {
                    if let Some(command) = command {
                        self.handle_command(command);
                    } else {
                        break;
                    }
                }
                Some(expired) = self.timer.next() => {
                    let (client_id, request_key) = expired.into_inner();
                    self.handle_failure(client_id, request_key, FailureReason::Timeout);
                }
            }
        }
    }

    /// Process all currently queued commands.
    #[cfg(test)]
    pub fn step(&mut self) {
        while let Ok(command) = self.command_rx.try_recv() {
            self.handle_command(command);
        }
    }

    #[cfg(test)]
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::InsertClient {
                client_id,
                request_tx,
            } => {
                self.insert_client(client_id, request_tx);
            }
            Command::RemoveClient { client_id } => {
                self.remove_client(client_id);
            }
            Command::HandleInitial {
                client_id,
                request,
                block_presence,
            } => {
                self.handle_initial(client_id, request, block_presence);
            }
            Command::HandleSuccess {
                client_id,
                request_key,
                requests,
            } => {
                self.handle_success(client_id, request_key, requests);
            }
            Command::HandleFailure {
                client_id,
                request_key,
            } => {
                self.handle_failure(client_id, request_key, FailureReason::Response);
            }
        }
    }

    #[instrument(skip(self, request_tx))]
    fn insert_client(&mut self, client_id: ClientId, request_tx: mpsc::UnboundedSender<Request>) {
        // tracing::debug!("insert_client");
        self.clients.insert(client_id, ClientState::new(request_tx));
    }

    #[instrument(skip(self))]
    fn remove_client(&mut self, client_id: ClientId) {
        // tracing::debug!("remove_client");

        let Some(client_state) = self.clients.remove(&client_id) else {
            return;
        };

        for (_, node_key) in client_state.requests {
            self.cancel_request(client_id, node_key);
        }
    }

    #[instrument(skip(self))]
    fn handle_initial(
        &mut self,
        client_id: ClientId,
        request: Request,
        block_presence: MultiBlockPresence,
    ) {
        // tracing::debug!("handle_initial");
        self.insert_request(client_id, request, block_presence, None)
    }

    #[instrument(skip(self))]
    fn handle_success(
        &mut self,
        client_id: ClientId,
        request_key: MessageKey,
        requests: Vec<(Request, MultiBlockPresence)>,
    ) {
        // tracing::debug!("handle_success");

        let node_key = self
            .clients
            .get_mut(&client_id)
            .and_then(|client_state| client_state.requests.remove(&request_key));

        let mut client_ids = if let Some(node_key) = node_key {
            let (client_ids, remove) = match self.requests.get_mut(node_key) {
                Some(node) => match node.value_mut() {
                    RequestState::InFlight {
                        sender_client_id,
                        sender_timer_key,
                        waiting,
                    } if *sender_client_id == client_id => {
                        self.timer.try_remove(sender_timer_key);

                        let waiting = mem::take(waiting);

                        // If this request has, or will have, children, mark it as complete, otherwise
                        // remove it.
                        let remove = if node.children().len() > 0 || !requests.is_empty() {
                            *node.value_mut() = RequestState::Complete;
                            false
                        } else {
                            true
                        };

                        (waiting, remove)
                    }
                    RequestState::InFlight { .. }
                    | RequestState::Complete
                    | RequestState::Cancelled => return,
                },
                None => return,
            };

            if remove {
                self.remove_request(node_key);
            }

            client_ids
        } else {
            Default::default()
        };

        // Register the followup (child) requests with this client but also with all the clients that
        // were waiting for the original request.
        client_ids.push_front(client_id);

        for (child_request, child_block_presence) in requests {
            for (client_id, child_request) in
                // TODO: use `repeat_n` once it gets stabilized.
                client_ids.iter().copied().zip(iter::repeat(child_request))
            {
                self.insert_request(
                    client_id,
                    child_request.clone(),
                    child_block_presence,
                    node_key,
                );
            }

            // Round-robin the requests among the clients.
            client_ids.rotate_left(1);
        }
    }

    #[instrument(skip(self))]
    fn handle_failure(
        &mut self,
        client_id: ClientId,
        request_key: MessageKey,
        reason: FailureReason,
    ) {
        // tracing::debug!("handle_failure");

        let Some(client_state) = self.clients.get_mut(&client_id) else {
            return;
        };

        let Some(node_key) = client_state.requests.remove(&request_key) else {
            return;
        };

        self.cancel_request(client_id, node_key);
    }

    fn insert_request(
        &mut self,
        client_id: ClientId,
        request: Request,
        block_presence: MultiBlockPresence,
        parent_key: Option<GraphKey>,
    ) {
        let node_key = self.requests.get_or_insert(
            request,
            block_presence,
            parent_key,
            RequestState::Cancelled,
        );

        self.update_request(client_id, node_key);
    }

    fn update_request(&mut self, client_id: ClientId, node_key: GraphKey) {
        let Some(node) = self.requests.get_mut(node_key) else {
            return;
        };

        let Some(client_state) = self.clients.get_mut(&client_id) else {
            return;
        };

        let request_key = MessageKey::from(node.request());

        match node.value_mut() {
            RequestState::InFlight { waiting, .. } => {
                waiting.push_back(client_id);
                client_state.requests.insert(request_key, node_key);
            }
            RequestState::Complete => (),
            RequestState::Cancelled => {
                let timer_key = self.timer.insert((client_id, request_key), REQUEST_TIMEOUT);

                *node.value_mut() = RequestState::InFlight {
                    sender_client_id: client_id,
                    sender_timer_key: timer_key,
                    waiting: VecDeque::new(),
                };

                client_state.requests.insert(request_key, node_key);
                client_state.request_tx.send(node.request().clone()).ok();
            }
        }

        // Note: we are using recursion, but the graph is only a few layers deep (currently 5) so
        // there is no danger of stack overflow.
        let children: Vec<_> = node.children().collect();

        for child_key in children {
            self.update_request(client_id, child_key);
        }
    }

    fn cancel_request(&mut self, client_id: ClientId, node_key: GraphKey) {
        let Some(node) = self.requests.get_mut(node_key) else {
            return;
        };

        let (request, state) = node.request_and_value_mut();

        let remove = match state {
            RequestState::InFlight {
                sender_client_id,
                sender_timer_key,
                waiting,
            } => {
                if *sender_client_id == client_id {
                    // The removed client is the current sender of this request.

                    // Remove the timeout for the previous sender
                    self.timer.try_remove(sender_timer_key);

                    // Find a waiting client
                    let next_client = iter::from_fn(|| waiting.pop_front()).find_map(|client_id| {
                        self.clients
                            .get(&client_id)
                            .map(|client_state| (client_id, client_state))
                    });

                    if let Some((next_client_id, next_client_state)) = next_client {
                        // Next waiting client found. Promote it to a sender.

                        *sender_client_id = next_client_id;
                        *sender_timer_key = self
                            .timer
                            .insert((next_client_id, MessageKey::from(request)), REQUEST_TIMEOUT);

                        // Send the request to the new sender.
                        next_client_state.request_tx.send(request.clone()).ok();

                        false
                    } else {
                        // No waiting client found. If this request has no children, we can remove
                        // it, otherwise we mark it as cancelled.
                        if node.children().len() > 0 {
                            *node.value_mut() = RequestState::Cancelled;
                            false
                        } else {
                            true
                        }
                    }
                } else {
                    // The removed client is one of the waiting clients - remove it from the
                    // waiting queue.
                    if let Some(index) = waiting
                        .iter()
                        .position(|next_client_id| *next_client_id == client_id)
                    {
                        waiting.remove(index);
                    }

                    false
                }
            }
            RequestState::Complete | RequestState::Cancelled => false,
        };

        if remove {
            self.remove_request(node_key);
        }
    }

    fn remove_request(&mut self, node_key: GraphKey) {
        let Some(node) = self.requests.remove(node_key) else {
            return;
        };

        for parent_key in node.parents() {
            let Some(parent_node) = self.requests.get(parent_key) else {
                continue;
            };

            if parent_node.children().len() > 0 {
                continue;
            }

            self.remove_request(parent_key);
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
struct ClientId(usize);

impl ClientId {
    fn next() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

enum Command {
    InsertClient {
        client_id: ClientId,
        request_tx: mpsc::UnboundedSender<Request>,
    },
    RemoveClient {
        client_id: ClientId,
    },
    HandleInitial {
        client_id: ClientId,
        request: Request,
        block_presence: MultiBlockPresence,
    },
    HandleSuccess {
        client_id: ClientId,
        request_key: MessageKey,
        requests: Vec<(Request, MultiBlockPresence)>,
    },
    HandleFailure {
        client_id: ClientId,
        request_key: MessageKey,
    },
}

struct ClientState {
    request_tx: mpsc::UnboundedSender<Request>,
    requests: HashMap<MessageKey, GraphKey>,
}

impl ClientState {
    fn new(request_tx: mpsc::UnboundedSender<Request>) -> Self {
        Self {
            request_tx,
            requests: HashMap::default(),
        }
    }
}

enum RequestState {
    /// This request is currently in flight
    InFlight {
        /// Client who's sending this request
        sender_client_id: ClientId,
        /// Timeout key for the request
        sender_timer_key: delay_queue::Key,
        /// Other clients interested in sending this request. If the current client fails or
        /// timeouts, a new one will be picked from this list.
        waiting: VecDeque<ClientId>,
    },
    /// The response to this request has already been received.
    Complete,
    /// The response for the current client failed and there are no more clients waiting.
    Cancelled,
}

#[derive(Debug)]
enum FailureReason {
    Response,
    Timeout,
}
