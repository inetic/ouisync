use super::message::{Content, Request, Response};
use crate::{
    block::{self, BlockId, BLOCK_SIZE},
    crypto::{sign::PublicKey, Hash},
    error::{Error, Result},
    event::{Event, Payload},
    index::{Index, InnerNode, LeafNode, RootNode},
};
use futures_util::TryStreamExt;
use tokio::{
    select,
    sync::{broadcast::error::RecvError, mpsc},
};
use tracing::instrument;

pub(crate) struct Server {
    index: Index,
    tx: Sender,
    rx: Receiver,
}

impl Server {
    pub fn new(index: Index, tx: mpsc::Sender<Content>, rx: mpsc::Receiver<Request>) -> Self {
        Self {
            index,
            tx: Sender(tx),
            rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let Self { index, tx, rx } = self;
        let responder = Responder::new(index, tx, rx);
        let monitor = Monitor::new(index, tx);

        select! {
            result = responder.run() => result,
            result = monitor.run() => result,
        }
    }
}

/// Receives requests from the peer and replies with responses.
struct Responder<'a> {
    index: &'a Index,
    tx: &'a Sender,
    rx: &'a mut Receiver,
}

impl<'a> Responder<'a> {
    fn new(index: &'a Index, tx: &'a Sender, rx: &'a mut Receiver) -> Self {
        Self { index, tx, rx }
    }

    async fn run(mut self) -> Result<()> {
        while let Some(request) = self.rx.recv().await {
            self.handle_request(request).await?;
        }

        Ok(())
    }

    async fn handle_request(&mut self, request: Request) -> Result<()> {
        match request {
            Request::RootNode(branch_id) => self.handle_root_node(branch_id).await,
            Request::ChildNodes(parent_hash) => self.handle_child_nodes(parent_hash).await,
            Request::Block(id) => self.handle_block(id).await,
        }
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_root_node(&mut self, branch_id: PublicKey) -> Result<()> {
        println!("server.handle_root_node");
        let mut conn = self.index.pool.acquire().await?;
        let root_node = RootNode::load_latest_complete_by_writer(&mut conn, branch_id).await;

        match root_node {
            Ok(node) => {
                tracing::trace!("root node found");

                let response = Response::RootNode {
                    proof: node.proof.into(),
                    summary: node.summary,
                };

                self.tx.send(response).await;
                Ok(())
            }
            Err(Error::EntryNotFound) => {
                tracing::warn!("root node not found");
                self.tx.send(Response::RootNodeError(branch_id)).await;
                Ok(())
            }
            Err(error) => {
                self.tx.send(Response::RootNodeError(branch_id)).await;
                Err(error)
            }
        }
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_child_nodes(&mut self, parent_hash: Hash) -> Result<()> {
        println!("server.handle_child_nodes");
        let mut conn = self.index.pool.acquire().await?;

        // At most one of these will be non-empty.
        let inner_nodes = InnerNode::load_children(&mut conn, &parent_hash).await?;
        let leaf_nodes = LeafNode::load_children(&mut conn, &parent_hash).await?;

        drop(conn);

        if !inner_nodes.is_empty() || !leaf_nodes.is_empty() {
            if !inner_nodes.is_empty() {
                tracing::trace!("inner nodes found");
                self.tx.send(Response::InnerNodes(inner_nodes)).await;
            }

            if !leaf_nodes.is_empty() {
                tracing::trace!("leaf nodes found");
                self.tx.send(Response::LeafNodes(leaf_nodes)).await;
            }
        } else {
            tracing::warn!("child nodes not found");
            self.tx.send(Response::ChildNodesError(parent_hash)).await;
        }

        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_block(&mut self, id: BlockId) -> Result<()> {
        println!("server.handle_block");
        let mut content = vec![0; BLOCK_SIZE].into_boxed_slice();
        let mut conn = self.index.pool.acquire().await?;
        let result = block::read(&mut conn, &id, &mut content).await;
        drop(conn); // don't hold the connection while sending is in progress

        match result {
            Ok(nonce) => {
                tracing::trace!("block found");
                self.tx.send(Response::Block { content, nonce }).await;
                Ok(())
            }
            Err(Error::BlockNotFound(_)) => {
                tracing::warn!("block not found");
                self.tx.send(Response::BlockError(id)).await;
                Ok(())
            }
            Err(error) => {
                self.tx.send(Response::BlockError(id)).await;
                Err(error)
            }
        }
    }
}

/// Monitors the repository for changes and notifies the peer.
struct Monitor<'a> {
    index: &'a Index,
    tx: &'a Sender,
}

impl<'a> Monitor<'a> {
    fn new(index: &'a Index, tx: &'a Sender) -> Self {
        Self { index, tx }
    }

    async fn run(self) -> Result<()> {
        let mut subscription = self.index.subscribe();

        // send initial branches
        //self.handle_all_branches_changed().await?;

        loop {
            let r = subscription.recv().await;
            //println!(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> {} {:?}", line!(), r);
            //match r {
            //    Ok(Event {
            //        payload:
            //            Payload::BranchChanged(branch_id) | Payload::BlockReceived { branch_id, .. },
            //        ..
            //    }) => self.handle_branch_changed_(branch_id).await?,
            //    Ok(Event {
            //        payload: Payload::FileClosed,
            //        ..
            //    }) => continue,
            //    Err(RecvError::Lagged(_)) => {
            //        tracing::warn!("event receiver lagged");
            //        self.handle_all_branches_changed().await?
            //    }
            //    Err(RecvError::Closed) => break,
            //}
        }

        Ok(())
    }

    async fn handle_all_branches_changed(&self) -> Result<()> {
        let root_nodes = self.load_all_root_nodes().await?;
        for root_node in root_nodes {
            self.handle_root_node_changed(root_node).await?;
        }

        Ok(())
    }

    async fn handle_branch_changed_(&self, branch_id: PublicKey) -> Result<()> {
        Ok(())
    }

    async fn handle_branch_changed(&self, branch_id: PublicKey) -> Result<()> {
        let root_node = match self.load_root_node(branch_id).await {
            Ok(node) => node,
            Err(Error::EntryNotFound) => {
                // branch was removed after the notification was fired.
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        self.handle_root_node_changed(root_node).await
    }

    async fn handle_root_node_changed(&self, root_node: RootNode) -> Result<()> {
        if !root_node.summary.is_complete() {
            // send only complete branches
            return Ok(());
        }

        if root_node.proof.version_vector.is_empty() {
            // Do not send branches with empty version vectors because they have no content yet
            return Ok(());
        }

        tracing::trace!(
            branch_id = ?root_node.proof.writer_id,
            hash = ?root_node.proof.hash,
            vv = ?root_node.proof.version_vector,
            block_presence = ?root_node.summary.block_presence,
            "handle_branch_changed",
        );

        let response = Response::RootNode {
            proof: root_node.proof.into(),
            summary: root_node.summary,
        };

        self.tx.send(response).await;

        Ok(())
    }

    async fn load_all_root_nodes(&self) -> Result<Vec<RootNode>> {
        let mut conn = self.index.pool.acquire().await?;
        RootNode::load_all_latest_complete(&mut conn)
            .try_collect()
            .await
    }

    async fn load_root_node(&self, branch_id: PublicKey) -> Result<RootNode> {
        let mut conn = self.index.pool.acquire().await?;
        RootNode::load_latest_complete_by_writer(&mut conn, branch_id).await
    }
}

type Receiver = mpsc::Receiver<Request>;

struct Sender(mpsc::Sender<Content>);

impl Sender {
    async fn send(&self, response: Response) -> bool {
        self.0.send(Content::Response(response)).await.is_ok()
    }
}
