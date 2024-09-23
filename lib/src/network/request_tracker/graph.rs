use super::MessageKey;
use crate::{
    collections::{HashMap, HashSet},
    network::message::Request,
    protocol::MultiBlockPresence,
};
use slab::Slab;
use std::collections::hash_map::Entry;

/// DAG for storing data for the request tracker.
pub(super) struct Graph<T> {
    index: HashMap<(MessageKey, MultiBlockPresence), Key>,
    nodes: Slab<Node<T>>,
}

impl<T> Graph<T> {
    pub fn new() -> Self {
        Self {
            index: HashMap::default(),
            nodes: Slab::new(),
        }
    }

    pub fn get_or_insert(
        &mut self,
        request: Request,
        block_presence: MultiBlockPresence,
        parent_key: Option<Key>,
        value: T,
    ) -> Key {
        let entry = match self
            .index
            .entry((MessageKey::from(&request), block_presence))
        {
            Entry::Occupied(entry) => return *entry.get(),
            Entry::Vacant(entry) => entry,
        };

        let node_key = self.nodes.insert(Node {
            request,
            block_presence,
            parents: parent_key.into_iter().collect(),
            children: HashSet::default(),
            value,
        });
        let node_key = Key(node_key);

        entry.insert(node_key);

        if let Some(parent_key) = parent_key {
            if let Some(parent_node) = self.nodes.get_mut(parent_key.0) {
                parent_node.children.insert(node_key);
            }
        }

        node_key
    }

    pub fn get(&self, key: Key) -> Option<&Node<T>> {
        self.nodes.get(key.0)
    }

    pub fn get_mut(&mut self, key: Key) -> Option<&mut Node<T>> {
        self.nodes.get_mut(key.0)
    }

    pub fn remove(&mut self, key: Key) -> Option<Node<T>> {
        let node = self.nodes.try_remove(key.0)?;

        self.index
            .remove(&(MessageKey::from(&node.request), node.block_presence));

        for parent_key in &node.parents {
            let Some(parent_node) = self.nodes.get_mut(parent_key.0) else {
                continue;
            };

            parent_node.children.remove(&key);
        }

        Some(node)
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub(super) struct Key(usize);

pub(super) struct Node<T> {
    request: Request,
    block_presence: MultiBlockPresence,
    parents: HashSet<Key>,
    children: HashSet<Key>,
    value: T,
}

impl<T> Node<T> {
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub fn request(&self) -> &Request {
        &self.request
    }

    pub fn request_and_value_mut(&mut self) -> (&Request, &mut T) {
        (&self.request, &mut self.value)
    }

    pub fn parents(&self) -> impl ExactSizeIterator<Item = Key> + '_ {
        self.parents.iter().copied()
    }

    pub fn children(&self) -> impl ExactSizeIterator<Item = Key> + '_ {
        self.children.iter().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{debug_payload::DebugRequest, message::ResponseDisambiguator};
    use rand::Rng;

    #[test]
    fn child_request() {
        let mut rng = rand::thread_rng();
        let mut graph = Graph::new();

        assert_eq!(graph.len(), 0);

        let request0 = Request::ChildNodes(
            rng.gen(),
            ResponseDisambiguator::new(MultiBlockPresence::Full),
            DebugRequest::start(),
        );

        let node_key0 = graph.get_or_insert(request0.clone(), MultiBlockPresence::Full, None, 1);

        assert_eq!(graph.len(), 1);

        let Some(node) = graph.get(node_key0) else {
            unreachable!()
        };

        assert_eq!(*node.value(), 1);
        assert_eq!(node.children().len(), 0);
        assert_eq!(node.request(), &request0);

        let request1 = Request::ChildNodes(
            rng.gen(),
            ResponseDisambiguator::new(MultiBlockPresence::Full),
            DebugRequest::start(),
        );

        let node_key1 = graph.get_or_insert(
            request1.clone(),
            MultiBlockPresence::Full,
            Some(node_key0),
            2,
        );

        assert_eq!(graph.len(), 2);

        let Some(node) = graph.get(node_key1) else {
            unreachable!()
        };

        assert_eq!(*node.value(), 2);
        assert_eq!(node.children().len(), 0);
        assert_eq!(node.request(), &request1);

        assert_eq!(
            graph.get(node_key0).unwrap().children().collect::<Vec<_>>(),
            [node_key1]
        );

        graph.remove(node_key1);

        assert_eq!(graph.get(node_key0).unwrap().children().len(), 0);
    }

    #[test]
    fn duplicate_request() {
        let mut rng = rand::thread_rng();
        let mut graph = Graph::new();

        assert_eq!(graph.len(), 0);

        let request = Request::ChildNodes(
            rng.gen(),
            ResponseDisambiguator::new(MultiBlockPresence::Full),
            DebugRequest::start(),
        );

        let node_key0 = graph.get_or_insert(request.clone(), MultiBlockPresence::Full, None, 1);
        assert_eq!(graph.len(), 1);

        let node_key1 = graph.get_or_insert(request, MultiBlockPresence::Full, None, 1);
        assert_eq!(graph.len(), 1);
        assert_eq!(node_key0, node_key1);
    }
}
