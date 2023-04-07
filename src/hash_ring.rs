use std::collections::{BinaryHeap, HashMap};
use std::fmt::{Display, Formatter};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use twox_hash::XxHash64;

/// As a convenience, rust-hash-ring provides a default struct to hold node
/// information. It is optional and you can define your own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeInfo {
    pub host: String,
    pub port: u16,
}

impl Display for NodeInfo {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{}:{}", self.host, self.port)
    }
}

impl From<&NodeInfo> for String {
    fn from(value: &NodeInfo) -> Self {
        format!("{}:{}", value.host, value.port)
    }
}

type XxHash64Hasher = BuildHasherDefault<XxHash64>;

/// HashRing
pub struct HashRing<T, S = XxHash64Hasher> {
    replicas: usize,
    ring: HashMap<u64, T>,
    sorted_keys: Vec<u64>,
    hash_builder: S,
    real_nodes: Vec<T>,
}

impl<T: ToString + Clone + PartialEq> HashRing<T, XxHash64Hasher> {
    /// Creates a new hash ring with the specified nodes.
    /// Replicas is the number of virtual nodes each node has to make a better distribution.
    pub fn new(nodes: Vec<T>, replicas: usize) -> HashRing<T, XxHash64Hasher> {
        HashRing::with_hasher(nodes, replicas, XxHash64Hasher::default())
    }
}

impl<T, S> HashRing<T, S>
where
    T: ToString + Clone + PartialEq,
    S: BuildHasher,
{
    pub fn with_hasher(nodes: Vec<T>, replicas: usize, hash_builder: S) -> HashRing<T, S> {
        let mut new_hash_ring: HashRing<T, S> = HashRing {
            replicas,
            ring: HashMap::new(),
            sorted_keys: Vec::new(),
            hash_builder,
            real_nodes: Vec::new(),
        };

        for n in &nodes {
            new_hash_ring.add_node(n);
        }
        new_hash_ring
    }

    /// Adds a node to the hash ring
    pub fn add_node(&mut self, node: &T) {
        for i in 0..self.replicas {
            let key = self.gen_key(format!("{}:{}", node.to_string(), i));
            self.ring.insert(key, (*node).clone());
            self.sorted_keys.push(key);
        }

        self.sorted_keys = BinaryHeap::from(self.sorted_keys.clone()).into_sorted_vec();
        self.real_nodes.push(node.clone());
    }

    /// Deletes a node from the hash ring
    pub fn remove_node(&mut self, node: &T) {
        for i in 0..self.replicas {
            let key = self.gen_key(format!("{}:{}", node.to_string(), i));
            if !self.ring.contains_key(&key) {
                return;
            }
            self.ring.remove(&key);
            let mut index = 0;
            for j in 0..self.sorted_keys.len() {
                if self.sorted_keys[j] == key {
                    index = j;
                    break;
                }
            }
            self.sorted_keys.remove(index);
        }
        if let Some(i) = self.real_nodes.iter().position(|x| x == node) {
            self.real_nodes.remove(i);
        }
    }

    /// Gets the node a specific key belongs to
    pub fn get_node(&self, key: String) -> Option<&T> {
        if self.sorted_keys.is_empty() {
            return None;
        }

        let generated_key = self.gen_key(key);
        let nodes = self.sorted_keys.clone();

        for node in &nodes {
            if generated_key <= *node {
                return Some(self.ring.get(node).unwrap());
            }
        }

        let node = &nodes[0];
        return Some(self.ring.get(node).unwrap());
    }

    pub fn pre_node(&self, node: &T) -> Option<&T> {
        if self.real_nodes.is_empty() {
            return None;
        }

        for (index, cur_node) in self.real_nodes.iter().enumerate() {
            if node == cur_node {
                if index == 0 {
                    return Some(self.real_nodes.last().unwrap());
                }
                return Some(self.real_nodes.get(index - 1).unwrap());
            }
        }
        None
    }

    pub fn next_node(&self, node: &T) -> Option<&T> {
        if self.real_nodes.is_empty() {
            return None;
        }

        for (index, cur_node) in self.real_nodes.iter().enumerate() {
            if node == cur_node {
                if index == self.real_nodes.len() - 1 {
                    return Some(self.real_nodes.first().unwrap());
                }
                return Some(self.real_nodes.get(index + 1).unwrap());
            }
        }
        None
    }

    /// Generates a key from a string value
    fn gen_key(&self, key: String) -> u64 {
        let mut hasher = self.hash_builder.build_hasher();
        hasher.write(key.as_bytes());
        hasher.finish()
    }
}

#[cfg(test)]
mod test {
    use crate::hash_ring::{HashRing, NodeInfo};
    use std::hash::BuildHasherDefault;
    use std::hash::Hasher;

    // Defines a NodeInfo for a localhost address with a given port.
    fn node(port: u16) -> NodeInfo {
        NodeInfo {
            host: "localhost".to_owned(),
            port,
        }
    }

    #[test]
    fn test_empty_ring() {
        let hash_ring: HashRing<NodeInfo> = HashRing::new(vec![], 10);
        assert_eq!(None, hash_ring.get_node("hello".to_string()));
    }

    #[test]
    fn test_default_nodes() {
        let nodes = vec![
            node(15324),
            node(15325),
            node(15326),
            node(15327),
            node(15328),
            node(15329),
        ];

        let mut hash_ring: HashRing<NodeInfo> = HashRing::new(nodes, 10);

        assert_eq!(Some(&node(15324)), hash_ring.get_node("two".to_string()));
        assert_eq!(Some(&node(15325)), hash_ring.get_node("seven".to_string()));
        assert_eq!(Some(&node(15326)), hash_ring.get_node("hello".to_string()));
        assert_eq!(Some(&node(15327)), hash_ring.get_node("dude".to_string()));
        assert_eq!(
            Some(&node(15328)),
            hash_ring.get_node("fourteen".to_string())
        );
        assert_eq!(Some(&node(15329)), hash_ring.get_node("five".to_string()));

        hash_ring.remove_node(&node(15329));
        assert_eq!(Some(&node(15326)), hash_ring.get_node("hello".to_string()));

        hash_ring.add_node(&node(15329));
        assert_eq!(Some(&node(15326)), hash_ring.get_node("hello".to_string()));
    }

    #[derive(Clone, PartialEq)]
    struct CustomNodeInfo {
        pub host: &'static str,
        pub port: u16,
    }

    impl ToString for CustomNodeInfo {
        fn to_string(&self) -> String {
            format!("{}:{}", self.host, self.port)
        }
    }

    #[test]
    fn test_custom_nodes() {
        let nodes = vec![
            CustomNodeInfo {
                host: "localhost",
                port: 15324,
            },
            CustomNodeInfo {
                host: "localhost",
                port: 15325,
            },
            CustomNodeInfo {
                host: "localhost",
                port: 15326,
            },
            CustomNodeInfo {
                host: "localhost",
                port: 15327,
            },
            CustomNodeInfo {
                host: "localhost",
                port: 15328,
            },
            CustomNodeInfo {
                host: "localhost",
                port: 15329,
            },
        ];

        let mut hash_ring: HashRing<CustomNodeInfo> = HashRing::new(nodes, 10);

        assert_eq!(
            Some("localhost:15326".to_string()),
            hash_ring
                .get_node("hello".to_string())
                .map(|x| x.to_string(),)
        );
        assert_eq!(
            Some("localhost:15327".to_string()),
            hash_ring
                .get_node("dude".to_string())
                .map(|x| x.to_string(),)
        );

        hash_ring.remove_node(&CustomNodeInfo {
            host: "localhost",
            port: 15329,
        });
        assert_eq!(
            Some("localhost:15326".to_string()),
            hash_ring
                .get_node("hello".to_string())
                .map(|x| x.to_string(),)
        );

        hash_ring.add_node(&CustomNodeInfo {
            host: "localhost",
            port: 15329,
        });
        assert_eq!(
            Some("localhost:15326".to_string()),
            hash_ring
                .get_node("hello".to_string())
                .map(|x| x.to_string(),)
        );
    }

    #[test]
    fn test_remove_actual_node() {
        let nodes = vec![
            node(15324),
            node(15325),
            node(15326),
            node(15327),
            node(15328),
            node(15329),
        ];

        let mut hash_ring: HashRing<NodeInfo> = HashRing::new(nodes, 10);

        // This should be num nodes * num replicas
        assert_eq!(60, hash_ring.sorted_keys.len());
        assert_eq!(60, hash_ring.ring.len());

        hash_ring.remove_node(&node(15326));

        // This should be num nodes * num replicas
        assert_eq!(50, hash_ring.sorted_keys.len());
        assert_eq!(50, hash_ring.ring.len());
    }

    #[test]
    fn test_remove_non_existent_node() {
        let nodes = vec![
            node(15324),
            node(15325),
            node(15326),
            node(15327),
            node(15328),
            node(15329),
        ];

        let mut hash_ring: HashRing<NodeInfo> = HashRing::new(nodes, 10);

        hash_ring.remove_node(&node(15330));

        // This should be num nodes * num replicas
        assert_eq!(60, hash_ring.sorted_keys.len());
        assert_eq!(60, hash_ring.ring.len());
    }

    #[test]
    fn test_custom_hasher() {
        #[derive(Default)]
        struct ConstantHasher;

        impl Hasher for ConstantHasher {
            fn write(&mut self, _bytes: &[u8]) {
                // Do nothing
            }

            fn finish(&self) -> u64 {
                1
            }
        }

        type ConstantBuildHasher = BuildHasherDefault<ConstantHasher>;

        let nodes = vec![
            node(15324),
            node(15325),
            node(15326),
            node(15327),
            node(15328),
            node(15329),
        ];

        let hash_ring: HashRing<NodeInfo, ConstantBuildHasher> =
            HashRing::with_hasher(nodes, 10, ConstantBuildHasher::default());

        assert_eq!(Some(&node(15329)), hash_ring.get_node("hello".to_string()));
        assert_eq!(Some(&node(15329)), hash_ring.get_node("dude".to_string()));
        assert_eq!(Some(&node(15329)), hash_ring.get_node("two".to_string()));
    }

    #[test]
    fn test_pre_next_node() {
        let nodes = vec![
            node(15324),
            node(15325),
            node(15326),
            node(15327),
            node(15328),
            node(15329),
        ];

        let hash_ring: HashRing<NodeInfo> = HashRing::new(nodes, 10);

        println!("{}", hash_ring.next_node(&node(15325)).unwrap());
        println!("{}", hash_ring.pre_node(&node(15329)).unwrap());

        println!("{}", hash_ring.pre_node(&node(15324)).unwrap());
        println!("{}", hash_ring.next_node(&node(15329)).unwrap());

        // assert_eq!(Some(&node(15324)), hash_ring.get_node("two".to_string()));
    }
}
