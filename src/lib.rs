use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    src: String,
    dst: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Relation {
    tuples: Vec<Record>,
    // index from 'src' field into tuples
    src_index: BTreeMap<String, Vec<usize>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    edges: Relation,
}

impl Database {
    pub fn new() -> Database {
        return Database {
            edges: Relation {
                tuples: Vec::new(),
                src_index: BTreeMap::new(),
            },
        };
    }

    pub fn insert_edge(&mut self, src: String, dst: String) {
        let index_entries = self
            .edges
            .src_index
            .entry(src.clone())
            .or_insert(Vec::new());
        // Bails if the src,dst entry already exists
        for i in &(*index_entries) {
            if self.edges.tuples[*i].dst == dst {
                return;
            }
        }
        self.edges.tuples.push(Record { src, dst });
        (*index_entries).push(self.edges.tuples.len() - 1);
    }
}
