use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    src: String,
    dst: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Relation {
    pub tuples: Vec<Record>,
    // index from 'src' field into tuples
    src_index: BTreeMap<String, Vec<usize>>,
}

impl Relation {
    pub fn new() -> Relation {
        return Relation {
            tuples: Vec::new(),
            src_index: BTreeMap::new(),
        };
    }

    pub fn insert_edge(&mut self, src: String, dst: String) -> bool {
        let index_entries = self.src_index.entry(src.clone()).or_insert(Vec::new());
        // Bails if the src,dst entry already exists
        for i in &(*index_entries) {
            if self.tuples[*i].dst == dst {
                return false;
            }
        }
        self.tuples.push(Record { src, dst });
        (*index_entries).push(self.tuples.len() - 1);
        return true;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    edges: Relation,
}

impl Database {
    pub fn new() -> Database {
        return Database {
            edges: Relation::new(),
        };
    }

    pub fn insert_edge(&mut self, src: String, dst: String) -> bool {
        return self.edges.insert_edge(src, dst);
    }
}

pub fn closure(db: &mut Database) -> Relation {
    let mut closure = Relation::new();

    // Base case: Copies source relation to closure.
    for record in &db.edges.tuples {
        closure.insert_edge(record.src.clone(), record.dst.clone());
    }
    dbg!(&closure);

    // Transitive case: If closure has a->b and b->c is in db, adds a->c to delta.
    // XXX iterate only over delta-closure values, not entire closure
    let mut changed = true;
    while changed {
        let mut delta = Relation::new();
        for a_b in &closure.tuples {
            for b_c_index in db.edges.src_index.get(&a_b.dst).unwrap_or(&Vec::new()) {
                let b_c = &db.edges.tuples[*b_c_index];
                delta.insert_edge(a_b.src.clone(), b_c.dst.clone());
            }
        }
        dbg!(&delta);
        // Initialize to false so OR works.
        changed = false;
        for a_b in &delta.tuples {
            changed |= closure.insert_edge(a_b.src.clone(), a_b.dst.clone());
        }
    }
    return closure;
}
