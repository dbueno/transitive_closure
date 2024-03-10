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

    pub fn clear(&mut self) {
        self.tuples.clear();
        self.src_index.clear();
    }

    pub fn len(&self) -> usize {
        return self.tuples.len();
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
    let mut delta = Relation::new();

    // Base case: Copies source relation to closure.
    for record in &db.edges.tuples {
        closure.insert_edge(record.src.clone(), record.dst.clone());
        delta.insert_edge(record.src.clone(), record.dst.clone());
    }

    // Transitive case: If closure has a->b and b->c is in db, adds a->c to delta.
    // delta is a queue of items to process, initially db. delta is a subset of closure. This loop
    // tries to produce tuples from all elements of delta; then it clears delta. If any of those
    // tuples are not already in closure, then they go back into delta for the next go-round.
    loop {
        // tmp stores transitive closure tuples derived from delta.
        let mut tmp = Relation::new();
        dbg!(delta.len());
        for a_b in &delta.tuples {
            for b_c_index in closure.src_index.get(&a_b.dst).unwrap_or(&Vec::new()) {
                let b_c = &closure.tuples[*b_c_index];
                tmp.insert_edge(a_b.src.clone(), b_c.dst.clone());
            }
        }
        // Clears delta since we're done with it.
        delta.clear();

        dbg!(&tmp);
        // Initialize to false so OR works.
        let mut changed = false;
        // Produces new elements of delta if they are tuples not already in closure.
        for a_b in &tmp.tuples {
            let is_new = closure.insert_edge(a_b.src.clone(), a_b.dst.clone());
            if is_new {
                delta.insert_edge(a_b.src.clone(), a_b.dst.clone());
            }
            changed |= is_new;
        }
        if !changed {
            break;
        }
    }
    return closure;
}
