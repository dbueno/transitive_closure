use serde::Serialize;
use std::collections::HashMap;
use std::collections::BTreeMap;

// Another way to do the interner is Arc<String>

type InternId = usize;

#[derive(Debug, Clone)]
struct StringInterner {
    strings: HashMap<&'static str, InternId>,
    indices: Vec<&'static str>,
}

impl StringInterner {
    fn new() -> Self {
        StringInterner {
            strings: HashMap::new(),
            indices: Vec::new(),
        }
    }

    /// Leaks the string into the interner and returns the id
    fn intern(&mut self, s: &str) -> InternId {
        if let Some(&index) = self.strings.get(s) {
            return index;
        }

        let index = self.indices.len();
        let ref_s = s.to_string().leak();
        self.indices.push(ref_s);
        self.strings.insert(ref_s, index);
        index
    }

    fn get_string(&self, index: InternId) -> Option<&'static str> {
        self.indices.get(index).map(|s| *s)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EdgeRecord {
    src: InternId,
    dst: InternId,
}

#[derive(Debug)]
struct EdgeRelation {
    tuples: Vec<EdgeRecord>,
    // index from 'src' field into tuples
    src_index: HashMap<InternId, Vec<usize>>,
}

impl EdgeRelation {
    pub fn new() -> Self {
        EdgeRelation {
            tuples: Vec::new(),
            src_index: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.tuples.clear();
        self.src_index.clear();
    }

    pub fn len(&self) -> usize {
        self.tuples.len()
    }

    /// Inserts an edge
    ///
    /// Returns true if the edge was not already in the relation; false otherwise
    pub fn insert_edge(&mut self, src: InternId, dst: InternId) -> bool {
        // or_insert_with returns a mutable ref to the value in the btree entry.
        let index_entries = self.src_index.entry(src).or_insert_with(Vec::new);
        // Vec.iter() is immutable iteration
        for &i in index_entries.iter() {
            if self.tuples[i].dst == dst {
                return false;
            }
        }

        // `index_entries` can now be used again because it hasn't been moved
        let index = self.tuples.len();
        self.tuples.push(EdgeRecord { src, dst });
        index_entries.push(index); // Mutation after the loop is fine
        true
    }
}

#[derive(Debug)]
pub struct Database {
    interner: StringInterner,
    edges: EdgeRelation,
}

impl Database {
    pub fn new() -> Self {
        Database {
            interner: StringInterner::new(),
            edges: EdgeRelation::new(),
        }
    }

    fn from_relation(&self, rel: EdgeRelation) -> Self {
        Database {
            interner: self.interner.clone(),
            edges: rel,
        }
    }

    pub fn insert_edge(&mut self, src: String, dst: String) -> bool {
        let src = self.interner.intern(&src);
        let dst = self.interner.intern(&dst);
        self.edges.insert_edge(src, dst)
    }

    pub fn get_string(&self, index: InternId) -> Option<&str> {
        self.interner.get_string(index)
    }

    pub fn get_element<'a>(&self, index: usize) -> Option<&str> {
        let records = &self.edges.tuples;
        if index < records.len() {
            self.get_string(records[index].src)
        } else {
            None
        }
    }
}

pub struct DatabaseIterator<'a> {
    db: &'a Database,
    index: usize,
}

#[derive(Debug, Serialize)]
pub struct RecordView<'a> {
    pub src: &'a str,
    pub dst: &'a str,
}

impl<'a> Iterator for DatabaseIterator<'a> {
    type Item = RecordView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.db.edges.tuples.len() {
            let edge_record = &self.db.edges.tuples[self.index];
            self.index += 1;
            Some(RecordView {
                src: self.db.get_string(edge_record.src).unwrap(),
                dst: self.db.get_string(edge_record.dst).unwrap(),
            })
        } else {
            None
        }
    }
}

impl Database {
    pub fn iter<'a>(&'a self) -> DatabaseIterator<'a> {
        DatabaseIterator { db: self, index: 0 }
    }
}

pub fn closure_tuple(db: &mut Database) -> Database {
    let mut closure = EdgeRelation::new();
    let mut delta_vec = Vec::new();

    // Base case: Copies source EdgeRelation to closure.
    for edge_record in &db.edges.tuples {
        closure.insert_edge(edge_record.src, edge_record.dst);
        delta_vec.push(edge_record.clone());
    }

    // Worklist algorithm. Worklist of edges. Tries to extend each edge a_b with b_c, and inserts
    // it into closure. If a_c is new, puts it into the worklist.
    while let Some (a_b) = delta_vec.pop() {
        let (a, b) = (a_b.src, a_b.dst);
        // Find tuples b_c, then add a_c
        for index in closure.src_index.get(&b).unwrap_or(&Vec::new()).clone() {
            let b_c = &closure.tuples[index];
            let c = b_c.dst;
            let is_new = closure.insert_edge(a, c);
            if is_new {
                delta_vec.push(EdgeRecord { src: a, dst: c});
            }
        }
    }
    dbg!(closure.len());
    db.from_relation(closure)
}

pub fn closure(db: &mut Database) -> Database {
    let mut closure = EdgeRelation::new();
    let mut delta = EdgeRelation::new();
    let mut tmp = EdgeRelation::new();

    // Base case: Copies source EdgeRelation to closure.
    for edge_record in &db.edges.tuples {
        closure.insert_edge(edge_record.src, edge_record.dst);
        delta.insert_edge(edge_record.src, edge_record.dst);
    }

    // Transitive case: If closure has a->b and b->c is in db, adds a->c to delta.
    // delta is a queue of items to process, initially db. delta is a subset of closure. This loop
    // tries to produce tuples from all elements of delta; then it clears delta. If any of those
    // tuples are not already in closure, then they go back into delta for the next go-round.
    loop {
        // tmp stores transitive closure tuples derived from delta.
        tmp.clear();
        dbg!(delta.len());
        // XXX sort tuples?
        for a_b in &delta.tuples {
            let (a, b) = (a_b.src, a_b.dst);
            // Find tuples b_c, then add a_c
            for &index in closure.src_index.get(&b).unwrap_or(&Vec::new()) {
                let b_c = &closure.tuples[index];
                let c = b_c.dst;
                tmp.insert_edge(a, c);
            }
        }
        // Delta stores tuples new to closure
        delta.clear();

        // Initialize to false so OR works.
        let mut changed = false;
        // Produces new elements of delta if they are tuples not already in closure.
        for a_b in tmp.tuples.iter() {
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
    dbg!(closure.len());
    db.from_relation(closure)
}
