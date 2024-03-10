use serde::Deserialize;
use std::{error::Error, io};

#[derive(Debug, Deserialize, Clone)]
struct Edge {
    src: String,
    dst: String,
}

fn example() -> Result<(), Box<dyn Error>> {
    let mut db = transitive_closure::Database::new();

    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_reader(io::stdin());
    for result in rdr.deserialize() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record: Edge = result?;
        db.insert_edge(record.src, record.dst);
        println!("Database: {:#?}", &db)
    }
    Ok(())
}

fn main() {
    match example() {
        Ok(_)=> {}
        Err(err) => {
            println!("Error: {}", err);
        }
    }
}
