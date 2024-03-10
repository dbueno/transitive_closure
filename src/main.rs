use serde::{Deserialize, Serialize};
use std::{error::Error, io, process};

#[derive(Debug, Deserialize, Clone)]
struct Edge {
    src: String,
    dst: String,
}

fn example() -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_reader(io::stdin());
    let mut edges = Vec::new();
    for result in rdr.deserialize() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record: Edge = result?;
        edges.push(record.clone());
        println!("{:?}", record);
    }
    Ok(())
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}
