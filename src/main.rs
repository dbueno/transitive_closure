use serde::Deserialize;
use std::{error::Error, io};

#[derive(Debug, Deserialize, Clone)]
struct Edge {
    src: String,
    dst: String,
}

fn example() -> Result<transitive_closure::Database, Box<dyn Error>> {
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
        let _changed = db.insert_edge(record.src, record.dst);
        //println!("Database: {} {:#?}", changed, &db)
    }
    let closure = transitive_closure::closure(&mut db);
    Ok(closure)
}

fn main1() {
    match example() {
        Ok(closure) => {
            dbg!(&closure);
            let mut wtr = csv::WriterBuilder::new()
                .delimiter(b'\t')
                .has_headers(false)
                .from_writer(io::stdout());
            for record in closure.iter() {
                let _ = wtr.serialize(record);
            }
        }
        Err(err) => {
            println!("Error: {}", err);
        }
    }
}

use std::env;

fn main() {
    // Get command-line arguments
    let args: Vec<String> = env::args().collect();

    // Check if at least one argument is provided
    if args.len() < 2 {
        println!("Usage: {} <integer>", args[0]);
        return;
    }

    // Attempt to parse the first argument into an i32
    let input = match args[1].parse::<i32>() {
        Ok(num) => num,
        Err(_) => {
            println!("Error: Invalid integer provided");
            return;
        }
    };

    // Print the parsed integer
    println!("Parsed integer: {}", input);

    let mut db = transitive_closure::Database::new();
    for i in 1..input {
        db.insert_edge(i.to_string(), (i+1).to_string());
    }
    let _closure = transitive_closure::closure(&mut db);
}

