//! Decode cowrie binary from stdin, emit JSON to stdout.
//! Used by the differential testing runner.

use std::io::Read;
use cowrie_rs::gen2::{decode, to_json};

fn main() {
    let mut data = Vec::new();
    std::io::stdin()
        .read_to_end(&mut data)
        .expect("failed to read stdin");

    match decode(&data) {
        Ok(value) => {
            let json = to_json(&value).expect("failed to convert to JSON");
            println!("{}", json);
        }
        Err(e) => {
            eprintln!("decode error: {}", e);
            std::process::exit(1);
        }
    }
}
