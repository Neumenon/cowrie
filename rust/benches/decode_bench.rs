use std::fs;
use std::path::PathBuf;
use std::time::Instant;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("benchmarks")
        .join("fixtures")
        .join(name)
}

fn bench(label: &str, data: &[u8], decode_fn: fn(&[u8]) -> Result<(), String>, iterations: usize) {
    // warmup
    for _ in 0..iterations.min(100) {
        let _ = decode_fn(data);
    }

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = decode_fn(data);
    }
    let elapsed = start.elapsed().as_secs_f64();

    let ops = iterations as f64 / elapsed;
    let us = (elapsed / iterations as f64) * 1e6;
    let mbps = (data.len() as f64 * iterations as f64 / elapsed) / 1e6;

    println!(
        "{:<10} {:>7}B {:>10.0} {:>10.1} {:>10.1}",
        label,
        data.len(),
        ops,
        us,
        mbps
    );
}

fn decode_gen1(data: &[u8]) -> Result<(), String> {
    cowrie_rs::gen1::decode(data).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn decode_gen2(data: &[u8]) -> Result<(), String> {
    cowrie_rs::gen2::decode::decode(data).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn main() {
    println!("{}", "=".repeat(72));
    println!("Cowrie Decode Benchmark — Rust");
    println!("{}", "=".repeat(72));
    println!(
        "{:<10} {:>8} {:>10} {:>10} {:>10}",
        "Payload", "Size", "ops/s", "us/op", "MB/s"
    );
    println!("{}", "-".repeat(72));

    let names = ["small", "medium", "large", "floats"];

    for name in &names {
        let g1 = fs::read(fixture_path(&format!("{}.gen1", name))).unwrap();
        let g2 = fs::read(fixture_path(&format!("{}.gen2", name))).unwrap();

        let iters = if g1.len() < 1000 { 500_000 } else { 10_000 };

        bench(&format!("{}/g1", name), &g1, decode_gen1, iters);
        bench(&format!("{}/g2", name), &g2, decode_gen2, iters);

        println!();
    }
}
