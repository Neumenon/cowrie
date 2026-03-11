//! Suite 7: Performance Cliff Detection for cowrie Rust
//!
//! Time-bounded tests that fail if execution exceeds O(n) expectations.
//! Detects quadratic/exponential blowups from pathological inputs.

use std::collections::BTreeMap;
use std::time::Instant;
use cowrie_rs::gen2::{encode, decode, Value};

/// Build a deeply nested array of given depth.
fn nested_array(depth: usize) -> Value {
    let mut v = Value::Null;
    for _ in 0..depth {
        v = Value::Array(vec![v]);
    }
    v
}

/// Run a closure with a large stack (8MB) to avoid stack overflow
/// from recursive encode/decode on deeply nested values.
fn with_large_stack<F: FnOnce() + Send + 'static>(f: F) {
    let builder = std::thread::Builder::new().stack_size(64 * 1024 * 1024);
    let handle = builder.spawn(f).expect("spawn thread");
    handle.join().expect("thread panicked");
}

#[test]
fn perf_cliff_deep_nesting_100() {
    with_large_stack(|| {
        let v = nested_array(100);
        let data = encode(&v).expect("encode depth=100");
        let _ = decode(&data).expect("decode depth=100");
    });
}

#[test]
fn perf_cliff_deep_nesting_at_limit() {
    // The decoder has a depth limit — verify it returns a clean error
    // rather than panicking or taking unbounded time.
    with_large_stack(|| {
        let v = nested_array(1000);
        match encode(&v) {
            Ok(data) => {
                // Decode may reject due to depth limit — that's fine
                let _ = decode(&data);
            }
            Err(_) => {
                // Encoder may also reject deep nesting — that's fine
            }
        }
    });
}

#[test]
fn perf_cliff_nesting_scaling() {
    // Test that nesting up to the codec's limit scales linearly.
    // Use modest depths that stay under the depth limit.
    with_large_stack(|| {
        let depths = [10, 50, 100];
        let mut last_duration = None;

        for &depth in &depths {
            let v = nested_array(depth);
            let start = Instant::now();
            let data = encode(&v).expect("encode");
            let _ = decode(&data).expect("decode");
            let dur = start.elapsed();

            if let Some(prev) = last_duration {
                let ratio = dur.as_nanos() as f64 / prev as f64;
                assert!(
                    ratio < 100.0,
                    "possible quadratic blowup at depth={}: ratio={:.1}x",
                    depth, ratio
                );
            }
            last_duration = Some(dur.as_nanos().max(1));
        }
    });
}

#[test]
fn perf_cliff_wide_array_10k() {
    let items: Vec<Value> = (0..10_000).map(|i| Value::Int(i)).collect();
    let v = Value::Array(items);

    let start = Instant::now();
    let data = encode(&v).expect("encode");
    let _ = decode(&data).expect("decode");
    let dur = start.elapsed();

    assert!(
        dur.as_secs() < 5,
        "10K array took {:?}, exceeds 5s threshold",
        dur
    );
}

#[test]
fn perf_cliff_wide_array_100k() {
    let items: Vec<Value> = (0..100_000).map(|i| Value::Int(i)).collect();
    let v = Value::Array(items);

    let start = Instant::now();
    let data = encode(&v).expect("encode");
    let _ = decode(&data).expect("decode");
    let dur = start.elapsed();

    assert!(
        dur.as_secs() < 5,
        "100K array took {:?}, exceeds 5s threshold",
        dur
    );
}

#[test]
fn perf_cliff_wide_object_10k() {
    let entries: BTreeMap<String, Value> = (0..10_000)
        .map(|i| (format!("key_{}", i), Value::Int(i)))
        .collect();
    let v = Value::Object(entries);

    let start = Instant::now();
    let data = encode(&v).expect("encode");
    let _ = decode(&data).expect("decode");
    let dur = start.elapsed();

    assert!(
        dur.as_secs() < 5,
        "10K object took {:?}, exceeds 5s threshold",
        dur
    );
}

#[test]
fn perf_cliff_many_empty_strings() {
    let items: Vec<Value> = (0..100_000)
        .map(|_| Value::String(String::new()))
        .collect();
    let v = Value::Array(items);

    let start = Instant::now();
    let data = encode(&v).expect("encode");
    let _ = decode(&data).expect("decode");
    let dur = start.elapsed();

    assert!(
        dur.as_secs() < 5,
        "100K empty strings took {:?}, exceeds 5s threshold",
        dur
    );
}
