#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Gen2 decode: must never panic on arbitrary input
    let _ = cowrie_rs::gen2::decode(data);

    // Gen2 framed decode: must never panic
    let _ = cowrie_rs::gen2::decode_framed(data);

    // Gen1 decode: must never panic
    let _ = cowrie_rs::gen1::decode(data);
});
