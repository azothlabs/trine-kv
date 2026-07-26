#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| trine_kv::fuzzing::decode_upload(bytes));
