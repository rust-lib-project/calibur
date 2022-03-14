mod bench_memtable;

use criterion::criterion_main;

criterion_main!(bench_memtable::benches);
