use criterion::{criterion_group, Criterion};
use rand::{thread_rng, Rng, RngCore};
use rocksdb_rs::{
    InlineSkipListMemtableRep, InternalKeyComparator, MemtableRep, SkipListMemtableRep,
};
use tokio::time::Instant;

fn bench_skiplist<M: MemtableRep>(_c: &mut Criterion, memtable: &M, max_write_buffer_size: usize) {
    let now = Instant::now();
    let mut count: u64 = 0;
    let mut rng = thread_rng();
    let mut v: [u8; 256] = [0u8; 256];
    rng.fill(&mut v);
    for _ in 0..10000 {
        if memtable.mem_size() + 1000 > max_write_buffer_size || count > 600000 {
            break;
        }
        let k = rng.next_u64() % 100000;
        let gap = rng.next_u32() % 10000;
        let mut key = b"t_00000001_".to_vec();
        let l = key.len();
        for i in 0..100 {
            let j = k + i * gap as u64;
            key.resize(l, 0);
            key.extend_from_slice(&j.to_le_bytes());
            memtable.add(&key, &v[..(i as usize + 100)], count);
            count += 1;
            if memtable.mem_size() + 1000 > max_write_buffer_size {
                break;
            }
        }
    }
    let cost = now.elapsed();
    println!("write {} keys cost time: {:?}", count, cost);
}

fn bench_memtable(c: &mut Criterion) {
    let comparator = InternalKeyComparator::default();
    let write_buffer_size: usize = 128 * 1024 * 1024;
    {
        let memtable = SkipListMemtableRep::new(comparator.clone(), write_buffer_size);
        bench_skiplist(c, &memtable, write_buffer_size);
    }
    {
        let memtable = InlineSkipListMemtableRep::new(comparator);
        bench_skiplist(c, &memtable, write_buffer_size);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_memtable
}
