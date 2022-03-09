use criterion::{criterion_group, Criterion};
use rand::{thread_rng, Rng, RngCore};
use rocksdb_rs::{
    InlineSkipListMemtableRep, InternalKeyComparator, MemtableRep, SkipListMemtableRep,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

fn bench_skiplist<M: MemtableRep + 'static>(
    _c: &mut Criterion,
    memtable: Arc<M>,
    max_write_buffer_size: usize,
    max_key_count: u64,
) -> usize {
    let now = Instant::now();
    let mut pools = vec![];
    let global_sequence = Arc::new(AtomicU64::new(0));
    for _ in 0..4 {
        let sequence = global_sequence.clone();
        let m = memtable.clone();
        let handle = std::thread::spawn(move || {
            let mut rng = thread_rng();
            let mut v: [u8; 256] = [0u8; 256];
            rng.fill(&mut v);
            let mut splice = M::Splice::default();
            let mut data_size = 0;
            for _ in 0..10000 {
                let last_sequence = sequence.fetch_add(100, Ordering::SeqCst);
                if m.mem_size() + 1000 > max_write_buffer_size || last_sequence > max_key_count {
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
                    data_size += key.len() + 8 + i as usize + 20;
                    m.add(
                        &mut splice,
                        &key,
                        &v[..(i as usize + 20)],
                        last_sequence + i,
                    );
                    if m.mem_size() + 1000 > max_write_buffer_size {
                        break;
                    }
                }
            }
            data_size
        });
        pools.push(handle);
    }

    let mut data_size = 0;
    for h in pools {
        data_size += h.join().unwrap();
    }

    let cost = now.elapsed();
    println!(
        "write {} keys cost time: {:?}",
        global_sequence.load(Ordering::Acquire),
        cost
    );
    data_size
}

fn bench_memtable(c: &mut Criterion) {
    let comparator = InternalKeyComparator::default();
    let write_buffer_size: usize = 256 * 1024 * 1024;
    let max_key_count = 1 * 1024 * 1024;
    {
        let now = Instant::now();
        let memtable = Arc::new(SkipListMemtableRep::new(
            comparator.clone(),
            write_buffer_size,
        ));
        let data_size = bench_skiplist(c, memtable.clone(), write_buffer_size, max_key_count);
        let cost = now.elapsed();
        println!(
            "mem_size: {}, cost time: {:?}",
            data_size + memtable.mem_size(),
            cost
        );
    }
    {
        let now = Instant::now();
        let memtable = Arc::new(InlineSkipListMemtableRep::new(comparator));
        bench_skiplist(c, memtable.clone(), write_buffer_size, max_key_count);
        let cost = now.elapsed();
        println!("mem_size: {} cost time: {:?}", memtable.mem_size(), cost);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_memtable
}
