use criterion::{criterion_group, Criterion};
use rand::{thread_rng, Rng, RngCore};
use rocksdb_rs::{
    InlineSkipListMemtableRep, InternalKeyComparator, MemtableRep, SkipListMemtableRep,
};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::time::Instant;

fn bench_skiplist<M: MemtableRep + 'static>(
    _c: &mut Criterion,
    memtable: Arc<M>,
    max_write_buffer_size: usize,
    max_key_count: u64,
) {
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
            for _ in 0..10000 {
                let last_sequence = sequence.fetch_add(100, std::sync::atomic::Ordering::SeqCst);
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
        });
        pools.push(handle);
    }

    for h in pools {
        h.join().unwrap();
    }

    let cost = now.elapsed();
    println!(
        "memtable size: {}, write {} keys cost time: {:?}",
        memtable.mem_size(),
        global_sequence.load(std::sync::atomic::Ordering::Acquire),
        cost
    );
}

fn bench_memtable(c: &mut Criterion) {
    let comparator = InternalKeyComparator::default();
    let write_buffer_size: usize = 256 * 1024 * 1024;
    let max_key_count = 1 * 1024 * 1024;
    {
        let now = Instant::now();
        let memtable = SkipListMemtableRep::new(comparator.clone(), write_buffer_size);
        bench_skiplist(c, Arc::new(memtable), write_buffer_size, max_key_count);
        let cost = now.elapsed();
        println!("cost time: {:?}", cost);
    }
    {
        let now = Instant::now();
        let memtable = InlineSkipListMemtableRep::new(comparator);
        bench_skiplist(c, Arc::new(memtable), write_buffer_size, max_key_count);
        let cost = now.elapsed();
        println!("cost time: {:?}", cost);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_memtable
}
