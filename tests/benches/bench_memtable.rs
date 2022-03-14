use criterion::{criterion_group, Criterion};
use crossbeam_skiplist::SkipMap;
use rand::{thread_rng, Rng, RngCore};
use rocksdb_rs::{
    InlineSkipListMemtableRep, InternalKeyComparator, KeyComparator, MemTableContext, MemtableRep,
    SkipListMemtableRep,
};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub struct CrossbeamSkiplistRep {
    list: SkipMap<Vec<u8>, Vec<u8>>,
    comp: InternalKeyComparator,
}

impl MemtableRep for CrossbeamSkiplistRep {
    fn new_iterator(&self) -> Box<dyn rocksdb_rs::InternalIterator> {
        unimplemented!();
    }

    fn add(&self, _splice: &mut MemTableContext, key: &[u8], value: &[u8], sequence: u64) {
        let mut ikey = Vec::with_capacity(key.len() + 8);
        ikey.extend_from_slice(key);
        ikey.extend_from_slice(&sequence.to_le_bytes());
        self.list.insert(ikey, value.to_vec());
    }

    fn delete(&self, _splice: &mut MemTableContext, key: &[u8], sequence: u64) {
        let mut ikey = Vec::with_capacity(key.len() + 8);
        ikey.extend_from_slice(key);
        ikey.extend_from_slice(&sequence.to_le_bytes());
        self.list.remove(&ikey);
    }

    fn mem_size(&self) -> usize {
        self.list.len()
    }

    fn name(&self) -> &str {
        "CrossbeamSkiplist"
    }

    fn cmp(&self, start: &[u8], end: &[u8]) -> std::cmp::Ordering {
        self.comp.compare_key(start, end)
    }

    fn scan<F: FnMut(&[u8], &[u8])>(&self, start: &[u8], end: &[u8], mut f: F) {
        let lower_bound = Bound::Included(start.to_vec());
        let upper_bound = Bound::Excluded(end.to_vec());
        for e in self.list.range((lower_bound, upper_bound)) {
            f(e.key(), e.value())
        }
    }
}

fn bench_skiplist<M: MemtableRep + 'static>(
    c: &mut Criterion,
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
            let mut ctx = MemTableContext::default();
            let mut data_size = 0;
            for _ in 0..10000 {
                let last_sequence = sequence.fetch_add(100, Ordering::SeqCst);
                if m.mem_size() + 1000 > max_write_buffer_size || last_sequence > max_key_count {
                    break;
                }
                let k = rng.next_u64() % 100000;
                let gap = rng.next_u32() % 10000;
                //let gap = 10000;
                let mut key = b"t_00000001_".to_vec();
                let l = key.len();

                for i in 0..100 {
                    let j = k + i * gap as u64;
                    key.resize(l, 0);
                    key.extend_from_slice(&j.to_le_bytes());
                    data_size += key.len() + 8 + i as usize + 20;
                    m.add(&mut ctx, &key, &v[..(i as usize + 20)], last_sequence + i);
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
    let now = Instant::now();
    println!(
        "{} write {} keys cost time: {:?}",
        memtable.name(),
        global_sequence.load(Ordering::Acquire),
        cost
    );
    let mut rng = thread_rng();
    let seek_tail = (global_sequence.load(Ordering::Acquire) + 1) << 8;
    let mut found_count = 0;
    c.bench_function(memtable.name(), |b| {
        b.iter(|| {
            let k = rng.next_u64() % 100000;
            let gap = 5;
            let mut start_key = b"t_00000001_".to_vec();
            let mut end_key = b"t_00000001_".to_vec();
            start_key.extend_from_slice(&k.to_le_bytes());
            start_key.extend_from_slice(&seek_tail.to_le_bytes());
            end_key.extend_from_slice(&(k + gap).to_le_bytes());
            end_key.extend_from_slice(&seek_tail.to_le_bytes());
            memtable.scan(&start_key, &end_key, |_a, _b| {
                found_count += 1;
            });
        });
    });
    let cost = now.elapsed();
    println!(
        "{} read {} keys cost time: {:?}",
        memtable.name(),
        found_count,
        cost
    );

    data_size
}

fn bench_memtable(c: &mut Criterion) {
    let comparator = InternalKeyComparator::default();
    let write_buffer_size: usize = 256 * 1024 * 1024;
    let max_key_count = 2 * 1024 * 1024;
    {
        let now = Instant::now();
        let memtable = Arc::new(SkipListMemtableRep::new(
            comparator.clone(),
            write_buffer_size,
        ));
        let data_size = bench_skiplist(c, memtable.clone(), write_buffer_size, max_key_count);
        let cost = now.elapsed();
        println!(
            "SkipListMemtableRep mem_size: {}, cost time: {:?}",
            data_size + memtable.mem_size(),
            cost
        );
    }
    {
        let now = Instant::now();
        let memtable = Arc::new(InlineSkipListMemtableRep::new(comparator.clone()));
        bench_skiplist(c, memtable.clone(), write_buffer_size, max_key_count);
        let cost = now.elapsed();
        println!(
            "InlineSkipListMemtableRep cost mem_size: {} cost time: {:?}",
            memtable.mem_size(),
            cost
        );
    }
    {
        let now = Instant::now();
        let memtable = Arc::new(CrossbeamSkiplistRep {
            list: SkipMap::default(),
            comp: comparator.clone(),
        });
        bench_skiplist(c, memtable.clone(), write_buffer_size, max_key_count);
        let cost = now.elapsed();
        println!("CrossbeamSkiplistRep cost time: {:?}", cost);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_memtable
}
