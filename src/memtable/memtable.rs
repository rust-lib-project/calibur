use super::list::Skiplist;
use crate::common::{FixedLengthSuffixComparator, KeyComparator};

pub struct Memtable {
    list: Skiplist<FixedLengthSuffixComparator>,
}
