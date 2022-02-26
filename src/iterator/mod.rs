mod async_merge_iterator;
mod merge_iterator;
mod table_accessor;
mod two_level_iterator;

pub use async_merge_iterator::MergingIterator as AsyncMergingIterator;
pub use merge_iterator::MergingIterator;
pub use table_accessor::*;
pub use two_level_iterator::TwoLevelIterator;

use async_trait::async_trait;

pub trait InternalIterator: Send {
    fn valid(&self) -> bool;
    fn seek(&mut self, key: &[u8]);
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn seek_for_prev(&mut self, key: &[u8]);
    fn next(&mut self);
    fn prev(&mut self);
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}

#[async_trait]
pub trait AsyncIterator: Send {
    fn valid(&self) -> bool;
    async fn seek(&mut self, key: &[u8]);
    async fn seek_for_prev(&mut self, key: &[u8]);
    async fn seek_to_first(&mut self);
    async fn seek_to_last(&mut self);
    async fn next(&mut self);
    async fn prev(&mut self);
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}
