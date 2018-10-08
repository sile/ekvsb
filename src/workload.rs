use std::time::Instant;
use std::vec;

use kvs::KeyValueStore;
use task::{Existence, Method, Seconds, Task, TaskResult};

#[derive(Debug, Serialize, Deserialize)]
pub struct Workload(Vec<Task>);
impl Workload {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug)]
pub struct WorkloadExecutor<T> {
    kvs: T,
    workload: vec::IntoIter<Task>,
    start_time: Instant,
    seqno: usize,
}
impl<T: KeyValueStore> WorkloadExecutor<T> {
    pub fn new(kvs: T, workload: Workload) -> Self {
        WorkloadExecutor {
            kvs,
            workload: workload.0.into_iter(),
            start_time: Instant::now(),
            seqno: 0,
        }
    }
}
impl<T: KeyValueStore> Iterator for WorkloadExecutor<T> {
    type Item = TaskResult;

    fn next(&mut self) -> Option<Self::Item> {
        let seqno = self.seqno;
        self.seqno += 1;
        match self.workload.next() {
            Some(Task::Put { key, value, .. }) => {
                let value = value.generate();
                let start_time = self.start_time.elapsed();
                let result = self.kvs.put(key.as_ref(), &value);
                let end_time = self.start_time.elapsed();
                let result = TaskResult {
                    seqno,
                    key,
                    method: Method::Put,
                    start_time: Seconds::new(start_time),
                    elapsed: Seconds::new(end_time - start_time),
                    exists: result
                        .as_ref()
                        .ok()
                        .cloned()
                        .unwrap_or_else(Existence::unknown),
                    error: result.err(),
                };
                Some(result)
            }
            Some(Task::Get { key, .. }) => {
                let start_time = self.start_time.elapsed();
                let result = self.kvs.get(key.as_ref());
                let end_time = self.start_time.elapsed();
                let result = TaskResult {
                    seqno,
                    key,
                    method: Method::Get,
                    start_time: Seconds::new(start_time),
                    elapsed: Seconds::new(end_time - start_time),
                    exists: result
                        .as_ref()
                        .ok()
                        .map(|v| Existence::new(v.is_some()))
                        .unwrap_or_else(Existence::unknown),
                    error: result.err(),
                };
                Some(result)
            }
            Some(Task::Delete { key, .. }) => {
                let start_time = self.start_time.elapsed();
                let result = self.kvs.delete(key.as_ref());
                let end_time = self.start_time.elapsed();
                let result = TaskResult {
                    seqno,
                    key,
                    method: Method::Delete,
                    start_time: Seconds::new(start_time),
                    elapsed: Seconds::new(end_time - start_time),
                    exists: result
                        .as_ref()
                        .ok()
                        .cloned()
                        .unwrap_or_else(Existence::unknown),
                    error: result.err(),
                };
                Some(result)
            }
            None => None,
        }
    }
}
