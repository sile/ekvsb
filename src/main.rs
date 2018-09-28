extern crate ekvsb;
#[macro_use]
extern crate trackable;

use ekvsb::KeyValueStore;

fn main() -> trackable::result::MainResult {
    let mut kvs = track!(ekvsb::fs::FileSystemKvs::new("/tmp/ekvsb/"))?;
    for i in 0..100 {
        let i = i.to_string().into_bytes();
        track!(kvs.put(&i, &i))?;
    }
    Ok(())
}
