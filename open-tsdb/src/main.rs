#![allow(dead_code)]
mod delta;
mod head;
mod index;
mod minitsdb;
mod model;
mod promql;
mod query;
mod serde;
mod storage;
#[cfg(test)]
mod test_utils;
mod tsdb;
mod util;

fn main() {
    println!("open-tsdb: timeseries store");
}
