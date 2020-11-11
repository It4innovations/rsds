# rsds

``rsds`` is a Rust implementation of dask-scheduler for [https://distributed.dask.org](dask/distributed).
It is an experiment for evaluating performance gain of non-Python scheduler and for playing with different schedulers.

## Usage
To compile and use `rsds`, you must have Rust toolchain installed. You can install it using e.g. [Rustup](https://rustup.rs/).

1) Build `rsds`:
```bash
$ cargo build --release
```
2) Install our modified version of Dask:
```bash
$ pip install git+https://github.com/Kobzol/distributed@simplified-encoding
```
3) Use `rsds-scheduler` instead of `dask-scheduler` when starting a Dask cluster:
```bash
$ ./target/release/rsds-scheduler
```

## Warning!

This branch uses a simplified Dask protocol, hence it does *NOT* work with an upstream version. You have to use Dask from:

https://github.com/Kobzol/distributed/

branch: simple-frame

## Reports

* https://github.com/dask/distributed/issues/3139
