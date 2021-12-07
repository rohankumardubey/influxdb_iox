use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use datafusion::parquet::file::serialized_reader::SerializedFileReader;
use read_buffer::RBChunk;
use std::fs::File;
use std::sync::mpsc::channel;
use std::sync::Arc;
use threadpool::ThreadPool;

fn main() {
    let files = vec![
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 06:00:00/9b8985bb-0a1e-4c85-b4df-564d9e43f079.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 06:00:00/dddb3c40-15ca-4c02-9360-d865094ee092.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 05:00:00/33837661-7501-460a-9b66-e64549b25b1c.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 07:00:00/48642d71-1206-42a6-a5d8-5bc0faa752d4.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 07:00:00/fb425523-c822-486c-b317-72a3ba7dbe57.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 07:00:00/012e9c79-65c8-419c-8a60-c26fbe0bb913.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 10:00:00/5fafd0c1-0d0d-4798-87a0-6accb7d54ec1.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 10:00:00/47f7d3a6-714e-4ffc-b8f6-0d144c7653d8.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 09:00:00/4e265d94-1479-43f4-8699-7b35738c0d30.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 09:00:00/91be1c79-d7da-41f0-b52f-f8816fe1bc7c.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 11:00:00/d2071087-31ab-4e69-90b8-b4e9d81c97fb.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 08:00:00/b2152255-5ca8-455d-bbc2-59cdbfbf6818.parquet",
        "/Users/edd/Documents/flux_query_log/iox_parquet/dbs/925280ba-4f8d-413a-a955-d86dd4b172d9/data/query_log/2021-11-30 08:00:00/2612a9fe-e823-432c-bc65-5c840fd21a08.parquet",
    ];

    let batch_size = 1024 * 25; // matches current batch size in IOx

    let num_threads = num_cpus::get();
    // let num_threads = 13;

    let now = std::time::Instant::now();
    println!("creating thread pool of size {:?}", num_threads);
    let pool = ThreadPool::new(num_threads);
    let (tx, rx) = channel();

    for file in files.clone() {
        let tx = tx.clone();
        pool.execute(move || {
            println!("creating RUB chunk");
            let now = std::time::Instant::now();
            let chunk = load_chunk_rub(file, batch_size);

            let elapsed = now.elapsed().as_secs();

            let throughput = if elapsed == 0 {
                f64::INFINITY
            } else {
                chunk.rows() as f64 / elapsed as f64
            };

            println!(
                "RUB chunk created in {:?} {:?} rows/sec",
                now.elapsed(),
                throughput
            );

            tx.send(chunk.row_groups()).expect("Could not send data!");
        });
    }

    let mut row_groups = 0;
    for _ in &files {
        row_groups += rx.recv().unwrap();
    }

    println!(
        "Finished in {:?}. Loaded {:?} chunks with {:?} row groups to the Read Buffer",
        now.elapsed(),
        files.len(),
        row_groups,
    );
}

fn load_chunk_rub(path: &str, batch_size: usize) -> RBChunk {
    println!("loading parquet file from {:?}", path);
    let file = File::open(path).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let record_batch_reader = arrow_reader.get_record_reader(batch_size).unwrap();
    let mut itr = record_batch_reader;
    let cm = read_buffer::ChunkMetrics::new_unregistered();

    let mut chunk = read_buffer::RBChunk::new("my_table", itr.next().unwrap().unwrap(), cm);
    for rb in itr {
        chunk.upsert_table(rb.unwrap());
    }

    chunk
}
