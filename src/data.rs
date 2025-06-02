use arrow::array::{Float32Array, Int32Array, RecordBatch, TimestampMicrosecondArray};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use iceberg::{
    spec::{Literal, PrimitiveLiteral, Struct},
    table::Table,
    transaction::Transaction,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use iceberg_catalog_rest::RestCatalog;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::{cmp::min, sync::Arc};
use uuid::Uuid;

pub struct Order {
    id: i32,
    customer_id: i32,
    amount: f32,
    ts: i64,
}

impl Order {
    pub fn generate() -> Self {
        let mut rng = rand::rng();
        let random_day = rng.random_range(1..=31);
        let date = NaiveDate::from_ymd_opt(2025, 5, random_day).unwrap();
        let random_hour = rng.random_range(1..=23);
        let random_min = rng.random_range(0..=59);
        let random_sec = rng.random_range(0..=59);
        let time = NaiveTime::from_hms_opt(random_hour, random_min, random_sec).unwrap();
        let dt = NaiveDateTime::new(date, time);
        let ts = dt.and_utc().timestamp_micros();

        Self {
            id: rng.random_range(1..10_000_000),
            customer_id: rng.random_range(1..10_000_000),
            amount: rng.random_range(1.0..10000.0),
            ts,
        }
    }
}

pub async fn insert(
    catalog: &RestCatalog,
    table: Table,
    batches: Vec<RecordBatch>,
) -> anyhow::Result<()> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "orders".to_string(),
        Some(Uuid::new_v4().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        Some(Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::Int(1),
        ))])),
        0,
    );

    let mut data_files = vec![];

    for batch in batches {
        let data_file_writer_builder = data_file_writer_builder.clone();
        let mut data_file_writer = data_file_writer_builder.build().await?;
        data_file_writer.write(batch).await?;
        let closed = data_file_writer.close().await?;

        data_files.extend(closed);
    }

    let txn = Transaction::new(&table);
    let mut action = txn.fast_append(None, vec![])?;
    action.add_data_files(data_files)?;
    let tx = action.apply().await?;
    tx.commit(catalog).await?;
    tracing::info!("Data committed to Iceberg.");

    Ok(())
}

pub async fn create_batches(
    schema: Arc<arrow_schema::Schema>,
    orders: Vec<Order>,
) -> anyhow::Result<Vec<RecordBatch>> {
    let batch_size = 100_000;
    let mut batches = Vec::new();

    let max = orders.len();
    let mut start = 0;

    while start < max {
        let end = usize::min(start + batch_size, max);
        let chunk = &orders[start..end];

        let mut ids = Vec::with_capacity(chunk.len());
        let mut customer_ids = Vec::with_capacity(chunk.len());
        let mut amounts = Vec::with_capacity(chunk.len());
        let mut tss = Vec::with_capacity(chunk.len());

        for order in chunk {
            ids.push(order.id);
            customer_ids.push(order.customer_id);
            amounts.push(order.amount);
            tss.push(order.ts);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(customer_ids)),
                Arc::new(Float32Array::from(amounts)),
                Arc::new(TimestampMicrosecondArray::from(tss)),
            ],
        )?;

        batches.push(batch);
        start = end;
    }

    tracing::info!("Generated {} batches from {} records", batches.len(), max);

    Ok(batches)
}

pub async fn generate_record_batches(
    schema: Arc<arrow_schema::Schema>,
    max: u64,
) -> anyhow::Result<Vec<RecordBatch>> {
    let batch_size = 100_000;
    let mut batches = Vec::new();

    let mut ids = vec![];
    let mut customer_ids = vec![];
    let mut amounts = vec![];
    let mut tss = vec![];

    let mut rng = rand::rng();
    let mut generated = 0;

    while generated < max {
        ids.clear();
        customer_ids.clear();
        amounts.clear();
        tss.clear();

        let current_batch_size = min(batch_size as u64, max - generated) as usize;

        for _ in 0..current_batch_size {
            let random_day = rng.random_range(1..=31); // 1 to 31 inclusive
            let date = NaiveDate::from_ymd_opt(2025, 5, random_day).unwrap();
            let time = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
            let dt = NaiveDateTime::new(date, time);
            let ts = dt.and_utc().timestamp_micros();

            ids.push(rng.random_range(1..10_000_000));
            customer_ids.push(rng.random_range(1..10_000_000));
            amounts.push(rng.random_range(0.1..100.0));
            tss.push(ts);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.clone())),
                Arc::new(Int32Array::from(customer_ids.clone())),
                Arc::new(Float32Array::from(amounts.clone())),
                Arc::new(TimestampMicrosecondArray::from(tss.clone())),
            ],
        )?;

        batches.push(batch);
        generated += current_batch_size as u64;
    }

    tracing::info!("Generated {max} records");

    Ok(batches)
}
