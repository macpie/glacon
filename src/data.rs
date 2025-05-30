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
use std::sync::Arc;
use uuid::Uuid;

pub async fn insert(catalog: &RestCatalog, table: Table, batch: RecordBatch) -> anyhow::Result<()> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "orders".to_string(),
        Some(Uuid::new_v4().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );

    // Create a parquet file writer builder. The parameter can get from table.
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
    let mut data_file_writer = data_file_writer_builder.build().await?;

    data_file_writer.write(batch).await?;
    // Close the write and it will return data files back
    let data_files = data_file_writer.close().await?;

    let txn = Transaction::new(&table);
    let mut action = txn.fast_append(None, vec![])?;
    action.add_data_files(data_files)?;
    let tx = action.apply().await?;
    tx.commit(catalog).await?;
    tracing::info!("Data committed to Iceberg.");

    Ok(())
}

pub async fn generate_record_batch(
    schema: Arc<arrow_schema::Schema>,
    max: u32,
) -> anyhow::Result<RecordBatch> {
    let mut ids = vec![];
    let mut customer_ids = vec![];
    let mut amounts = vec![];
    let mut tss = vec![];

    let mut rng = rand::rng();

    for _ in 0..max {
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
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(customer_ids)),
            Arc::new(Float32Array::from(amounts)),
            Arc::new(TimestampMicrosecondArray::from(tss)),
        ],
    )?;

    tracing::info!("Generated {max} records");

    Ok(batch)
}
