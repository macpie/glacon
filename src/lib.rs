use crate::{order::Order, partitioned_location_generator::PartitionedLocationGenerator};
use arrow::array::{Float32Array, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{
        Literal, NestedField, PrimitiveType, Schema, Struct, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    },
    table::Table,
    transaction::Transaction,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{ParquetWriterBuilder, location_generator::DefaultFileNameGenerator},
    },
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::{
    basic::{Compression, Encoding, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

pub mod order;
pub mod partitioned_location_generator;

pub async fn setup(namespace: String, table_name: String) -> anyhow::Result<RestCatalog> {
    let catalog_cfg = RestCatalogConfig::builder()
        .uri("http://localhost:8181/iceberg".to_string())
        .warehouse("warehouse".to_string())
        .build();
    let catalog = RestCatalog::new(catalog_cfg);

    let namespace_ident = NamespaceIdent::new(namespace.clone());
    if !catalog.namespace_exists(&namespace_ident).await? {
        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await?;
        tracing::info!("Namespace {} created", namespace);
    }

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "customer_id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(3, "amount", Type::Primitive(PrimitiveType::Float)).into(),
            NestedField::required(4, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::required(5, "order_type", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(6, "note", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("could not build schema");

    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_fields(vec![UnboundPartitionField {
            source_id: 4,
            field_id: None,
            name: "ts_day".to_string(),
            transform: Transform::Day,
        }])
        .expect("could not add partition fields")
        .build();

    let partition_spec = unbound_partition_spec
        .bind(schema.clone())
        .expect("could not bind to schema");

    if !catalog
        .table_exists(&TableIdent::new(
            namespace_ident.clone(),
            table_name.clone(),
        ))
        .await?
    {
        let table_creation = TableCreation::builder()
            .name(table_name.clone())
            .schema(schema.clone())
            .partition_spec(partition_spec)
            .build();

        catalog
            .create_table(&namespace_ident, table_creation)
            .await?;
        tracing::info!("Table {} created", table_name);
    }

    tracing::info!("Setup done");

    Ok(catalog)
}

pub async fn insert(
    catalog: &RestCatalog,
    table: Table,
    batches: HashMap<Vec<DateTime<Utc>>, RecordBatch>,
) -> anyhow::Result<()> {
    let mut data_files = vec![];

    for (part_values, batch) in batches {
        let partition_spec_id = 0;

        let keys = part_values
            .iter()
            .map(|v| v.format("%Y-%m-%d").to_string())
            .collect();
        let location_generator =
            PartitionedLocationGenerator::new(table.metadata().clone(), partition_spec_id, keys)?;

        let table_name = table.identifier().name();

        let file_name_generator = DefaultFileNameGenerator::new(
            table_name.to_string(),
            Some(Uuid::new_v4().to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder()
                .set_encoding(Encoding::PLAIN)
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(11)?))
                .build(),
            table.metadata().current_schema().clone(),
            table.file_io().clone(),
            location_generator.clone(),
            file_name_generator.clone(),
        );

        let partition_values: Vec<Option<Literal>> = part_values
            .iter()
            .map(|v| Some(Literal::int(v.day() as i32)))
            .collect();

        let data_file_writer_builder = DataFileWriterBuilder::new(
            parquet_writer_builder,
            Some(Struct::from_iter(partition_values)),
            partition_spec_id,
        );
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

pub async fn create_partitioned_batches(
    schema: Arc<arrow_schema::Schema>,
    orders: Vec<Order>,
) -> anyhow::Result<HashMap<Vec<DateTime<Utc>>, RecordBatch>> {
    let mut partitioned: HashMap<Vec<DateTime<Utc>>, Vec<&Order>> = HashMap::new();

    for order in &orders {
        let key = vec![set_to_midnight(order.ts)];
        partitioned.entry(key).or_default().push(order);
    }

    let mut batches: HashMap<Vec<DateTime<Utc>>, RecordBatch> = HashMap::new();

    for (key, orders) in partitioned {
        let size = orders.len();

        let mut ids = Vec::with_capacity(size);
        let mut customer_ids = Vec::with_capacity(size);
        let mut amounts = Vec::with_capacity(size);
        let mut tss = Vec::with_capacity(size);
        let mut types = Vec::with_capacity(size);
        let mut notes = Vec::with_capacity(size);

        for order in orders {
            ids.push(order.id as i32);
            customer_ids.push(order.customer_id as i32);
            amounts.push(order.amount);
            tss.push(order.ts.timestamp_micros());
            types.push(order.order_type as i32);
            notes.push(order.note.clone());
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(customer_ids)),
                Arc::new(Float32Array::from(amounts)),
                Arc::new(TimestampMicrosecondArray::from(tss)),
                Arc::new(Int32Array::from(types)),
                Arc::new(StringArray::from(notes)),
            ],
        )?;

        batches.insert(key.clone(), batch);
        tracing::info!("Generated {} records for batch {:?} ", size, key);
    }

    tracing::info!(
        "Generated {} partitioned batches from {} records",
        batches.len(),
        orders.len()
    );

    Ok(batches)
}

fn set_to_midnight(datetime: DateTime<Utc>) -> DateTime<Utc> {
    let date = datetime.date_naive(); // Extracts the NaiveDate (Y-M-D)
    Utc.with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
        .unwrap()
}
