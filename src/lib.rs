use crate::order::Order;
use arrow::array::{Float32Array, Int32Array, RecordBatch, TimestampMicrosecondArray};
use chrono::Datelike;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{
        Literal, NestedField, PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
    },
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
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::file::properties::WriterProperties;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use uuid::Uuid;

pub mod order;

pub async fn setup(namespace: String, table_name: String) -> anyhow::Result<RestCatalog> {
    let catalog_cfg = RestCatalogConfig::builder()
        .uri("http://localhost:8181".to_string())
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
        ])
        .build()
        .expect("could not build schema");

    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(4, "ts_day", Transform::Day)
        .expect("could not add partition field")
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
    batches: Vec<(u32, RecordBatch)>,
) -> anyhow::Result<()> {
    let mut data_files = vec![];

    for (day, batch) in batches {
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;

        let table_name = table.identifier().name();

        let file_name_generator = DefaultFileNameGenerator::new(
            table_name.to_string(),
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
            Some(Struct::from_iter([Some(Literal::int(day as i32))])),
            0,
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

pub async fn create_batches_by_day(
    schema: Arc<arrow_schema::Schema>,
    orders: Vec<Order>,
) -> anyhow::Result<Vec<(u32, RecordBatch)>> {
    let mut day_groups: BTreeMap<u32, Vec<&Order>> = BTreeMap::new();

    // Group orders by day of month
    for order in &orders {
        let day = order.ts.day();
        day_groups.entry(day).or_default().push(order);
    }

    // Convert each group into a RecordBatch
    let mut batches = Vec::new();

    for (day, group) in day_groups {
        let size = group.len();

        let mut ids = Vec::with_capacity(size);
        let mut customer_ids = Vec::with_capacity(size);
        let mut amounts = Vec::with_capacity(size);
        let mut tss = Vec::with_capacity(size);

        for order in group {
            ids.push(order.id as i32);
            customer_ids.push(order.customer_id as i32);
            amounts.push(order.amount);
            tss.push(order.ts.timestamp_micros());
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

        batches.push((day, batch));
        tracing::info!("Generated {} records for day {} ", size, day);
    }

    tracing::info!(
        "Generated {} day-based batches from {} records",
        batches.len(),
        orders.len()
    );

    Ok(batches)
}

pub async fn create_batches_by_size(
    schema: Arc<arrow_schema::Schema>,
    orders: Vec<Order>,
) -> anyhow::Result<Vec<RecordBatch>> {
    let batch_size = 100_000_000;
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
            ids.push(order.id as i32);
            customer_ids.push(order.customer_id as i32);
            amounts.push(order.amount);
            tss.push(order.ts.timestamp_micros());
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
