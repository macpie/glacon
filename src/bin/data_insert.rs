use glacon::{create_partitioned_batches, insert, order::Order};
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::{env, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = env::args().collect::<Vec<_>>();

    let namespace_default = "db".to_string();
    let namespace = args.get(1).unwrap_or(&namespace_default);
    let table_name_default = "orders".to_string();
    let table_name = args.get(2).unwrap_or(&table_name_default);
    let how_many_default = "10000000".to_string();
    let how_many = args.get(3).unwrap_or(&how_many_default);
    let how_many = how_many.parse::<u32>().expect("Failed to parse int");

    let catalog_cfg = RestCatalogConfig::builder()
        .uri("http://localhost:8181/iceberg".to_string())
        .warehouse("warehouse".to_string())
        .build();
    let catalog = RestCatalog::new(catalog_cfg);
    let table = catalog
        .load_table(&TableIdent::from_strs([
            namespace.clone(),
            table_name.clone(),
        ])?)
        .await?;
    tracing::info!("Table loaded");

    let mut orders = vec![];

    for _ in 0..how_many {
        orders.push(Order::generate());
    }

    let schema: Arc<arrow_schema::Schema> =
        Arc::new(table.metadata().current_schema().as_ref().try_into()?);
    let batches = create_partitioned_batches(schema, orders).await?;

    insert(&catalog, table, batches).await?;

    Ok(())
}
