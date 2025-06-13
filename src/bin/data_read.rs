use futures::StreamExt;
use glacon::setup;
use iceberg::{Catalog, TableIdent};

use std::{env, time::Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = env::args().collect::<Vec<_>>();

    let namespace_default = "namespace".to_string();
    let namespace = args.get(1).unwrap_or(&namespace_default);
    let table_name_default = "orders".to_string();
    let table_name = args.get(2).unwrap_or(&table_name_default);
    let catalog = setup(namespace.clone(), table_name.clone()).await?;

    let table = catalog
        .load_table(&TableIdent::from_strs([
            namespace.clone(),
            table_name.clone(),
        ])?)
        .await?;
    tracing::info!("Table {}.{} loaded", namespace, table_name);

    let now = Instant::now();
    let mut stream = table
        .scan()
        .with_batch_size(None)
        .select_all()
        .build()?
        .to_arrow()
        .await?;

    let mut x = 0;

    while (stream.next().await).is_some() {
        x += 1;
    }

    tracing::info!("Got {} rows in {:?}", x, now.elapsed());

    Ok(())
}
