use glacon::{create_batches_by_day, insert, order::Order, setup};
use iceberg::{Catalog, TableIdent};
use rand::Rng;
use std::{env, sync::Arc, thread::sleep, time::Duration};
use tokio::{sync::mpsc, task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = env::args().collect::<Vec<_>>();

    let namespace_default = "namespace".to_string();
    let namespace = args.get(1).unwrap_or(&namespace_default);
    let table_name_default = "orders".to_string();
    let table_name = args.get(2).unwrap_or(&table_name_default);
    let catalog = setup(namespace.clone(), table_name.clone()).await?;
    let catalog_ref = Arc::new(catalog);

    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<Order>>();

    // Spawn a task to send messages
    let tx1 = tx.clone();
    task::spawn(async move {
        let mut rng = rand::rng();
        loop {
            let mut orders = vec![];

            for _ in 0..rng.random_range(10_000_000..10_000_001) {
                orders.push(Order::generate());
            }

            tx1.send(orders).unwrap();

            sleep(Duration::from_secs(rng.random_range(10..30)));
        }
    });

    while let Some(orders) = rx.recv().await {
        let total = orders.len();
        tracing::info!("got {} orders", total);

        let catalog = catalog_ref.clone();
        let table = catalog
            .load_table(&TableIdent::from_strs([
                namespace.clone(),
                table_name.clone(),
            ])?)
            .await?;
        tracing::info!("Table loaded");

        let schema: Arc<arrow_schema::Schema> =
            Arc::new(table.metadata().current_schema().as_ref().try_into()?);
        let batches = create_batches_by_day(schema, orders).await?;

        insert(&catalog, table, batches).await?;

        tracing::info!("All done with {} orders", total);
    }

    Ok(())
}
