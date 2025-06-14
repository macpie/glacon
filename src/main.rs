use chrono::Local;
use dotenv::dotenv;
use glacon::{create_partitioned_batches, insert, order::Order, setup};
use iceberg::{Catalog, TableIdent};
use rand::Rng;
use std::{env, sync::Arc, thread::sleep, time::Duration};
use tokio::{sync::mpsc, task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    let args = env::args().collect::<Vec<_>>();

    let now = Local::now().format("%Y_%m_%d_%H_%M_%S").to_string();
    let namespace_default = format!("namespace_{}", now);
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
        tracing::debug!("{:?}", table);

        let schema: Arc<arrow_schema::Schema> =
            Arc::new(table.metadata().current_schema().as_ref().try_into()?);
        let batches = create_partitioned_batches(schema, orders).await?;

        insert(&catalog, table, batches).await?;

        tracing::info!("All done with {} orders", total);
    }

    Ok(())
}
