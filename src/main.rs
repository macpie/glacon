use data_project::{
    data::{generate_record_batch, insert},
    setup,
};
use iceberg::{Catalog, TableIdent};
use rand::Rng;
use std::sync::Arc;
use tokio::{sync::mpsc, task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let namespace = "namespace1".to_string();
    let table_name = "orders".to_string();
    let catalog = setup(namespace.clone(), table_name.clone()).await?;
    let catalog_ref = Arc::new(catalog);

    let (tx, mut rx) = mpsc::unbounded_channel::<u32>();

    // Spawn a task to send messages
    let tx1 = tx.clone();
    task::spawn(async move {
        let mut rng = rand::rng();
        loop {
            let n = rng.random_range(1..10_000_000);
            tx1.send(n).unwrap();
        }
    });

    while let Some(n) = rx.recv().await {
        tracing::info!("Got msg {n}");

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
        let batch = generate_record_batch(schema.clone(), n).await?;

        insert(&catalog, table, batch).await?;
    }

    Ok(())
}
