use std::env;

use glacon::setup;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = env::args().collect::<Vec<_>>();

    let namespace_default = "db".to_string();
    let namespace = args.get(1).unwrap_or(&namespace_default);
    let table_name_default = "orders".to_string();
    let table_name = args.get(2).unwrap_or(&table_name_default);

    let _ = setup(namespace.clone(), table_name.clone()).await?;

    Ok(())
}
