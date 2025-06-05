use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec},
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::collections::HashMap;

pub mod data;

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
        tracing::info!("Namespace created");
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
        tracing::info!("Table created");
    }

    tracing::info!("Setup done");

    Ok(catalog)
}
