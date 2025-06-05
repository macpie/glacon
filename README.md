# Glacon

## Setup

```sh
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_REGION=us-east-1
```

## Issues

1. Partitions are not in sub directories, see:
   1. https://github.com/apache/iceberg-rust/pull/890
   2. https://github.com/apache/iceberg-rust/issues/891
   3. https://github.com/apache/iceberg-rust/pull/893
2. SQL catalog not ready yet either
```rust
async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
   Err(Error::new(
      ErrorKind::FeatureUnsupported,
         "Updating a table is not supported yet",
   ))
}
```

## Spark

```sql
CREATE TABLE namespace1.orders2
(
  id INT,
  customer_id INT,
  amount FLOAT,
  ts TIMESTAMP
)
PARTITIONED BY ( days(ts));

INSERT INTO namespace1.orders2
SELECT id, customer_id, amount, ts FROM namespace1.orders;

INSERT INTO namespace1.orders2
VALUES (
    123,
    456,
    36.17,
    TIMESTAMP '2025-10-01 12:00:00'
);

SELECT COUNT(*)
FROM namespace1.orders
WHERE ts > TIMESTAMP '2025-05-20 00:00:00';

SELECT COUNT(*)
FROM namespace1.orders2
WHERE ts > TIMESTAMP '2025-05-20 00:00:00';

```
