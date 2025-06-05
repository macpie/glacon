```sql
CREATE CONNECTION iceberg_conn WITH (
    type = 'iceberg',
    catalog.type='rest',
    catalog.uri= 'http://rest:8181',
    warehouse.path = 'warehouse/',
    s3.endpoint = 'http://minio:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
);

SET iceberg_engine_connection = 'public.iceberg_conn';
ALTER system SET iceberg_engine_connection = 'public.iceberg_conn';


CREATE TABLE names (
    id INT PRIMARY KEY, 
    name VARCHAR
) WITH (commit_checkpoint_interval = 1) 
ENGINE = iceberg;

INSERT INTO names (id, name)
VALUES
  (1, 'Bob'),
  (2,  'Peter');
```