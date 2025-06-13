CREATE TABLE IF NOT EXISTS db.orders (
    id INT NOT NULL,
    customer_id INT NOT NULL,
    amount FLOAT NOT NULL,
    ts TIMESTAMP_NTZ NOT NULL,
    order_type INT NOT NULL
) PARTITIONED BY (days(ts));