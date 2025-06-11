CREATE TABLE namespace_2025_06_10_14_31_10.orders2 (
    id INT NOT NULL,
    customer_id INT NOT NULL,
    amount FLOAT NOT NULL,
    ts TIMESTAMP NOT NULL,
    order_type INT NOT NULL
) PARTITIONED BY (days(ts));

INSERT INTO
    namespace_2025_06_10_14_31_10.orders1
SELECT
    id,
    customer_id,
    amount,
    ts,
    order_type
FROM
    namespace_2025_06_10_14_31_10.orders;

SELECT
    COUNT(*)
FROM
    namespace_2025_06_10_14_31_10.orders
WHERE
    ts > TIMESTAMP '2025-05-20 00:00:00';

SELECT
    COUNT(*)
FROM
    namespace_2025_06_10_14_31_10.orders1
WHERE
    ts > TIMESTAMP '2025-05-20 00:00:00';

SELECT
    COUNT(*)
FROM
    namespace_2025_06_10_14_31_10.orders1
WHERE
    ts > TIMESTAMP '2025-05-20';