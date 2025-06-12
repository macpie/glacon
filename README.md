# Glacon

## Setup

```sh
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_REGION=us-east-1
```

## Spark

Download `iceberg-aws-bundle-1.9.1.jar` and `iceberg-spark-runtime-3.5_2.12-1.9.1.jar` from https://repo1.maven.org/maven2/org/apache/iceberg/ and put in `spark/jars`

`docker exec -it glacon-spark-1 /opt/spark/bin/spark-sql`