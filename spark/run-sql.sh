#!/bin/bash
set -e

SQL_DIR="/opt/spark/sql-scripts"

if [ ! -d "$SQL_DIR" ]; then
    echo "SQL directory $SQL_DIR not found"
    exit 1
fi

echo "Executing SQL files from $SQL_DIR..."

for file in "$SQL_DIR"/*.sql; do
    if [ -f "$file" ]; then
        echo "Running: $(basename "$file")"
        /opt/spark/bin/spark-sql -f "$file"
    fi
done

echo "All SQL scripts executed successfully."

"tail", "-f", "/dev/null"
