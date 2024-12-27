#!/bin/bash

# Interactive login for SQL Server credentials
#I'd like to build this out to be a one-time pop-up that creates a refesh daemon, but I've got to sharpen my SecOps chops so I don't bug your computer
#read -p "Enter SQL Server username: " SQL_USER
#read -sp "Enter SQL Server password: " SQL_PASS
#echo ""

# inline credentials for debugging purposes, mirroring my local env
SQL_USER="your_username"
SQL_PASS="your_password"

DB_CONTAINER_NAME="sqledge_container"
DB_NAME="your_database"
CSV_DIR="./csv_files"

# Stage 1: Stage files in Docker container
for csv_file in "$CSV_DIR"/*.csv; do
  docker cp "$csv_file" "$DB_CONTAINER_NAME:/data/"
done

# Stage 2: Run SQL scripts for table creation and data insertion
docker exec -i "$DB_CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd -S localhost -U "$SQL_USER" -P "$SQL_PASS" -d "$DB_NAME" <<-EOSQL
-- CREATE TABLE statements

-- BULK INSERT each_CSV FROM '/stage/filename.csv' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2);
EOSQL

echo "Data ingestion process completed."
