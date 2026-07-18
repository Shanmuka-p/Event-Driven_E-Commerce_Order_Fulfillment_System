#!/bin/bash
set -e

echo "Initializing databases..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE order_db;
    CREATE DATABASE payment_db;
    CREATE DATABASE inventory_db;
    CREATE DATABASE shipping_db;
EOSQL

echo "Creating tables in order_db..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "order_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS orders (
        id VARCHAR(50) PRIMARY KEY,
        items JSONB NOT NULL,
        total_price DECIMAL(10, 2) NOT NULL,
        status VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS outbox (
        id VARCHAR(50) PRIMARY KEY,
        event_type VARCHAR(100) NOT NULL,
        aggregate_id VARCHAR(50) NOT NULL,
        payload JSONB NOT NULL,
        routing_key VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        processed BOOLEAN DEFAULT FALSE,
        processed_at TIMESTAMP
    );
EOSQL

echo "Creating tables in payment_db..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "payment_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS processed_events (
        event_id VARCHAR(50) PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS payments (
        id VARCHAR(50) PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        amount DECIMAL(10, 2) NOT NULL,
        status VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS outbox (
        id VARCHAR(50) PRIMARY KEY,
        event_type VARCHAR(100) NOT NULL,
        aggregate_id VARCHAR(50) NOT NULL,
        payload JSONB NOT NULL,
        routing_key VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        processed BOOLEAN DEFAULT FALSE,
        processed_at TIMESTAMP
    );
EOSQL

echo "Creating tables in inventory_db..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "inventory_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS processed_events (
        event_id VARCHAR(50) PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS inventory (
        product_id VARCHAR(100) PRIMARY KEY,
        stock INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS reservations (
        id VARCHAR(50) PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        product_id VARCHAR(100) NOT NULL,
        quantity INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS outbox (
        id VARCHAR(50) PRIMARY KEY,
        event_type VARCHAR(100) NOT NULL,
        aggregate_id VARCHAR(50) NOT NULL,
        payload JSONB NOT NULL,
        routing_key VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        processed BOOLEAN DEFAULT FALSE,
        processed_at TIMESTAMP
    );
    INSERT INTO inventory (product_id, stock) VALUES ('prod-123', 100) ON CONFLICT (product_id) DO NOTHING;
    INSERT INTO inventory (product_id, stock) VALUES ('FAIL-ME', 10) ON CONFLICT (product_id) DO NOTHING;
EOSQL

echo "Creating tables in shipping_db..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "shipping_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS processed_events (
        event_id VARCHAR(50) PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS shipments (
        id VARCHAR(50) PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        tracking_number VARCHAR(100) NOT NULL,
        status VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS outbox (
        id VARCHAR(50) PRIMARY KEY,
        event_type VARCHAR(100) NOT NULL,
        aggregate_id VARCHAR(50) NOT NULL,
        payload JSONB NOT NULL,
        routing_key VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        processed BOOLEAN DEFAULT FALSE,
        processed_at TIMESTAMP
    );
EOSQL

echo "Database initialization complete!"
