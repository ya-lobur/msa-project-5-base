-- Create marketing database
CREATE DATABASE marketing_db;

-- Connect to marketing_db
\c marketing_db;

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data (stub)
INSERT INTO customers (email, name, is_active) VALUES
    ('customer1@example.com', 'Customer 1', TRUE),
    ('customer2@example.com', 'Customer 2', TRUE),
    ('customer3@example.com', 'Customer 3', FALSE);

INSERT INTO orders (customer_id, total_amount, status) VALUES
    (1, 100.00, 'completed'),
    (1, 200.00, 'pending'),
    (2, 150.00, 'completed');
