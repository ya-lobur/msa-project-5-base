-- Database initialization script for shipments ETL system
-- Creates tables and generates test data

-- Drop tables if they exist
DROP TABLE IF EXISTS shipment_events CASCADE;
DROP TABLE IF EXISTS shipments CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS clients CASCADE;

-- Create clients table
CREATE TABLE clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(50),
    address TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create drivers table
CREATE TABLE drivers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    license_number VARCHAR(50) NOT NULL UNIQUE,
    phone VARCHAR(50) NOT NULL,
    email VARCHAR(255),
    rating DECIMAL(3, 2) DEFAULT 5.00,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create vehicles table
CREATE TABLE vehicles (
    id SERIAL PRIMARY KEY,
    license_plate VARCHAR(50) NOT NULL UNIQUE,
    model VARCHAR(255) NOT NULL,
    capacity_kg DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'available',
    year INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create shipments table
CREATE TABLE shipments (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL REFERENCES clients(id),
    driver_id INTEGER NOT NULL REFERENCES drivers(id),
    vehicle_id INTEGER NOT NULL REFERENCES vehicles(id),
    origin VARCHAR(255) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    cargo_type VARCHAR(100) NOT NULL,
    cargo_weight DECIMAL(10, 2) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    pickup_date TIMESTAMP,
    delivery_date TIMESTAMP,
    notes TEXT
);

-- Create shipment_events table
CREATE TABLE shipment_events (
    id SERIAL PRIMARY KEY,
    shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMP NOT NULL DEFAULT NOW(),
    location VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX idx_shipments_client_id ON shipments(client_id);
CREATE INDEX idx_shipments_driver_id ON shipments(driver_id);
CREATE INDEX idx_shipments_vehicle_id ON shipments(vehicle_id);
CREATE INDEX idx_shipments_status ON shipments(status);
CREATE INDEX idx_shipments_created_at ON shipments(created_at);
CREATE INDEX idx_shipment_events_shipment_id ON shipment_events(shipment_id);
CREATE INDEX idx_shipment_events_event_time ON shipment_events(event_time);

-- Insert test data for clients (10,000 - 50,000 rows)
-- Generating 20,000 clients for this example
INSERT INTO clients (name, email, phone, address)
SELECT
    'Client ' || i,
    'client' || i || '@example.com',
    '+7' || LPAD((900 + (i % 100))::TEXT, 3, '0') || LPAD(i::TEXT, 7, '0'),
    'Address ' || i || ', Moscow, Russia'
FROM generate_series(1, 20000) AS i;

-- Insert test data for drivers (1,000 - 5,000 rows)
-- Generating 2,000 drivers
INSERT INTO drivers (name, license_number, phone, email, rating, status)
SELECT
    'Driver ' || i,
    'DL' || LPAD(i::TEXT, 8, '0'),
    '+7' || LPAD((900 + (i % 100))::TEXT, 3, '0') || LPAD(i::TEXT, 7, '0'),
    'driver' || i || '@example.com',
    4.0 + (random() * 1.0),
    CASE WHEN i % 10 = 0 THEN 'inactive' ELSE 'active' END
FROM generate_series(1, 2000) AS i;

-- Insert test data for vehicles (500 - 2,000 rows)
-- Generating 1,000 vehicles
INSERT INTO vehicles (license_plate, model, capacity_kg, status, year)
SELECT
    'A' || LPAD((i % 1000)::TEXT, 3, '0') || 'AA' || LPAD((i / 1000 + 1)::TEXT, 2, '0') || '77',
    CASE (i % 5)
        WHEN 0 THEN 'Kamaz 5490'
        WHEN 1 THEN 'Mercedes Actros'
        WHEN 2 THEN 'Volvo FH16'
        WHEN 3 THEN 'Scania R500'
        ELSE 'MAN TGX'
    END,
    5000 + (i % 20) * 1000,
    CASE WHEN i % 15 = 0 THEN 'maintenance' ELSE 'available' END,
    2015 + (i % 9)
FROM generate_series(1, 1000) AS i;

-- Insert test data for shipments (50,000 - 200,000 rows)
-- Generating 100,000 shipments for this example
INSERT INTO shipments (
    client_id, driver_id, vehicle_id, origin, destination,
    cargo_type, cargo_weight, price, status,
    created_at, updated_at, pickup_date, delivery_date, notes
)
SELECT
    (i % 20000) + 1,  -- client_id
    (i % 2000) + 1,   -- driver_id
    (i % 1000) + 1,   -- vehicle_id
    CASE (i % 10)
        WHEN 0 THEN 'Moscow'
        WHEN 1 THEN 'Saint Petersburg'
        WHEN 2 THEN 'Novosibirsk'
        WHEN 3 THEN 'Yekaterinburg'
        WHEN 4 THEN 'Kazan'
        WHEN 5 THEN 'Nizhny Novgorod'
        WHEN 6 THEN 'Chelyabinsk'
        WHEN 7 THEN 'Samara'
        WHEN 8 THEN 'Omsk'
        ELSE 'Rostov-on-Don'
    END,
    CASE ((i + 5) % 10)
        WHEN 0 THEN 'Moscow'
        WHEN 1 THEN 'Saint Petersburg'
        WHEN 2 THEN 'Novosibirsk'
        WHEN 3 THEN 'Yekaterinburg'
        WHEN 4 THEN 'Kazan'
        WHEN 5 THEN 'Nizhny Novgorod'
        WHEN 6 THEN 'Chelyabinsk'
        WHEN 7 THEN 'Samara'
        WHEN 8 THEN 'Omsk'
        ELSE 'Rostov-on-Don'
    END,
    CASE (i % 8)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Food Products'
        WHEN 2 THEN 'Construction Materials'
        WHEN 3 THEN 'Furniture'
        WHEN 4 THEN 'Automotive Parts'
        WHEN 5 THEN 'Chemicals'
        WHEN 6 THEN 'Textiles'
        ELSE 'General Cargo'
    END,
    1000 + (i % 5000),  -- cargo_weight
    5000 + (i % 50000),  -- price
    CASE (i % 5)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'in_transit'
        WHEN 2 THEN 'delivered'
        WHEN 3 THEN 'cancelled'
        ELSE 'completed'
    END,
    NOW() - INTERVAL '1 day' * (i % 90),  -- created_at (last 90 days)
    NOW() - INTERVAL '1 day' * (i % 90),  -- updated_at
    CASE WHEN i % 5 != 0 THEN NOW() - INTERVAL '1 day' * (i % 90) + INTERVAL '1 hour' END,  -- pickup_date
    CASE WHEN i % 5 = 4 THEN NOW() - INTERVAL '1 day' * (i % 90) + INTERVAL '2 days' END,  -- delivery_date
    CASE WHEN i % 7 = 0 THEN 'Fragile cargo, handle with care' ELSE NULL END
FROM generate_series(1, 100000) AS i;

-- Insert test data for shipment_events (500,000 - 1,000,000 rows)
-- Generating ~700,000 events (7 events per shipment on average)
INSERT INTO shipment_events (shipment_id, event_type, event_time, location, description)
SELECT
    shipment_id,
    event_type,
    event_time,
    location,
    description
FROM (
    SELECT
        s.id AS shipment_id,
        CASE (sub.event_num % 7)
            WHEN 0 THEN 'created'
            WHEN 1 THEN 'assigned'
            WHEN 2 THEN 'picked_up'
            WHEN 3 THEN 'in_transit'
            WHEN 4 THEN 'arrived'
            WHEN 5 THEN 'delivered'
            ELSE 'completed'
        END AS event_type,
        s.created_at + INTERVAL '1 hour' * sub.event_num AS event_time,
        CASE (sub.event_num % 3)
            WHEN 0 THEN s.origin
            WHEN 1 THEN 'Transit Point ' || (sub.event_num / 2)
            ELSE s.destination
        END AS location,
        'Event ' || sub.event_num || ' for shipment ' || s.id AS description
    FROM shipments s
    CROSS JOIN LATERAL (
        SELECT generate_series(0, 6) AS event_num
    ) sub
    WHERE s.id % 100 < 70  -- 70% of shipments have all events
) events_data;

-- Add some additional events for active shipments
INSERT INTO shipment_events (shipment_id, event_type, event_time, location, description)
SELECT
    s.id,
    'status_update',
    NOW() - INTERVAL '1 hour' * (random() * 24)::INTEGER,
    'Checkpoint ' || ((s.id % 10) + 1),
    'Shipment status updated'
FROM shipments s
WHERE s.status IN ('in_transit', 'pending')
AND s.id % 10 = 0;

-- Update statistics for better query planning
ANALYZE clients;
ANALYZE drivers;
ANALYZE vehicles;
ANALYZE shipments;
ANALYZE shipment_events;

-- Print summary
DO $$
DECLARE
    client_count INTEGER;
    driver_count INTEGER;
    vehicle_count INTEGER;
    shipment_count INTEGER;
    event_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO client_count FROM clients;
    SELECT COUNT(*) INTO driver_count FROM drivers;
    SELECT COUNT(*) INTO vehicle_count FROM vehicles;
    SELECT COUNT(*) INTO shipment_count FROM shipments;
    SELECT COUNT(*) INTO event_count FROM shipment_events;

    RAISE NOTICE 'Database initialization completed:';
    RAISE NOTICE '  Clients: % rows', client_count;
    RAISE NOTICE '  Drivers: % rows', driver_count;
    RAISE NOTICE '  Vehicles: % rows', vehicle_count;
    RAISE NOTICE '  Shipments: % rows', shipment_count;
    RAISE NOTICE '  Shipment Events: % rows', event_count;
END $$;
