-- SQL script to create test data for pytest using psql_docker

-- Create a test table
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value INTEGER NOT NULL
);

-- Insert test data into test_table
INSERT INTO test_table (name, value) VALUES
('Alice', 10),
('Bob', 20),
('Charlie', 30);

-- Additional data for update, delete, and transaction tests
INSERT INTO test_table (name, value) VALUES
('Ethan', 50),
('Fiona', 70),
('George', 80);
