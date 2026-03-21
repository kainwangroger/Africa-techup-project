CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;

-- Drop tables si elles existent
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS publishers;

-- =========================
-- Table Authors
-- =========================
CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    author_name TEXT UNIQUE NOT NULL
);

-- =========================
-- Table Publishers
-- =========================
CREATE TABLE publishers (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name TEXT UNIQUE NOT NULL
);

-- =========================
-- Table Books
-- =========================
CREATE TABLE books (
    isbn VARCHAR(20) PRIMARY KEY,
    title TEXT NOT NULL,
    year_of_publication INTEGER,
    author_id INTEGER REFERENCES authors(author_id),
    publisher_id INTEGER REFERENCES publishers(publisher_id),
    image_url_s TEXT,
    image_url_m TEXT,
    image_url_l TEXT
);

-- =========================
-- Index pour performance
-- =========================
CREATE INDEX idx_books_author ON books(author_id);
CREATE INDEX idx_books_publisher ON books(publisher_id);
CREATE INDEX idx_books_year ON books(year_of_publication);
