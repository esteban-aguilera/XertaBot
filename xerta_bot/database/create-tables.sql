-- SELECT DATABASE
USE xerta_bot;


-- CREATE USERS TABLE
CREATE TABLE users(
    id INT PRIMARY KEY,
    privilege INT DEFAULT 0,
    first_name VARCHAR(32),
    last_name VARCHAR(32),
    username VARCHAR(32),
    language_code VARCHAR(32)
);


-- CREATE COMMANDS TABLE
CREATE TABLE commands(
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    command VARCHAR(255) NOT NULL,
    -- SET FOREIGN KEYS
    FOREIGN KEY(user_id)
        REFERENCES users(id)
        ON DELETE CASCADE
);


-- CREATE JOKES TABLE
CREATE TABLE jokes(
    id INT PRIMARY KEY AUTO_INCREMENT,
    joke VARCHAR(255)
    -- SET UNIQUE KEYS
    UNIQUE KEY(joke),
);