-- Инициализация базы данных для системы обработки заказов
CREATE DATABASE IF NOT EXISTS orders_db;
USE orders_db;

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    buyer   VARCHAR(255)  NOT NULL,
    product VARCHAR(255)  NOT NULL,
    amount  DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
