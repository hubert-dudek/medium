-- Databricks notebook source
CREATE OR REPLACE TABLE main.samples.companies (
    id INT PRIMARY KEY,
    company_name VARCHAR(100),
    country VARCHAR(50),
    post_code VARCHAR(20),
    province_code VARCHAR(5)
);

INSERT INTO main.samples.companies (id, company_name, country, post_code, province_code)
VALUES
(1, 'Best Wines',       'Italy',           '37100', 'VR'),
(2, 'Fashion Group',    'Italy',           '20121', 'MI'),
(3, 'Tech Ltd.',        'Italy',           '00100', 'RM'),
(4, 'Fresh Foods',      'Italy',           '80121', 'NA');

