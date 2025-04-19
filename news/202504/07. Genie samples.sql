-- Databricks notebook source
CREATE TABLE main.samples.companies (
    id INT PRIMARY KEY,
    company_name VARCHAR(100),
    country VARCHAR(50),
    post_code VARCHAR(20),
    province_code VARCHAR(5),
    city VARCHAR(50)
);

INSERT INTO main.samples.companies (id, company_name, country, post_code, province_code, city)
VALUES
(1, 'Verona Best Wines',       'Italy',           '37100', 'VR', 'VR'),
(2, 'Milano Fashion Group',    'Italy',           '20121', 'MI', 'MI'),
(3, 'ROMA Tech Ltd.',          'Italy',           '00100', 'RM', 'RM'),
(4, 'Napoli Fresh Foods',      'Italy',           '80121', 'NA', 'NA');

