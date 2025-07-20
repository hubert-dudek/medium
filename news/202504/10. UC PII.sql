-- Databricks notebook source
CREATE TABLE IF NOT EXISTS main.samples.persons (
  person_id INT,
  full_name STRING,
  email_address STRING,
  contact_number STRING,
  city STRING,
  pesel STRING
);

INSERT INTO main.samples.persons
  (person_id, full_name, email_address, contact_number, city, pesel)
VALUES
  (1, 'Jan Kowalski', 'jan.kowalski@example.pl', '123-456-789', 'Warszawa', '12345678901'),
  (2, 'Anna Nowak', 'anna.nowak@example.pl', '987-654-321', 'Warszawa', '98765432109'),
  (3, 'Piotr Wiśniewski', 'piotr.wisniewski@example.pl', '555-555-555', 'Kraków',  '55555555555');



-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.samples.us_persons (
  person_id INT,
  full_name STRING,
  email_address STRING,
  contact_number STRING,
  city STRING,
  ssn STRING
);

INSERT INTO main.samples.us_persons
  (person_id, full_name, email_address, contact_number, city, ssn)
VALUES
  (1, 'John Smith', 'john.smith@example.com', '123-456-7890', 'New York', '123-45-6789'),
  (2, 'Jane Doe', 'jane.doe@example.com', '987-654-3210', 'Los Angeles', '987-65-4321'),
  (3, 'Michael Johnson', 'michael.johnson@example.com', '555-555-5555', 'Chicago', '555-55-5555');
