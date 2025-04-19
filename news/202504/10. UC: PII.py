# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.samples.persons (
# MAGIC   person_id INT,
# MAGIC   full_name STRING,
# MAGIC   email_address STRING,
# MAGIC   contact_number STRING,
# MAGIC   city STRING,
# MAGIC   pesel STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO main.samples.persons
# MAGIC   (person_id, full_name, email_address, contact_number, city, pesel)
# MAGIC VALUES
# MAGIC   (1, 'Jan Kowalski', 'jan.kowalski@example.pl', '123-456-789', 'Warszawa', '12345678901'),
# MAGIC   (2, 'Anna Nowak', 'anna.nowak@example.pl', '987-654-321', 'Warszawa', '98765432109'),
# MAGIC   (3, 'Piotr Wiśniewski', 'piotr.wisniewski@example.pl', '555-555-555', 'Kraków',  '55555555555');
# MAGIC
# MAGIC
