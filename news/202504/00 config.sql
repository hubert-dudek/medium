-- Databricks notebook source
CREATE SCHEMA main.samples;

-- COMMAND ----------

CREATE TABLE main.samples.sales (
  transaction_id INT,
  transaction_date DATE,
  amount DECIMAL(10, 2),
  description STRING
);

INSERT INTO main.samples.sales (transaction_id, transaction_date, amount, description) VALUES
  (1, '2025-04-01', 100.00, 'Sample transaction 1'),
  (2, '2025-04-01', 150.00, 'Sample transaction 2'),
  (3, '2025-04-01', 200.00, 'Sample transaction 3'),
  (4, '2025-04-02', 250.00, 'Sample transaction 4'),
  (5, '2025-04-02', 300.00, 'Sample transaction 5'),
  (6, '2025-04-02', 350.00, 'Sample transaction 6'),
  (7, '2025-04-03', 400.00, 'Sample transaction 7'),
  (8, '2025-04-03', 450.00, 'Sample transaction 8'),
  (9, '2025-04-03', 500.00, 'Sample transaction 9'),
  (10, '2025-04-04', 550.00, 'Sample transaction 10'),
  (11, '2025-04-04', 600.00, 'Sample transaction 11'),
  (12, '2025-04-04', 650.00, 'Sample transaction 12'),
  (13, '2025-04-05', 700.00, 'Sample transaction 13'),
  (14, '2025-04-05', 750.00, 'Sample transaction 14'),
  (15, '2025-04-05', 800.00, 'Sample transaction 15'),
  (16, '2025-04-06', 850.00, 'Sample transaction 16'),
  (17, '2025-04-06', 900.00, 'Sample transaction 17'),
  (18, '2025-04-06', 950.00, 'Sample transaction 18'),
  (19, '2025-04-07', 1000.00, 'Sample transaction 19'),
  (20, '2025-04-07', 1050.00, 'Sample transaction 20'),
  (21, '2025-04-07', 1100.00, 'Sample transaction 21'),
  (22, '2025-04-08', 1150.00, 'Sample transaction 22'),
  (23, '2025-04-08', 1200.00, 'Sample transaction 23'),
  (24, '2025-04-08', 1250.00, 'Sample transaction 24'),
  (25, '2025-04-09', 1300.00, 'Sample transaction 25'),
  (26, '2025-04-09', 1350.00, 'Sample transaction 26'),
  (27, '2025-04-09', 1400.00, 'Sample transaction 27'),
  (28, '2025-04-10', 1450.00, 'Sample transaction 28'),
  (29, '2025-04-10', 1500.00, 'Sample transaction 29'),
  (30, '2025-04-10', 1550.00, 'Sample transaction 30');

-- COMMAND ----------

CREATE VOLUME main.samples.invoices;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC def generate_sample_invoices(save_dir="/Volumes/main/samples/invoices"):
-- MAGIC     # Ensure the output directory exists
-- MAGIC     os.makedirs(save_dir, exist_ok=True)
-- MAGIC     
-- MAGIC     # Define a few sample invoices as text
-- MAGIC     # (You can add more details or generate them programmatically if needed)
-- MAGIC     invoice_texts = [
-- MAGIC         {
-- MAGIC             "filename": "invoice_01.txt",
-- MAGIC             "content": """\
-- MAGIC Invoice Number: INV-2025-001
-- MAGIC Invoice Date: 2025-04-01
-- MAGIC
-- MAGIC Seller:
-- MAGIC   Name: ABC Supplies
-- MAGIC   Address: 123 Main St, Springfield, USA
-- MAGIC
-- MAGIC Buyer:
-- MAGIC   Name: XYZ Retail
-- MAGIC   Address: 987 Market Ave, Metropolis, USA
-- MAGIC
-- MAGIC Items:
-- MAGIC   1) Laptop (Quantity: 10, Unit Price: 799.99)
-- MAGIC   2) Docking Station (Quantity: 5, Unit Price: 149.99)
-- MAGIC
-- MAGIC Subtotal: 9499.85
-- MAGIC Tax: 760.00
-- MAGIC Total Due: 10259.85
-- MAGIC Payment Terms: Net 30
-- MAGIC """
-- MAGIC         },
-- MAGIC         {
-- MAGIC             "filename": "invoice_02.txt",
-- MAGIC             "content": """\
-- MAGIC Invoice Number: INV-2025-002
-- MAGIC Invoice Date: 2025-04-15
-- MAGIC
-- MAGIC Seller:
-- MAGIC   Name: Global Office Supplies
-- MAGIC   Address: 456 Industrial Way, Shelbyville, USA
-- MAGIC
-- MAGIC Buyer:
-- MAGIC   Name: J. Smith Consulting
-- MAGIC   Address: 321 North Ave, Capital City, USA
-- MAGIC
-- MAGIC Items:
-- MAGIC   1) Paper Reams (Quantity: 20, Unit Price: 5.49)
-- MAGIC   2) Ink Cartridges (Quantity: 10, Unit Price: 29.99)
-- MAGIC
-- MAGIC Subtotal: 559.80
-- MAGIC Tax: 44.78
-- MAGIC Total Due: 604.58
-- MAGIC Payment Terms: Net 15
-- MAGIC """
-- MAGIC         },
-- MAGIC         {
-- MAGIC             "filename": "invoice_03.txt",
-- MAGIC             "content": """\
-- MAGIC Invoice Number: INV-2025-003
-- MAGIC Invoice Date: 2025-04-20
-- MAGIC
-- MAGIC Seller:
-- MAGIC   Name: OfficeMart
-- MAGIC   Address: 789 Commerce Blvd, Springfield, USA
-- MAGIC
-- MAGIC Buyer:
-- MAGIC   Name: Johnson & Co.
-- MAGIC   Address: 555 Elm St, Ogdenville, USA
-- MAGIC
-- MAGIC Items:
-- MAGIC   1) Desk Chairs (Quantity: 4, Unit Price: 120.00)
-- MAGIC   2) Ergonomic Keyboards (Quantity: 4, Unit Price: 65.00)
-- MAGIC
-- MAGIC Subtotal: 740.00
-- MAGIC Tax: 59.20
-- MAGIC Total Due: 799.20
-- MAGIC Payment Terms: Net 30
-- MAGIC """
-- MAGIC         }
-- MAGIC     ]
-- MAGIC
-- MAGIC     # Write the sample invoices to text files
-- MAGIC     for invoice in invoice_texts:
-- MAGIC         file_path = os.path.join(save_dir, invoice["filename"])
-- MAGIC         with open(file_path, "w", encoding="utf-8") as f:
-- MAGIC             f.write(invoice["content"])
-- MAGIC         print(f"Saved {invoice['filename']} to {file_path}")
-- MAGIC
-- MAGIC
-- MAGIC generate_sample_invoices()
-- MAGIC
