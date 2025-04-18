-- Databricks notebook source
CREATE SCHEMA main.samples;

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
