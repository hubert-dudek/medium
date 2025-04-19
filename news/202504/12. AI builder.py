# Databricks notebook source
#  example schema to extract for invoices

{
  "invoice_number": "INV-2025-001",
  "invoice_date": "2025-04-01",
  "seller": {
    "name": "ABC Supplies",
    "address": "123 Main St, Springfield, USA"
  },
  "buyer": {
    "name": "XYZ Retail",
    "address": "987 Market Ave, Metropolis, USA"
  },
  "items": [
    {
      "description": "Laptop",
      "quantity": 10,
      "unit_price": 799.99
    },
    {
      "description": "Docking Station",
      "quantity": 5,
      "unit_price": 149.99
    }
  ],
  "subtotal": 9499.85,
  "tax": 760.00,
  "total_due": 10259.85,
  "payment_terms": "Net 30"
}


# COMMAND ----------

# MAGIC %md
# MAGIC fine-tuning - model specialization
# MAGIC
# MAGIC knowledge about elixirs 
# MAGIC
# MAGIC Cat’s Gaze Elixir
# MAGIC
# MAGIC     Effect: Bestows heightened night vision and swift reflexes.
# MAGIC
# MAGIC     Ingredients: Strigo Root (C₇H₆O), Varin’s Ether (C₄H₁₀O), and secret “Monster Essence.”
# MAGIC
# MAGIC     Warning: May cause glowing pupils and mild sensitivity to sunlight.
# MAGIC
# MAGIC Griffin’s Might Potion
# MAGIC
# MAGIC     Effect: Temporarily enhances strength and stamina for intense battles.
# MAGIC
# MAGIC     Ingredients: Pulverized Griffin Claw, Heart of Oxbane (C₈H₁₀N₄O₂, jokingly referencing caffeine), and trace minerals.
# MAGIC
# MAGIC     Warning: Excessive use can lead to muscle cramps and insatiable appetite.
# MAGIC
# MAGIC Basilisk’s Ward Draught
# MAGIC
# MAGIC     Effect: Grants resistance to poisons and mild protection against petrification spells.
# MAGIC
# MAGIC     Ingredients: Basilisk Scale Powder (fortified with C₆H₅OH), an herbal base of Wolfsbane, and a pinch of powdered silver.
# MAGIC
# MAGIC     Warning: Overindulgence may result in brief paralysis or oddly reptilian eye changes.
