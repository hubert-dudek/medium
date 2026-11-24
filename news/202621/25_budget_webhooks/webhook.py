# Databricks notebook source
sample_event = {
    "event_type": "budgets.on_threshold_exceeded",
    "event_id": "sample-event-1",
    "budget": {"budget_configuration_id": "sample-budget", "display_name": "AI usage"},
    "evaluation": {"quantity_threshold": "10.00", "current_spend": "10.20"},
}
print(sample_event)

# COMMAND ----------

seen = set()
for event in [sample_event, sample_event]:
    if event["event_id"] in seen:
        print("Duplicate delivery:", event["event_id"])
        continue
    seen.add(event["event_id"])
    spend = event["evaluation"]["current_spend"]
    threshold = event["evaluation"]["quantity_threshold"]
    print(f'{event["budget"]["display_name"]}: USD {spend} spent against USD {threshold}')
