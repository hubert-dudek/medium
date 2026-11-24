# Databricks notebook source
stories = {
    "Apps": ["Deployment history", "App Builder"],
    "AI/ML": ["Genie CLI", "Agent memory"],
    "Data Engineering": ["Materialized views", "JDBC connections"],
}

topics = "\n".join(f"{topic}: {', '.join(titles)}" for topic, titles in stories.items())
print(
    "Build a Databricks News Tracker with six cards. Give each card a title, "
    "a one-sentence summary and a topic. Use the following inline data:\n" + topics
)

# COMMAND ----------

print(
    "Add an All button and one filter button per topic. Start with all six stories. "
    "Selecting a topic should display its two stories and update the visible count."
)
