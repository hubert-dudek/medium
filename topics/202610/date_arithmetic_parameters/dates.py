from datetime import date, timedelta

base_date = dbutils.widgets.get("base_date")
prior_week = dbutils.widgets.get("prior_week")
print(f"base_date: {base_date}")
print(f"prior_week: {prior_week}")
assert date.fromisoformat(prior_week) == date.fromisoformat(base_date) - timedelta(weeks=1)
