import os
import streamlit as st

# "yellow work" = a simple "Hello, World!" headline in yellow.
st.set_page_config(page_title="Yellow Hello World", page_icon="🟨", layout="centered")

catalog = os.getenv("CATALOG")

st.markdown("<h1 style='color: #f1c40f;'>Hello, World!</h1>", unsafe_allow_html=True)

if catalog:
    st.write("Catalog from env var **CATALOG**:")
    st.code(catalog)
else:
    st.warning(
        "CATALOG env var is not set. "
        "If you deployed via this bundle, check resources.apps.catalog_env_app.config.env in databricks.yml."
    )

# Also print to logs (useful when checking app logs in the Databricks UI).
print(f"CATALOG={catalog!r}")
