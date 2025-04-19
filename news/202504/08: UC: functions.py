# Databricks notebook source
CREATE OR REPLACE FUNCTION main.samples.maget_snow_white_imdb_rating()
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["IMDbPY==2023.05.*"]',
  environment_version = 'None'
)
AS $$
import imdb

def fetch_rating():
    # Create an IMDb instance
    ia = imdb.IMDb()
    
    # "Snow White has the IMDb ID "tt6208148"
    movie = ia.get_movie('6208148')
    
    # Attempt to retrieve the 'rating' field
    rating = movie.get('rating')
    if rating is not None:
        return str(rating)
    else:
        return "No rating found"

return fetch_rating()
$$;


# COMMAND ----------

main.samples.maget_snow_white_imdb_rating()
