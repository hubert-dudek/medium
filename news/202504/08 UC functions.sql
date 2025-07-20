-- Databricks notebook source
CREATE OR REPLACE FUNCTION main.samples.maget_snow_white_imdb_rating()
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["cinemagoer==2023.5.1"]',
  environment_version = 'None'
)
COMMENT "get the rating of Snow White"
AS $$
import imdb

def fetch_rating():
  # Create an IMDb instance
  from imdb import Cinemagoer

  ia = Cinemagoer()
    
  # Snow White has the IMDb ID tt6208148
  movie = ia.get_movie('6208148')
  
  # Attempt to retrieve the rating field
  rating = movie.get('rating')
  if rating is not None:
      return str(rating)
  else:
      return "No rating found"

return fetch_rating()
$$;


-- COMMAND ----------

SELECT main.samples.maget_snow_white_imdb_rating() AS rating
