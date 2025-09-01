-- Databricks notebook source
-- MAGIC %md
-- MAGIC 17.1 brings geospatial support in Delta. 2 new data types - GEOGRAPHY and GEOMETRY and dozens of functions.

-- COMMAND ----------

-- DBTITLE 1,cities
CREATE OR REPLACE TEMP VIEW cities_geo AS
SELECT
  name,
  ST_GEOMFROMTEXT(wkt, 4326) AS boundary_geog, --  4326: Spatial Reference System Identifier
  ST_ASGEOJSON(boundary_geog) AS geo_json
FROM VALUES
('New York (Manhattan S)', 'POLYGON((-74.0180 40.7050,-74.0175 40.7400,-74.0060 40.7480,-73.9900 40.7480,-73.9750 40.7350,-73.9800 40.7050,-74.0180 40.7050))'),
('Jersey City', 'POLYGON((-74.1070 40.7050,-74.0950 40.7350,-74.0750 40.7400,-74.0550 40.7350,-74.0450 40.7150,-74.0600 40.6950,-74.0850 40.6950,-74.1070 40.7050))'),
('Hoboken', 'POLYGON((-74.0480 40.7350,-74.0465 40.7550,-74.0200 40.7550,-74.0200 40.7400,-74.0300 40.7350,-74.0480 40.7350))'),
('Newark', 'POLYGON((-74.2100 40.7100,-74.2000 40.7550,-74.1650 40.7550,-74.1450 40.7350,-74.1550 40.7100,-74.1850 40.7000,-74.2100 40.7100))'),
('Brooklyn (Central)',     'POLYGON((-73.9700 40.7050,-73.9650 40.7000,-73.9500 40.6950,-73.9300 40.6950,-73.9100 40.6850,-73.9050 40.6700,-73.9200 40.6600,-73.9450 40.6550,-73.9650 40.6650,-73.9750 40.6850,-73.9700 40.7050))'),
('Queens (LIC/Astoria)', 'POLYGON((-73.9550 40.7350,-73.9550 40.7600,-73.9400 40.7800,-73.9100 40.7800,-73.8950 40.7600,-73.9050 40.7400,-73.9250 40.7350,-73.9550 40.7350))')
AS t(name, wkt);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_cities = spark.table("cities_geo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_cities)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_cities.write.saveAsTable('cities_geo', mode="overwrite")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC import plotly.express as px
-- MAGIC
-- MAGIC # pull strings + a simple id
-- MAGIC pdf = df_cities.selectExpr("name", "cast(geo_json as string) as g").toPandas()
-- MAGIC
-- MAGIC # build FeatureCollection
-- MAGIC features = [
-- MAGIC     {"type": "Feature",
-- MAGIC      "geometry": json.loads(s),
-- MAGIC      "properties": {"id": i, "name": n}}
-- MAGIC     for i, (s, n) in enumerate(zip(pdf["g"], pdf["name"]))
-- MAGIC ]
-- MAGIC fc = {"type": "FeatureCollection", "features": features}
-- MAGIC
-- MAGIC # center on the FIRST feature (so you actually land on a city)
-- MAGIC ring0 = fc["features"][0]["geometry"]["coordinates"][0]
-- MAGIC center = {"lon": sum(p[0] for p in ring0)/len(ring0),
-- MAGIC           "lat": sum(p[1] for p in ring0)/len(ring0)}
-- MAGIC
-- MAGIC fig = px.choropleth_mapbox(
-- MAGIC     pdf.assign(id=range(len(pdf))),
-- MAGIC     geojson=fc,
-- MAGIC     locations="name",
-- MAGIC     featureidkey="properties.name",
-- MAGIC     color="id",
-- MAGIC     mapbox_style="open-street-map",
-- MAGIC     center=center,
-- MAGIC     zoom=10,
-- MAGIC     opacity=0.6
-- MAGIC )
-- MAGIC
-- MAGIC # use displayHTML to get full interactivity in Databricks
-- MAGIC displayHTML(fig.to_html(include_plotlyjs='cdn', full_html=False))
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can see new GEOMETRY and GEOGRAPHY type in Unity Catalog.

-- COMMAND ----------

-- DBTITLE 1,deliveries
CREATE OR REPLACE TEMP VIEW deliveries_geo AS
SELECT
  CAST(id AS INT) AS id,
  CAST(lon AS DOUBLE) AS lon,
  CAST(lat AS DOUBLE) AS lat,
  ST_POINT(lon, lat, 4326) AS pt_geog -- GEOGRAPHY data type
  --TO_GEOGRAPHY(ST_ASWKT(pt_geog)) AS geog
FROM
  VALUES
    (1, - 74.0080, 40.7130),
    (2, - 74.0005, 40.7305),
    (3, - 74.0710, 40.7220),
    (4, - 74.0600, 40.7080),
    (5, - 74.0320, 40.7450),
    (6, - 74.0250, 40.7485),
    (7, - 74.1800, 40.7350),
    (8, - 74.1950, 40.7180),
    (9, - 73.9550, 40.6850),
    (10, - 73.9400, 40.6680),
    (11, - 73.9300, 40.7550),
    (12, - 73.9150, 40.7700) AS t (id, lon, lat);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_deliveries = spark.table("deliveries_geo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import plotly.express as px
-- MAGIC
-- MAGIC pdf_pts = df_deliveries.toPandas()
-- MAGIC
-- MAGIC fig_deliveries = px.scatter_mapbox(
-- MAGIC     pdf_pts,
-- MAGIC     lon="lon",
-- MAGIC     lat="lat",
-- MAGIC     hover_name="id",
-- MAGIC     mapbox_style="open-street-map",
-- MAGIC     zoom=10
-- MAGIC )
-- MAGIC
-- MAGIC # Force larger fixed marker size
-- MAGIC fig_deliveries.update_traces(
-- MAGIC     marker=dict(size=16, sizemode="diameter", color="red")   # increase to desired size
-- MAGIC )
-- MAGIC
-- MAGIC displayHTML(fig_deliveries.to_html(include_plotlyjs="cdn", full_html=False))

-- COMMAND ----------

-- DBTITLE 1,cities and deliveries
-- MAGIC %python
-- MAGIC import json
-- MAGIC import plotly.express as px
-- MAGIC
-- MAGIC # cities polygon
-- MAGIC pdf_cities = df_cities.toPandas()
-- MAGIC # delivery points
-- MAGIC pdf_pts = df_deliveries.toPandas()
-- MAGIC
-- MAGIC # Build FeatureCollection for polygons
-- MAGIC features = [
-- MAGIC     {"type": "Feature",
-- MAGIC      "geometry": json.loads(s),
-- MAGIC      "properties": {"name": n}}
-- MAGIC     for n, s in zip(pdf_cities["name"], pdf_cities["geo_json"])
-- MAGIC ]
-- MAGIC fc = {"type": "FeatureCollection", "features": features}
-- MAGIC
-- MAGIC # Center on the first polygon
-- MAGIC ring0 = fc["features"][0]["geometry"]["coordinates"][0]
-- MAGIC center = {"lon": sum(p[0] for p in ring0)/len(ring0),
-- MAGIC           "lat": sum(p[1] for p in ring0)/len(ring0)}
-- MAGIC
-- MAGIC # Base map with polygons
-- MAGIC fig = px.choropleth_mapbox(
-- MAGIC     pdf_cities.assign(loc=pdf_cities["name"]),
-- MAGIC     geojson=fc,
-- MAGIC     locations="loc",
-- MAGIC     featureidkey="properties.name",
-- MAGIC     color="loc",
-- MAGIC     mapbox_style="open-street-map",
-- MAGIC     center=center,
-- MAGIC     zoom=10,
-- MAGIC     opacity=0.55
-- MAGIC )
-- MAGIC
-- MAGIC
-- MAGIC # Add delivery points on top
-- MAGIC fig.add_scattermapbox(
-- MAGIC     lon=pdf_pts["lon"],
-- MAGIC     lat=pdf_pts["lat"],
-- MAGIC     mode="markers",
-- MAGIC     text=pdf_pts["id"].astype(str),
-- MAGIC     name="Deliveries",
-- MAGIC     marker=dict(
-- MAGIC         size=16,
-- MAGIC         sizemode="diameter",
-- MAGIC         color="red"
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC # Render in Databricks
-- MAGIC displayHTML(fig.to_html(include_plotlyjs="cdn", full_html=False))
-- MAGIC

-- COMMAND ----------

SELECT
  d.id AS delivery_id,
  c.name AS city_name
FROM
  deliveries_geo AS d
    LEFT JOIN cities_geo AS c
      ON st_contains(c.boundary_geog, d.pt_geog);

-- COMMAND ----------

-- DBTITLE 1,distance between city centeres
SELECT c1.name AS city1,
       c2.name AS city2,
       ROUND( st_distancespheroid( st_centroid(c1.boundary_geog), 
                                   st_centroid(c2.boundary_geog) ) / 1000 , 1 ) AS distance_km
FROM cities_geo AS c1
CROSS JOIN cities_geo AS c2
WHERE c1.name < c2.name

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE starting_id INT DEFAULT 1;

WITH RECURSIVE mst AS (
  -- anchor: start with id of our choice
  SELECT
    1 AS step,
    array(starting_id) AS visited,
    CAST(NULL AS INT) AS from_id,
    min(id) AS to_id,
    CAST(0.0 AS DOUBLE) AS edge_km,
    CAST(0.0 AS DOUBLE) AS total_km
  FROM deliveries_geo

  UNION ALL

  -- step: among all edges from visited -> unvisited, pick the shortest
  SELECT step, visited, from_id, to_id, edge_km, total_km
  FROM (
    SELECT
      t.step + 1 AS step,
      concat(t.visited, array(v.id)) AS visited,
      u.id AS from_id,
      v.id AS to_id,
      ST_DISTANCESPHEROID(u.pt_geog, v.pt_geog) / 1000 AS edge_km,
      t.total_km + ST_DISTANCESPHEROID(u.pt_geog, v.pt_geog) / 1000 AS total_km,
      ROW_NUMBER() OVER (
        PARTITION BY t.step
        ORDER BY ST_DISTANCESPHEROID(u.pt_geog, v.pt_geog)
      ) AS rn
    FROM mst t
    JOIN deliveries_geo u
      ON array_contains(t.visited, u.id)           -- already in tree
    JOIN deliveries_geo v
      ON NOT array_contains(t.visited, v.id)       -- not yet in tree
    WHERE t.step < (SELECT COUNT(*) FROM deliveries_geo)
  ) ranked
  WHERE rn = 1
)
SELECT
  step,
  from_id,
  to_id,
  edge_km,
  total_km,
  visited AS path_ids
FROM mst
ORDER BY step;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import plotly.graph_objects as go
-- MAGIC
-- MAGIC path_ids = [1, 2, 6, 5, 3, 4, 9, 10, 11, 12, 7, 8]
-- MAGIC
-- MAGIC pdf_route = (
-- MAGIC     df_deliveries
-- MAGIC       .selectExpr("CAST(id AS INT) id", "CAST(lon AS DOUBLE) lon", "CAST(lat AS DOUBLE) lat")
-- MAGIC       .toPandas()
-- MAGIC )
-- MAGIC pdf_route = pdf_route.set_index("id").loc[path_ids].reset_index()
-- MAGIC
-- MAGIC fig = go.Figure()
-- MAGIC
-- MAGIC # Route line
-- MAGIC fig.add_trace(go.Scattermapbox(
-- MAGIC     lon=pdf_route["lon"], lat=pdf_route["lat"],
-- MAGIC     mode="lines", name="Route",
-- MAGIC     line=dict(width=4, color="blue")
-- MAGIC ))
-- MAGIC
-- MAGIC # White halo UNDERLAY (fake outline)
-- MAGIC fig.add_trace(go.Scattermapbox(
-- MAGIC     lon=pdf_route["lon"], lat=pdf_route["lat"],
-- MAGIC     mode="markers",
-- MAGIC     marker=dict(size=30, sizemode="diameter", color="white", opacity=1.0),
-- MAGIC     hoverinfo="skip", showlegend=False
-- MAGIC ))
-- MAGIC
-- MAGIC # Red markers + big labels
-- MAGIC fig.add_trace(go.Scattermapbox(
-- MAGIC     lon=pdf_route["lon"], lat=pdf_route["lat"],
-- MAGIC     mode="markers+text",
-- MAGIC     text=pdf_route["id"].astype(str),
-- MAGIC     textposition="middle center",
-- MAGIC     textfont=dict(size=18, color="black"),   # black on white halo is crisp
-- MAGIC     name="Stops",
-- MAGIC     marker=dict(size=24, sizemode="diameter", color="red", opacity=1.0)
-- MAGIC ))
-- MAGIC
-- MAGIC fig.update_layout(
-- MAGIC     mapbox_style="open-street-map",
-- MAGIC     mapbox_zoom=11,
-- MAGIC     mapbox_center={"lon": pdf_route["lon"].mean(), "lat": pdf_route["lat"].mean()},
-- MAGIC     margin={"r":0,"t":0,"l":0,"b":0}
-- MAGIC )
-- MAGIC
-- MAGIC displayHTML(fig.to_html(include_plotlyjs="cdn", full_html=False))
-- MAGIC
