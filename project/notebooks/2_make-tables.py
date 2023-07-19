# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # 2. Make tables

# %% [markdown]
# **This file is used to prepare the two final tables that will be used for the project.  
# The first table "proba_models_table" is based on the sbb data and will then be used to build the model to predict the probabilities of delay and therefore of failure of the path.  
# The second "graph_table" is based on timetable data and will then be used to find the shortest path.**

# %% [markdown]
# ## 2.1. Setup
#
# ### Hive initialization

# %%
import os

username = os.environ['RENKU_USERNAME']
hiveaddr = os.environ['HIVE_SERVER2']
(hivehost,hiveport) = hiveaddr.split(':')
print("Operating as: {0}".format(username))

# %%
from pyhive import hive

# create connection
conn = hive.connect(host=hivehost, port=hiveport, username=username)

# create cursor
cur = conn.cursor()

# %% [markdown]
# ### Import packages

# %%
# Import
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

# %% [markdown]
# ### Import data

# %% [markdown]
# **sbb data:**

# %%
# Import istdaten data
# => See in prepare-env_project_LSDS

# Visualize data
pd.read_sql(f'SELECT * FROM {username}.sbb LIMIT 5', conn)

# %% [markdown]
# **timetable data:**

# %%
# Import allstops.orc
# => See in prepare-env_project_LSDS

# Visualize data
pd.read_sql(f'SELECT * FROM {username}.stops LIMIT 5', conn)

# %%
# Import stop_times.txt
# => See in prepare-env_project_LSDS

# Visualize data
pd.read_sql(f'SELECT * FROM {username}.stop_times LIMIT 5', conn)

# %%
# Import trips.txt
# => See in prepare-env_project_LSDS

# Visualize data
pd.read_sql(f'SELECT * FROM {username}.trips LIMIT 5', conn)

# %%
# Import calendar.txt
# => See in prepare-env_project_LSDS

# Visualize data
pd.read_sql(f'SELECT * FROM {username}.calendar LIMIT 5', conn)

# %%
# Import routes.txt
# => See in prepare-env_project_LSDS

# Visualize data
pd.read_sql(f'SELECT * FROM {username}.routes LIMIT 5', conn)

# %% [markdown]
# ## 2.2. Pre-processing and merge

# %%
# Filter stops onsider only departure and arrival stops in a 15km radius of Zürich's train station
# Zürich HB (8503000), (lat, lon) = (47.378177, 8.540192)

cur.execute("""DROP TABLE IF EXISTS {0}.stops_filtered""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.stops_filtered
        STORED AS ORC
        AS
            SELECT dist.stop_id, dist.stop_name, dist.stop_lat, dist.stop_lon, dist.location_type, dist.parent_station, dist.distance_km
            FROM(
                SELECT
                  stop_id,
                  stop_name,
                  stop_lat,
                  stop_lon,
                  location_type,
                  parent_station,
                  2 * asin(
                      sqrt(
                        cos(radians(52.2909264998)) *
                        cos(radians(52.3773759354)) *
                        pow(sin(radians((8.540192 - stop_lon)/2)), 2)
                            +
                        pow(sin(radians((47.378177 - stop_lat)/2)), 2)
                      )
                    ) * 6371 AS distance_km
                FROM {0}.stops
                ) AS dist
            WHERE dist.distance_km < 15
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.stops_filtered LIMIT 5', conn)

# %%
# Put in pandas
stops = pd.read_sql(f'SELECT * FROM {username}.stops_filtered', conn)
stops.head()

# %%
# Save
stops.to_csv('../data/stops.csv', index=False)

# %%
# Filter sbb onsider only departure and arrival stops in a 15km radius of Zürich's train station
# And if the vehicule do the stop

cur.execute("""DROP TABLE IF EXISTS {0}.sbb_filtered""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.sbb_filtered
        STORED AS ORC
        AS(
            SELECT
                ssb_tmp.betriebstag AS date_trip,
                ssb_tmp.year AS year,
                ssb_tmp.month AS month,
                ssb_tmp.fahrt_bezichner AS trip_id,
                ssb_tmp.produkt_id AS vehicule,
                ssb_tmp.linien_id AS linien_id,
                ssb_tmp.linien_text AS linien_text,
                ssb_tmp.zusatzfahrt_tf AS additional_trip,
                ssb_tmp.faellt_aus_tf AS failure,
                ssb_tmp.haltestellen_name AS stop_name,
                ssb_tmp.ankunftszeit AS arrival_time_shedule,
                ssb_tmp.an_prognose AS arrival_time_actual,
                ssb_tmp.abfahrtszeit AS departure_time_shedule,
                ssb_tmp.ab_prognose AS departure_time_actual,
                date_format(from_unixtime(unix_timestamp(ssb_tmp.betriebstag, 'dd.MM.yyyy')), 'EEEE') AS day_name,
                stops_filtered.stop_id AS stop_id,
                stops_filtered.stop_lat AS stop_lat,
                stops_filtered.stop_lon AS stop_lon,
                stops_filtered.location_type AS location_type,
                stops_filtered.parent_station AS parent_station,
                stops_filtered.distance_km AS distance_km
            FROM (SELECT * FROM {0}.sbb WHERE sbb.durchfahrt_tf = 'false') AS ssb_tmp
            INNER JOIN {0}.stops_filtered
            ON ssb_tmp.haltestellen_name == stops_filtered.stop_name
        )
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.sbb_filtered LIMIT 5', conn)

# %%
# Add infos from stop_times.txt in stops_filtered

cur.execute("""DROP TABLE IF EXISTS {0}.stops_stop_times""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.stops_stop_times
        STORED AS ORC
        AS(
            SELECT
                stops_filtered.stop_id AS stop_id,
                stops_filtered.stop_name AS stop_name,
                stops_filtered.stop_lat AS stop_lat,
                stops_filtered.stop_lon AS stop_lon,
                stops_filtered.location_type AS location_type,
                stops_filtered.parent_station AS parent_station,
                stop_times.trip_id AS trip_id,
                stop_times.arrival_time AS arrival_time,
                stop_times.departure_time AS departure_time,
                stop_times.stop_sequence AS stop_sequence,
                stop_times.pickup_type AS pickup_type,
                stop_times.drop_off_type AS drop_off_type
            FROM {0}.stops_filtered
            INNER JOIN {0}.stop_times
            ON stops_filtered.stop_id == stop_times.stop_id
        )
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.stops_stop_times LIMIT 5', conn)

# %%
# add infos from trips.txt in stops_stop_times

cur.execute("""DROP TABLE IF EXISTS {0}.stops_stop_times_trips""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.stops_stop_times_trips
        STORED AS ORC
        AS(
            SELECT
                stops_stop_times.stop_id AS stop_id,
                stops_stop_times.stop_name AS stop_name,
                stops_stop_times.stop_lat AS stop_lat,
                stops_stop_times.stop_lon AS stop_lon,
                stops_stop_times.location_type AS location_type,
                stops_stop_times.parent_station AS parent_station,
                stops_stop_times.trip_id AS trip_id,
                stops_stop_times.arrival_time AS arrival_time,
                stops_stop_times.departure_time AS departure_time,
                stops_stop_times.stop_sequence AS stop_sequence,
                stops_stop_times.pickup_type AS pickup_type,
                stops_stop_times.drop_off_type AS drop_off_type,
                trips.route_id AS route_id,
                trips.service_id AS service_id,
                trips.trip_headsign AS trip_headsign,
                trips.trip_short_name AS trip_short_name,
                trips.direction_id AS direction_id
            FROM {0}.stops_stop_times
            INNER JOIN {0}.trips
            ON stops_stop_times.trip_id == trips.trip_id
        )
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.stops_stop_times_trips LIMIT 5', conn)

# %%
# Add infos from calendar.txt in stops_stop_times_trips
# only keeps trips that have at least one day other than the weekend

cur.execute("""DROP TABLE IF EXISTS {0}.stops_stop_times_trips_calendar""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.stops_stop_times_trips_calendar
        STORED AS ORC
        AS(
            SELECT
                stops_stop_times_trips.stop_id AS stop_id,
                stops_stop_times_trips.stop_name AS stop_name,
                stops_stop_times_trips.stop_lat AS stop_lat,
                stops_stop_times_trips.stop_lon AS stop_lon,
                stops_stop_times_trips.location_type AS location_type,
                stops_stop_times_trips.parent_station AS parent_station,
                stops_stop_times_trips.trip_id AS trip_id,
                stops_stop_times_trips.arrival_time AS arrival_time,
                stops_stop_times_trips.departure_time AS departure_time,
                stops_stop_times_trips.stop_sequence AS stop_sequence,
                stops_stop_times_trips.pickup_type AS pickup_type,
                stops_stop_times_trips.drop_off_type AS drop_off_type,
                stops_stop_times_trips.route_id AS route_id,
                stops_stop_times_trips.service_id AS service_id,
                stops_stop_times_trips.trip_headsign AS trip_headsign,
                stops_stop_times_trips.trip_short_name AS trip_short_name,
                stops_stop_times_trips.direction_id AS direction_id,
                calendar.monday AS monday,
                calendar.tuesday AS tuesday,
                calendar.wednesday AS wednesday,
                calendar.thursday AS thursday,
                calendar.friday AS friday,
                calendar.saturday AS saturday,
                calendar.sunday AS sunday,
                calendar.start_date AS start_date,
                calendar.end_date AS end_date
            FROM {0}.stops_stop_times_trips
            INNER JOIN {0}.calendar 
            ON stops_stop_times_trips.service_id == calendar.service_id
        )
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.stops_stop_times_trips_calendar LIMIT 5', conn)

# %%
# Add infos from routes.txt in sbb

cur.execute("""DROP TABLE IF EXISTS {0}.stops_stop_times_trips_calendar_routes""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.stops_stop_times_trips_calendar_routes
        STORED AS ORC
        AS(
            SELECT
                stops_stop_times_trips_calendar.stop_id AS stop_id,
                stops_stop_times_trips_calendar.stop_name AS stop_name,
                stops_stop_times_trips_calendar.stop_lat AS stop_lat,
                stops_stop_times_trips_calendar.stop_lon AS stop_lon,
                stops_stop_times_trips_calendar.location_type AS location_type,
                stops_stop_times_trips_calendar.parent_station AS parent_station,
                stops_stop_times_trips_calendar.trip_id AS trip_id,
                stops_stop_times_trips_calendar.arrival_time AS arrival_time,
                stops_stop_times_trips_calendar.departure_time AS departure_time,
                stops_stop_times_trips_calendar.stop_sequence AS stop_sequence,
                stops_stop_times_trips_calendar.pickup_type AS pickup_type,
                stops_stop_times_trips_calendar.drop_off_type AS drop_off_type,
                stops_stop_times_trips_calendar.route_id AS route_id,
                stops_stop_times_trips_calendar.service_id AS service_id,
                stops_stop_times_trips_calendar.trip_headsign AS trip_headsign,
                stops_stop_times_trips_calendar.trip_short_name AS trip_short_name,
                stops_stop_times_trips_calendar.direction_id AS direction_id,
                stops_stop_times_trips_calendar.monday AS monday,
                stops_stop_times_trips_calendar.tuesday AS tuesday,
                stops_stop_times_trips_calendar.wednesday AS wednesday,
                stops_stop_times_trips_calendar.thursday AS thursday,
                stops_stop_times_trips_calendar.friday AS friday,
                stops_stop_times_trips_calendar.saturday AS saturday,
                stops_stop_times_trips_calendar.sunday AS sunday,
                stops_stop_times_trips_calendar.start_date AS start_date,
                stops_stop_times_trips_calendar.end_date AS end_date,
                routes.agency_id AS agency_id,
                routes.route_short_name AS route_short_name,
                routes.route_long_name AS route_long_name,
                routes.route_desc AS route_desc,
                routes.route_type AS route_type
            FROM {0}.stops_stop_times_trips_calendar
            INNER JOIN {0}.routes
            ON stops_stop_times_trips_calendar.route_id == routes.route_id
        )
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.stops_stop_times_trips_calendar_routes LIMIT 5', conn)

# %% [markdown]
# ## 2.3. Create Tables for graph and probability models

# %%
# Filter sbb_filtered to keep only years 2021 to 2023
# => This will be the table to construct our probability models => see file "3_model-delays-prediction"

cur.execute("""DROP TABLE IF EXISTS {0}.proba_models_table""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.proba_models_table
        STORED AS ORC
        AS(
            SELECT *
            FROM {0}.sbb_filtered
            WHERE sbb_filtered.year IN (2021, 2022,2023) 
        )
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.proba_models_table LIMIT 5', conn)

# %%
# => This will be the table to construct our shortest path => See file "4_shortest-paths"

cur.execute("""DROP TABLE IF EXISTS {0}.graph_table""".format(username))

query = """
        CREATE EXTERNAL TABLE {0}.graph_table
        STORED AS ORC
        AS(
            SELECT 
                stops_stop_times_trips_calendar_routes.stop_id AS stop_id,
                stops_stop_times_trips_calendar_routes.trip_id AS trip_id,
                stops_stop_times_trips_calendar_routes.arrival_time AS arrival_time,
                stops_stop_times_trips_calendar_routes.departure_time AS departure_time,
                stops_stop_times_trips_calendar_routes.stop_sequence AS stop_sequence,
                stops_stop_times_trips_calendar_routes.direction_id AS direction_id,
                stops_stop_times_trips_calendar_routes.route_desc AS vehicule,
                stops_stop_times_trips_calendar_routes.monday AS monday,
                stops_stop_times_trips_calendar_routes.tuesday AS tuesday,
                stops_stop_times_trips_calendar_routes.wednesday AS wednesday,
                stops_stop_times_trips_calendar_routes.thursday AS thursday,
                stops_stop_times_trips_calendar_routes.friday AS friday
            FROM {0}.stops_stop_times_trips_calendar_routes
            WHERE
              stops_stop_times_trips_calendar_routes.arrival_time BETWEEN '05:00:00' AND '23:00:00'
              AND
              stops_stop_times_trips_calendar_routes.departure_time BETWEEN '05:00:00' AND '23:00:00'
              AND
              (
                stops_stop_times_trips_calendar_routes.monday = '1'
                OR stops_stop_times_trips_calendar_routes.tuesday = '1'
                OR stops_stop_times_trips_calendar_routes.wednesday = '1'
                OR stops_stop_times_trips_calendar_routes.thursday = '1'
                OR stops_stop_times_trips_calendar_routes.friday = '1'
              )
          )
""".format(username)

cur.execute(query)

# %%
# Check data
pd.read_sql(f'SELECT * FROM {username}.graph_table LIMIT 5', conn)

# %%
# Put in pandas
table_graph = pd.read_sql(f'SELECT * FROM {username}.graph_table', conn)
table_graph.head()

# %%
# Save
table_graph.to_csv('../data/table_graph.csv', index=False)
