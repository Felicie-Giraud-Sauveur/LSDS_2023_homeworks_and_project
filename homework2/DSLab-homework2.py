# ---
# jupyter:
#   jupytext:
#     comment_magics: false
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
# # Homework 2 - Data Wrangling with Hadoop
#
#
# <div class="info alert-block alert-info">
# Before doing this exercise, you must have run <b>kinit</b> <em>your-gaspar-id</em> in a terminal.
# </div>
#
# ---
#
# The goal of this assignment is to put into action the data wrangling techniques from the exercises of module 2. We highly suggest you to finish these two exercises first and then start the homework. In this homework, we are going to reuse the same __sbb__ and __twitter__ datasets as seen before in these two exercises. 
#
# ## Hand-in Instructions
# - __Due: 04.04.2023 23:59 CET__
# - `git push` your final verion to your group's Renku repository before the due date
# - Verify that `Dockerfile`, `environment.yml` and `requirements.txt` are properly written and notebook is functional
# - Do not commit notebooks with the results, do a `Restart Kernel and Clear All Outputs...` first.
# - Add necessary comments and discussion to make your queries readable
#
# ## Hive Documentation
#
# Hive queries: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select>
#
# Hive functions: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF>

# %% [markdown]
# <div style="font-size: 100%" class="alert alert-block alert-warning">
#     <b>Get yourself ready:</b> 
#     <br>
#     Before you jump into the questions, you must have created an <em>EXTERNAL TABLE</em> <em>sbb_orc</em> in your database. Please first go through the notebook <a href='../notebooks/prepare-env.md'>prepare_env</a> and make sure that your environment is properly set up. The notebook includes the command needed to create the database. <b>DO NOT</b> modify the command.
#     <br><br>
#     <b>Cluster Usage:</b>
#     <br>
#     As there are many of you working with the cluster, we encourage you to prototype your queries on small data samples before running them on whole datasets.
#     <br><br>
#     <b>Try to use as much HiveQL as possible and avoid using pandas operations.</b>
# </div>

# %% [markdown]
# ## Part I: SBB/CFF/FFS Data (44 Points)
#
# Data source: <https://opentransportdata.swiss/en/dataset/istdaten>
#
# In this part, you will leverage Hive to perform exploratory analysis of data published by the [Open Data Platform Swiss Public Transport](https://opentransportdata.swiss).
#
# Format: the dataset is originally presented as a collection of textfiles with fields separated by ';' (semi-colon). For efficiency, the textfiles have been compressed into Optimized Row Columnar ([ORC](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC)) file format. 
#
# Location: you can find the data in ORC format on HDFS at the path `/data/sbb/part_orc/istdaten` and `/data/sbb/orc/allstops`. Your notebook [prepare-env](../notebooks/prepare-env.md) contains the command needed to create the external tables `sbb_orc` and `sbb_stops_orc`.
#
# The full description from opentransportdata.swiss can be found in <https://opentransportdata.swiss/de/cookbook/ist-daten/> in four languages. Because of the translation typos there may be some misunderstandings. We suggest you rely on the German version and use an automated translator when necessary. We will clarify if there is still anything unclear in class and Slack.
#
#
# Here are the relevant column descriptions:
#
# **Istdaten**
#
# - `BETRIEBSTAG`: date of the trip
# - `FAHRT_BEZEICHNER`: identifies the trip
# - `BETREIBER_ABK`, `BETREIBER_NAME`: operator (name will contain the full name, e.g. Schweizerische Bundesbahnen for SBB)
# - `PRODUKT_ID`: type of transport, e.g. train, bus
# - `LINIEN_ID`: for trains, this is the train number
# - `LINIEN_TEXT`,`VERKEHRSMITTEL_TEXT`: for trains, the service type (IC, IR, RE, etc.)
# - `ZUSATZFAHRT_TF`: boolean, true if this is an additional trip (not part of the regular schedule)
# - `FAELLT_AUS_TF`: boolean, true if this trip failed (cancelled or not completed)
# - `HALTESTELLEN_NAME`: name of the stop
# - `ANKUNFTSZEIT`: arrival time at the stop according to schedule
# - `AN_PROGNOSE`: actual arrival time
# - `AN_PROGNOSE_STATUS`: show how the actual arrival time is calcluated
# - `ABFAHRTSZEIT`: departure time at the stop according to schedule
# - `AB_PROGNOSE`: actual departure time
# - `AB_PROGNOSE_STATUS`: show how the actual departure time is calcluated
# - `DURCHFAHRT_TF`: boolean, true if the transport does not stop there
#
# Each line of the file represents a stop and contains arrival and departure times. When the stop is the start or end of a journey, the corresponding columns will be empty (`ANKUNFTSZEIT`/`ABFAHRTSZEIT`).
#
# In some cases, the actual times were not measured so the `AN_PROGNOSE_STATUS`/`AB_PROGNOSE_STATUS` will be empty or set to `PROGNOSE` and `AN_PROGNOSE`/`AB_PROGNOSE` will be empty.
#
# **Stations**
#
# - `STOP_ID`: numerical ID of the stop
# - `STOP_NAME`: actual name of the stop
# - `STOP_LAT`: the latitude (in WGS84 coordinates system) of the stop
# - `STOP_LON`: the longitude of the stop
# - `LOCATION_TYPE`: set to 1 (True) if the stop is a cluster of stops, such as a train station or a bus terminal.
# - `PARENT_STATION`: If the stop is part of a cluster, this is the `STOP_ID` of the cluster.
#
# A majority of `HALTESTELLEN_NAME` in the **Istdaten** data set have a matching `STOP_NAME` in **geostop**. However, this is often not always the case: the names are sometime spelled differently (e.g. abbreviations), or expressed in different languages. In many cases, the `STOP_ID` is even used to identify the stop in `HALTESTELLEN`. To complicate matters, character encodings were used throughout the years, and a stop name does not always compare the same under different encodings. Do your best, but do not expect a 100% success matching all the stops.
#

# %% [markdown]
# ---
# __Initialization__

# %%
import os
import pandas as pd
pd.set_option("display.max_columns", 50)
import matplotlib.pyplot as plt
import warnings
import plotly.express as px
import plotly.graph_objects as go
warnings.simplefilter(action='ignore', category=UserWarning)

# %%
%matplotlib inline

# %%
from pyhive import hive

# Set python variables from environment variables
username = os.environ['USERNAME']
hive_host = os.environ['HIVE_SERVER2'].split(':')[0]
hive_port = os.environ['HIVE_SERVER2'].split(':')[1]

# create connection
conn = hive.connect(
    host=hive_host,
    port=hive_port,
    # auth="KERBEROS",
    # kerberos_service_name = "hive"
)

# create cursor
cur = conn.cursor()

print(f"your username is {username}")
print(f"you are connected to {hive_host}:{hive_port}")

# %% [markdown]
# ### a) Type of transport - 10/40
#
# In earlier exercises, you have already explored the stop distribution of different types of transport. Now, let's do the same for the whole of 2022 and visualize it in a bar graph.
#
# - Query `sbb_orc` to get the total number of stops for different types of transport in each month, and order it by time and type of transport.
# |month_year|ttype|stops|
# |---|---|---|
# |...|...|...|
# - Use `plotly` to create a facet bar chart partitioned by the type of transportation. 
# - Document any patterns or abnormalities you can find.
#
# __Note__: 
# - In general, one entry in the `sbb_orc` table means one stop.
# - You might need to filter out the rows where:
#     - `BETRIEBSTAG` is not in the format of `__.__.____`
#     - `PRODUKT_ID` is NULL or empty
# - Facet _bar_ plot with plotly: https://plotly.com/python/facet-plots/ the monthly count of stops per transport mode as shown below:
#
# <img src="../notebooks/1a-example.png" alt="1a-example.png" width="400"/>

# %%
query = f"""
SELECT CONCAT(month,'_2022') AS month_year, LOWER(produkt_id) AS ttype, COUNT(*) AS stops
FROM {username}.sbb_orc AS expr
WHERE year = 2022 AND LENGTH(produkt_id) > 0
GROUP BY month, LOWER(produkt_id)
ORDER BY month, LOWER(produkt_id)
"""

df_ttype = pd.read_sql(query, conn)

# %%
fig = px.bar(
    df_ttype, x='month_year', y='stops', color='ttype',
    facet_col='ttype', facet_col_wrap=3, 
    facet_col_spacing=0.05, facet_row_spacing=0.2,
    labels={'month_year':'Month', 'stops':'#stops', 'ttype':'Type'},
    title='Monthly count of stops'
)
fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
fig.update_yaxes(matches=None, showticklabels=True)
fig.update_layout(showlegend=False)
fig.show()

# %% [markdown]
# ### b) Schedule - 10/40
#
# - Select any typical week day in the past 6 months (not Saturday, not Sunday, not a bank holiday) from `sbb_orc`. Query the one-day table and get the set of IC (`VERKEHRSMITTEL_TEXT`) trains you can take to go (without connections) from `Genève` to `Lausanne` on that day. 
# - Display the train number (`LINIEN_ID`) as well as the schedule (arrival and departure time) of the trains.
#
# |train_number|departure|arrival|
# |---|---|---|
# |...|...|...|
#
# __Note:__ 
# - The schedule of **IC** trains from Genève to Lausanne has not changed for the past few months. You can use the advanced search of SBB's website to check your answer.
# - Do not hesitate to create intermediary _Hive managed_ tables.
# - Take advantage of the schema of the table _sbb_orc_ to make the query as efficient as possible. 
# - You might need to add filters on these flags: `ZUSATZFAHRT_TF`, `FAELLT_AUS_TF`, `DURCHFAHRT_TF` 
# - Functions that could be useful (not necessarily all of them): `unix_timestamp`, `to_utc_timestamp`, `date_format`.

# %%
query = f"""
SELECT dep.train_number, dep.departure, arr.arrival
FROM (
    SELECT linien_id as train_number, abfahrtszeit as departure
    FROM {username}.sbb_orc
    WHERE betriebstag = '23.11.2022' AND verkehrsmittel_text = 'IC' AND LENGTH(abfahrtszeit) > 0 and haltestellen_name = 'Genève'
        AND zusatzfahrt_tf = 'false' AND durchfahrt_tf = 'false'
) AS dep
INNER JOIN (
    SELECT linien_id as train_number, ankunftszeit as arrival
    FROM {username}.sbb_orc
    WHERE betriebstag = '23.11.2022' AND verkehrsmittel_text = 'IC' AND LENGTH(ankunftszeit) > 0 and haltestellen_name = 'Lausanne'
        AND zusatzfahrt_tf = 'false' AND durchfahrt_tf = 'false'
) AS arr
ON dep.train_number = arr.train_number 
    AND unix_timestamp(dep.departure, 'dd.MM.yyyy HH:mm') < unix_timestamp(arr.arrival, 'dd.MM.yyyy HH:mm')
    AND (unix_timestamp(arr.arrival, 'dd.MM.yyyy HH:mm') - unix_timestamp(dep.departure, 'dd.MM.yyyy HH:mm')) < 43200
ORDER BY dep.departure
"""
df_schedule = pd.read_sql(query, conn)
df_schedule

# %% [markdown]
# ### c) Delay percentiles - 10/40
#
# - Query `sbb_orc` to compute the 50th and 75th percentiles of __arrival__ delays for the IC trains listed in the previous question (16 trains total) at Genève main station for the full year 2022. 
# - Use `plotly` to plot your results in a proper way. For each IC train find show two bars, corresponding to the 50th and 75th percentiles.
# - Which trains are the most disrupted? Can you find the tendency and interpret?
#
# __Note:__
# - Do not hesitate to create intermediary _Hive managed_ tables. 
# - When the train is ahead of schedule, count this as a delay of 0.
# - Only the delays at `Lausanne` train station of the trains found in the previous question.
# - You may use the `IN` condition to only select the train from a list.
# - Use only stops with `AN_PROGNOSE_STATUS` equal to __REAL__ or __GESCHAETZT__.
# - Functions that may be useful: `unix_timestamp`, `percentile_approx`, `if`

# %%
query = f"""
SELECT train_number, percentile_approx(delay, 0.5, 10000) as p_50, percentile_approx(delay, 0.75, 10000) as p_75
FROM (
    SELECT linien_id as train_number, GREATEST(0,(unix_timestamp(an_prognose, 'dd.MM.yyyy HH:mm:ss') - unix_timestamp(ankunftszeit, 'dd.MM.yyyy HH:mm'))) as delay
    FROM {username}.sbb_orc
    WHERE verkehrsmittel_text = 'IC' AND an_prognose_status IN ('REAL','GESCHAETZT')
        AND year = 2022 AND LOWER(produkt_id) = 'zug' AND haltestellen_name = 'Genève'
        AND linien_id IN {tuple(df_schedule['dep.train_number'])}
) AS del
GROUP BY train_number
ORDER BY train_number
"""

df_delay = pd.read_sql(query, conn)

# %%
fig = px.bar(
    df_delay, x='train_number', y='p_50', title='percentile50')
fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
fig.update_yaxes(matches=None, showticklabels=True)
fig.update_layout(showlegend=False)
fig.show()

# %%
fig = px.bar(
    df_delay, x='train_number', y='p_75', title='percentile75')
fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
fig.update_yaxes(matches=None, showticklabels=True)
fig.update_layout(showlegend=False)
fig.show()


# %% [markdown]
# __Answer__: The most disrupted trains are the first and last one. 
# For the first one, since it is the first run of the day, we can expect a longer preparation/controls which might delay the departure a bit at the first station.
#
# For the last one, we can expect it to wait for other connections that are late, making itself late. 
#
# Furthermore, we can observe bigger delays around 8:00 (713) and 18:00 (731), which corresponds to rush hours.

# %% [markdown]
# ### d) Delay heatmap 10/40
#
# - For each week (1 to 52) of each year (2018 to 2022), query `{username}.sbb_orc` to compute the median of delays of all trains __departing__ from __any__ train stations in Zürich area (i.e. all stop names that begin with `Zürich`) during that week. 
# - Use `plotly` to draw a heatmap (year x week) of the median delays. 
# - In which weeks and year were the trains delayed the most/least?
#
# __Note:__
# - Do not hesitate to create intermediary tables. 
# - When the train is ahead of schedule, count this as a delay of 0.
# - Use only stops with `AB_PROGNOSE_STATUS` equal to __REAL__ or __GESCHAETZT__.
# - For simplicty, a train station in Zürich area <=> it's a train station & its `HALTESTELLEN_NAME` starts with __Zürich__.
# - Heatmap with `plotly`: https://plotly.com/python/heatmaps/
# - Functions that may be useful: `unix_timestamp`, `from_unixtime`, `weekofyear`, `percentile_approx`, and `IF` (to create a column of 50th percentiles for each year)
# - Each row should have a _week,2018,2019,2020,2021,2022_ columns, where the week is the week number between 1 and 52, and the other columns are the median delay in Zürich for that week on the corresponding year.

# %%
query = f"""
SELECT year, week, percentile_approx(delay, 0.5, 10000) as p_50
FROM (
    SELECT year, weekofyear(from_unixtime(unix_timestamp(abfahrtszeit, 'dd.MM.yyyy HH:mm'))) as week, 
        GREATEST(0,(unix_timestamp(ab_prognose, 'dd.MM.yyyy HH:mm:ss')-unix_timestamp(abfahrtszeit, 'dd.MM.yyyy HH:mm'))) as delay
    FROM {username}.sbb_orc
    WHERE LOWER(produkt_id) = 'zug' AND haltestellen_name LIKE 'Zürich%' AND ab_prognose_status in ('REAL','GESCHAETZT')
) AS t
GROUP BY year, week
ORDER BY year, week
"""
df_heat = pd.read_sql(query, conn)

# %%
df_heat = df_heat.pivot(index='week', columns='year')['p_50']

# %%
fig = px.imshow(df_heat, aspect='auto')
fig.show()

# %% [markdown]
# __Answer__: We can see that trains were most delayed end of 2019, from week 35 to week 50. We can also see two periods with fewer delays: weeks 12 to 20 of 2020 and the start of 2022. We don't have data for the first few weeks of 2021.

# %% [markdown]
# ### e) Delay geomap 4/40
#
# Compute the median train delays in switzerland for the month of October 2019, and combine it with `{username}.sbb_stops_orc` to create a [density heatmap](https://plotly.com/python/mapbox-density-heatmaps/) of the delays across switzerland for that month.

# %%
query = f"""
SELECT DISTINCT t.haltestellen_name, t.p_50, geo.stop_lat, geo.stop_lon
FROM (
    SELECT haltestellen_name, percentile_approx(delay, 0.5, 10000) as p_50
    FROM (
        SELECT haltestellen_name, GREATEST(0,(unix_timestamp(an_prognose, 'dd.MM.yyyy HH:mm:ss') - unix_timestamp(ankunftszeit, 'dd.MM.yyyy HH:mm'))) as delay
        FROM {username}.sbb_orc
        WHERE year = 2019 AND month = 10 AND an_prognose_status in ('REAL','GESCHAETZT') AND LOWER(produkt_id) = 'zug'
    ) AS t
    GROUP BY haltestellen_name
) AS t
INNER JOIN (
    SELECT stop_name, stop_lat, stop_lon
    FROM {username}.sbb_stops_orc
) as geo
ON t.haltestellen_name = geo.stop_name
"""
df_geo = pd.read_sql(query, conn)
df_geo

# %%
fig = px.density_mapbox(df_geo, lat='geo.stop_lat', lon='geo.stop_lon', z='t.p_50', radius=10, zoom=6,
                        mapbox_style="stamen-terrain", title='Train arrival delays in CH')
fig.update_layout(autosize=False, width=900, height=600)
fig.show()

# %% [markdown]
# ## Part II: Twitter Data (16 Points)
#
# Data source: https://archive.org/details/twitterstream?sort=-publicdate 
#
# In this part, you will leverage Hive to extract the hashtags from the source data, and then perform light exploration of the prepared data. 
#
# ### Dataset Description 
#
# Format: the dataset is presented as a collection of textfiles containing one JSON document per line. The data is organized in a hierarchy of folders, with one file per minute. The textfiles have been compressed using bzip2. In this part, we will mainly focus on __2016 twitter data__.
#
# Location: you can find the data on HDFS at the path `/data/twitter/part_json/year={year}/month={month}/day={day}/{hour}/{minute}.json.bz2`. 
#
# Relevant fields: 
# - `created_at`, `timestamp_ms`: The first is a human-readable string representation of when the tweet was posted. The latter represents the same instant as a timestamp in seconds since UNIX epoch. 
# - `lang`: the language of the tweet content 
# - `entities`: parsed entities from the tweet, e.g. hashtags, user mentions, URLs.
# - In this repository, you can find [a tweet example](../data/tweet-example.json).

# %% [markdown]
# <div style="font-size: 100%" class="alert alert-block alert-warning">
#     <b>Disclaimer</b>
#     <br>
#     This dataset contains unfiltered data from Twitter. As such, you may be exposed to tweets/hashtags containing vulgarities, references to sexual acts, drug usage, etc.
#     </div>

# %% [markdown]
# ### a) JsonSerDe - 4/16
#
# In the exercise of week 4, you have already seen how to use the [SerDe framework](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe) to extract JSON fields from raw text format. 
#
# In this question, please use SerDe to create an <font color="red" size="3px">EXTERNAL</font> table with __one day__ twitter data, namely Jan 1st 2016. You only need to extract three columns: `timestamp_ms`, `lang` and `entities`(with the field `hashtags` only) with following schema (you need to figure out what to fill in `...`):
# ```
# timestamp_ms string,
# lang         string,
# entities     struct<hashtags:array<...<text:..., indices:...>>>
# ```
#
# The table you create should be similar to:
#
# | timestamp_ms | lang | entities |
# |---|---|---|
# | 1234567890001 | en | {"hashtags":[]} |
# | 1234567890002 | fr | {"hashtags":[{"text":"hashtag1","indices":[10]}]} |
# | 1234567890002 | jp | {"hashtags":[{"text":"hashtag1","indices":[14,23]}, {"text":"hashtag2","indices":[45]}]} |
#
# __Note:__
#    - JsonSerDe: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe
#    - Hive data types: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes

# %%
query=f"""
    DROP TABLE IF EXISTS {username}.hashtags_one_day_intermediary
"""
cur.execute(query)

query=f"""
    CREATE EXTERNAL TABLE {username}.hashtags_one_day_intermediary(
                timestamp_ms string,
                lang string,
                entities struct<hashtags:array<struct<text:string, indices:array<int>>>>
                )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    WITH SERDEPROPERTIES("ignore.malformed.json"="true")
    STORED AS TEXTFILE
    LOCATION '/data/twitter/part_json/year=2016/month=01/day=01'
"""
cur.execute(query)

# %%
query=f"""
    SELECT * FROM {username}.hashtags_one_day_intermediary LIMIT 10
"""
pd.read_sql(query, conn)

# %% [markdown]
# ### b) Explosion - 4/16
#
# In a), you created an external table where each row could contain a list of multiple hashtags. Create another _Hive Managed_ table by normalizing the table obtained from the previous step. This means that each row should contain exactly one hashtag. Include `timestamp_ms` and `lang` in the resulting table, as shown below.
#
# | timestamp_ms | lang | hashtag |
# |---|---|---|
# | 1234567890001 | es | hashtag1 |
# | 1234567890001 | es | hashtag2 |
# | 1234567890002 | en | hashtag2 |
# | 1234567890003 | zh | hashtag3 |
#
# __Note:__
#    - `LateralView`: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView
#    - `explode` function: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-explode>

# %%
query=f"""
    DROP TABLE IF EXISTS {username}.hashtags_one_day
"""
cur.execute(query)

query=f"""
    CREATE TABLE IF NOT EXISTS {username}.hashtags_one_day
    STORED AS ORC
    AS(
        SELECT timestamp_ms, 
               lang, 
               hashtag 
        FROM {username}.hashtags_one_day_intermediary LATERAL VIEW explode(entities.hashtags.text) adTable AS hashtag)
"""
cur.execute(query)

# %%
query=f"""
    SELECT * FROM {username}.hashtags_one_day LIMIT 10
"""
pd.read_sql(query, conn)

# %% [markdown]
# ### c) Hashtags - 8/16
#
# Query the normailized table you obtained in b). Create a table of the top 20 most mentioned hashtags with the contribution of each language. And, for each hashtag, order languages by their contributions. You should have a table similar to:
#
# |hashtag|lang|lang_count|total_count|
# |---|---|---|---|
# |hashtag_1|en|2000|3500|
# |hashtag_1|fr|1000|3500|
# |hashtag_1|jp|500|3500|
# |hashtag_2|te|500|500|
#
# Use `plotly` to create a stacked bar chart to show the results. Each bar correspond to a hashtag, and consists of a _stack_ of all the language contributions to the hashtag, each of them shown using different a color as (partially) shown below:
#
# <img src="../notebooks/2c-example.png" alt="example 2c" width="100"/>
#
# __Note:__ to properly order the bars, you may need:
# ```python
# fig.update_layout(xaxis_categoryorder = 'total descending')
# ```

# %%
query=f"""
    DROP TABLE IF EXISTS {username}.top_hashtags_one_day
"""
cur.execute(query)


query=f"""
    CREATE TABLE IF NOT EXISTS {username}.top_hashtags_one_day
    STORED AS ORC
    AS
    SELECT tot.hashtag as hashtag, la.lang as lang, la.lang_count as lang_count, tot.total_count as total_count
    FROM (
        SELECT hashtag, COUNT(*) as total_count
        FROM {username}.hashtags_one_day
        GROUP BY hashtag
        ORDER BY total_count DESC
        LIMIT 20
    ) as tot
    INNER JOIN (
        SELECT hashtag, lang, COUNT(*) as lang_count
        FROM {username}.hashtags_one_day
        GROUP BY hashtag, lang
    ) la
    ON tot.hashtag = la.hashtag
    ORDER BY total_count DESC, lang_count DESC
    
"""
cur.execute(query)

# %%
query=f"""
    SELECT hashtag, lang, lang_count, total_count
    FROM {username}.top_hashtags_one_day
"""
df_hashtag = pd.read_sql(query, conn)

# %%
df_hashtag

# %%
fig = px.bar(
    df_hashtag, x='hashtag', y='lang_count', color='lang',
    labels={'lang_count':'count', 'lang':'language'},
    title='The top 20 most popular hashtags of one day in 2020'
)
fig.update_layout(xaxis_categoryorder = 'total descending')
fig.show()

# %% [markdown]
# # That's all, folks!
