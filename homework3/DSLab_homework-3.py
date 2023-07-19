# -*- coding: utf-8 -*-
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
# # DSLab Homework3 - Uncovering World Events using Twitter Hashtags
#
# ## ... and learning about Spark `DataFrames` along the way
#
# In this notebook, we will use temporal information about Twitter hashtags to discover trending topics and potentially uncover world events as they occurred. 
#
# ## Hand-in Instructions:
#
# - __Due: 02.05.2023 23:59:59 CET__
# - your project must be private
# - `git push` your final verion to the master branch of your group's Renku repository before the due date
# - check if `Dockerfile`, `environment.yml` and `requirements.txt` are properly written
# - add necessary comments and discussion to make your codes readable
# - make sure the image builds successfully and your code is runnable

# %% [markdown]
# ## Hashtags
#
# The idea here is that when an event is happening and people are having a conversation about it on Twitter, a set of uniform hashtags that represent the event spontaneously evolves. Twitter users then use those hashtags to communicate with one another. Some hashtags, like `#RT` for "retweet" or just `#retweet` are used frequently and don't tell us much about what is going on. But a sudden appearance of a hashtag like `#oscars` probably indicates that the oscars are underway. For a particularly cool example of this type of analysis, check out [this blog post about earthquake detection using Twitter data](https://blog.twitter.com/official/en_us/a/2015/usgs-twitter-data-earthquake-detection.html) (although they search the text and not necessarily hashtags).

# %% [markdown]
# ## Initialize the environment

# %%
# %load_ext sparkmagic.magics

# %%
import os
from IPython import get_ipython
username = os.environ['RENKU_USERNAME']
server = "http://iccluster044.iccluster.epfl.ch:8998"

# set the application name as "<your_gaspar_id>-homework3"
get_ipython().run_cell_magic(
    'spark',
    line='config', 
    cell="""{{ "name": "{0}-homework3", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G" }}""".format(username)
)

# %% [markdown]
# Send `username` to Spark kernel, which will first start the Spark application if there is no active session.

# %%
get_ipython().run_line_magic(
    "spark", f"""add -s {username}-homework3 -l python -u {server} -k"""
)

# %% language="spark"
# print('We are using Spark %s' % spark.version)

# %%
# %%spark?

# %% [markdown]
# ## PART I: Set up (5 points)
#
# The twitter stream data is downloaded from [Archive Team: The Twitter Stream Grab](https://archive.org/details/twitterstream), which is a collection of a random sample of all tweets. We have parsed the stream data and prepared the twitter hashtag data of __2016__, a very special and different year in many ways. Let's see if we can see any trends about all these events of 2016 in the Twitter data. 
#
# <div style="font-size: 100%" class="alert alert-block alert-danger">
# <b>Disclaimer</b>
# <br>
# This dataset contains unfiltered data from Twitter. As such, you may be exposed to tweets/hashtags containing vulgarities, references to sexual acts, drug usage, etc.
# </div>

# %% [markdown]
# ### a) Load data - 1/5
#
# Load the **orc** data from `/data/twitter/part_orc/hashtags/year=2016` into a Spark dataframe using the appropriate `SparkSession` method. 
#
# Look at the first few rows of the dataset - note the timestamp and its units!

# %% language="spark"
# df = spark.read.orc("/data/twitter/part_orc/hashtags/year=2016")
# df.printSchema()

# %% language="spark"
# df.show(n=5, truncate=False, vertical=False)

# %% [markdown]
# <div style="font-size: 100%" class="alert alert-block alert-info">
#     <b>Cluster Usage:</b> As there are many of you working with the cluster, we encourage you to
#     <ul>
#         <li>prototype your queries on small data samples before running them on whole datasets</li>
#         <li>save your intermediate results in your own directory at hdfs <b>"/user/&lt;your-gaspar-id&gt;/"</b></li>
#     </ul>
# </div>
#
# For example:
#
# ```python
#     # create a subset of original dataset
#     df_sample = df.sample(0.01)
#     
#     # save as orc
#     df_sample.write.orc('/user/%s/sample.orc' % username, mode='overwrite')
#
# ```

# %% [markdown]
# ### b) Functions - 2/5

# %% language="spark"
# import pyspark.sql.functions as F

# %% [markdown]
# __User-defined functions__
#
# A neat trick of spark dataframes is that you can essentially use something very much like an RDD `map` method but without switching to the RDD. If you are familiar with database languages, this works very much like e.g. a user-defined function in SQL. 
#
# So, for example, if we wanted to make a user-defined python function that returns the hashtags in lowercase, we could do something like this:

# %% language="spark"
# @F.udf
# def lowercase(text):
#     """Convert text to lowercase"""
#     return text.lower()

# %% [markdown]
# The `@F.udf` is a "decorator" -- this is really handy python syntactic sugar and in this case is equivalent to:
#
# ```python
# def lowercase(text):
#     return text.lower()
#     
# lowercase = F.udf(lowercase)
# ```
#
# It basically takes our function and adds to its functionality. In this case, it registers our function as a pyspark dataframe user-defined function (UDF).
#
# Using these UDFs is very straightforward and analogous to other Spark dataframe operations. For example:

# %% language="spark"
# df.select(lowercase(df.hashtag)).show(n=5)

# %% [markdown]
# __Built-in functions__
#
# Using a framework like Spark is all about understanding the ins and outs of how it functions and knowing what it offers. One of the cool things about the dataframe API is that many functions are already defined for you (turning strings into lowercase being one of them). Find the [Spark python API documentation](https://spark.apache.org/docs/2.3.2/api/python/index.html). Look for the `sql` section and find the listing of `sql.functions`. Repeat the above (turning hashtags into lowercase) but use the built-in function.

# %% language="spark"
# df.select(F.lower(df.hashtag)).show(n=5)

# %% [markdown]
# We'll work with a combination of these built-in functions and user-defined functions for the remainder of this homework. 
#
# Note that the functions can be combined. Consider the following dataframe and its transformation:

# %% language="spark"
# from pyspark.sql import Row
#
# # create a sample dataframe with one column "degrees" going from 0 to 180
# test_df = spark.createDataFrame(spark.sparkContext.range(180).map(lambda x: Row(degrees=x)), ['degrees'])
#
# # define a function "sin_rad" that first converts degrees to radians and then takes the sine using built-in functions
# sin_rad = F.sin(F.radians(test_df.degrees))
#
# # show the result
# test_df.select(sin_rad).show()

# %% [markdown]
# ### c) Tweets in english - 2/5
#
# - Create `english_df` with only english-language tweets. 
# - Turn hashtags into lowercase.
# - Convert the timestamp to a more readable format and name the new column as `date`.
# - Sort the table in chronological order. 
#
# Your `english_df` should look something like this:
#
# ```
# +-----------+----+--------------------+-------------------+
# |timestamp_s|lang|             hashtag|               date|
# +-----------+----+--------------------+-------------------+
# | 1462084200|  en|             science|2016-05-01 08:30:00|
# | 1462084200|  en|           worcester|2016-05-01 08:30:00|
# | 1462084200|  en|internationalwork...|2016-05-01 08:30:00|
# | 1462084200|  en|         waterstreet|2016-05-01 08:30:00|
# | 1462084200|  en|               funny|2016-05-01 08:30:00|
# +-----------+----+--------------------+-------------------+
# ```
#
# __Note:__ 
# - The hashtags may not be in english.
# - [pyspark.sql.functions](https://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# %% language="spark"
# english_df = df.filter(df.lang == 'en')
# english_df = english_df.select("timestamp_s", "lang", F.lower(df.hashtag).alias("hashtag"))
# english_df = english_df.withColumn("date", F.to_timestamp(F.col("timestamp_s")))
# english_df = english_df.sort(F.asc("date"))
# english_df.show(n=5)

# %% [markdown]
# ## PART II: Twitter hashtag trends (30 points)
#
# In this section we will try to do a slightly more complicated analysis of the tweets. Our goal is to get an idea of tweet frequency as a function of time for certain hashtags. 
#
# Have a look [here](http://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#module-pyspark.sql.functions) to see the whole list of custom dataframe functions - you will need to use them to complete the next set of TODO items.

# %% [markdown]
# ### a) Top hashtags - 1/30
#
# We used `groupBy` already in the previous notebooks, but here we will take more advantage of its features. 
#
# One important thing to note is that unlike other RDD or DataFrame transformations, the `groupBy` does not return another DataFrame, but a `GroupedData` object instead, with its own methods. These methods allow you to do various transformations and aggregations on the data of the grouped rows. 
#
# Conceptually the procedure is a lot like this:
#
# ![groupby](https://i.stack.imgur.com/sgCn1.jpg)
#
# The column that is used for the `groupBy` is the `key` - once we have the values of a particular key all together, we can use various aggregation functions on them to generate a transformed dataset. In this example, the aggregation function is a simple `sum`. In the simple procedure below, the `key` will be the hashtag.
#
#
# Use `groupBy`, calculate the top 5 most common hashtags in the whole english-language dataset.
#
# This should be your result:
#
# ```
# +-------------------+-------+
# |            hashtag|  count|
# +-------------------+-------+
# |         mtvhottest|1508316|
# |      veranomtv2016| 832977|
# | pushawardslizquens| 645521|
# |pushawardskathniels| 397085|
# |         teenchoice| 345656|
# +-------------------+-------+
# ```

# %% language="spark"
# english_df.groupBy('hashtag').count().sort(F.desc("count")).show(n=5)

# %% [markdown]
# ### b) Daily hashtags - 2/30
#
# Now, let's see how we can start to organize the tweets by their timestamps. Remember, our goal is to uncover trending topics on a timescale of a few days. A much needed column then is simply `day`. Spark provides us with some handy built-in dataframe functions that are made for transforming date and time fields.
#
# - Create a dataframe called `daily_hashtag` that includes the columns `month`, `week`, `day` and `hashtag`. 
# - Use the `english_df` you made above to start, and make sure you find the appropriate spark dataframe functions to make your life easier. For example, to convert the date string into day-of-year, you can use the built-in [dayofyear](http://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#pyspark.sql.functions.dayofyear) function. 
# - For the simplicity of following analysis, filter only tweets of 2016.
# - Show the result.
#
# Try to match this view:
#
# ```
# +-----+----+---+--------------------+
# |month|week|day|             hashtag|
# +-----+----+---+--------------------+
# |    5|  17|122|             science|
# |    5|  17|122|           worcester|
# |    5|  17|122|internationalwork...|
# |    5|  17|122|         waterstreet|
# |    5|  17|122|               funny|
# +-----+----+---+--------------------+
# ```

# %% language="spark"
# daily_hashtag = english_df.filter(F.year('date') == 2016)
# daily_hashtag = daily_hashtag.select(F.month('date').alias('month'), F.weekofyear('date').alias('week'), F.dayofyear('date').alias('day'), "hashtag")
# daily_hashtag.show(n=5)

# %% [markdown]
# ### c) Daily counts - 2/30
#
# Now we want to calculate the number of times a hashtag is used per day based on the dataframe `daily_hashtag`. Sort in descending order of daily counts and show the result. Call the resulting dataframe `day_counts`.
#
# Your output should look like this:
#
# ```
# +---+----------+----+-----+
# |day|hashtag   |week|count|
# +---+----------+----+-----+
# |204|mtvhottest|29  |66372|
# |205|mtvhottest|29  |63495|
# |219|mtvhottest|31  |63293|
# |218|mtvhottest|31  |61187|
# |207|mtvhottest|30  |60768|
# +---+----------+----+-----+
# ```
#
# <div class="alert alert-info">
# <p>Make sure you use <b>cache()</b> when you create <b>day_counts</b> because we will need it in the steps that follow!</p>
# </div>

# %% language="spark"
# day_counts = daily_hashtag.groupBy(['day', 'hashtag', 'week']).count().sort(F.desc("count")).cache()
# day_counts.show(n=5, truncate=False)

# %% [markdown]
# ### d) Weekly average - 2/30
#
# To get an idea of which hashtags stay popular for several days, calculate the average number of daily occurences for each week. Sort in descending order and show the top 20.
#
# __Note:__
# - Use the `week` column we created above.
# - Calculate the weekly average using `F.mean(...)`.

# %% language="spark"
# day_counts.groupBy(['week', 'hashtag']).agg({"count": "avg"}).sort(F.desc("avg(count)")).show(n=5)

# %% [markdown]
# ### e) Ranking - 3/30
#
# Window functions are another awesome feature of dataframes. They allow users to accomplish complex tasks using very concise and simple code. 
#
# Above we computed just the hashtag that had the most occurrences on *any* day. Now lets say we want to know the top tweets for *each* day.  
#
# This is a non-trivial thing to compute and requires "windowing" our data. I recommend reading this [window functions article](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html) to get acquainted with the idea. You can think of a window function as a fine-grained and more flexible `groupBy`. 
#
# There are two things we need to define to use window functions:
#
# 1. the "window" to use, based on which columns (partitioning) and how the rows should be ordered 
# 2. the computation to carry out for each windowed group, e.g. a max, an average etc.
#
# Lets see how this works by example. We will define a window function, `daily_window` that will partition data based on the `day` column. Within each window, the rows will be ordered by the daily hashtag count that we computed above. Finally, we will use the rank function **over** this window to give us the ranking of top tweets. 
#
# In the end, this is a fairly complicated operation achieved in just a few lines of code! (can you think of how to do this with an RDD??)

# %% language="spark"
# from pyspark.sql import Window

# %% [markdown]
# First, we specify the window function and the ordering:

# %% language="spark"
# daily_window = Window.partitionBy('day').orderBy(F.desc('count'))

# %% [markdown]
# The above window function says that we should window the data on the `day` column and order it by count. 
#
# Now we need to define what we want to compute on the windowed data. We will start by just calculating the daily ranking of hashtags, so we can use the helpful built-in `F.rank()` and sort:

# %% language="spark"
# daily_rank = F.rank() \
#               .over(daily_window) \
#               .alias('rank')

# %% [markdown]
# Now compute the top five hashtags for each day in our data:

# %% language="spark"
# day_counts.select("day", "hashtag", daily_rank).filter(F.col('rank') < 6).show(n=20)

# %% [markdown]
# ### f) Rolling sum - 5/30
#
# With window functions, you can also calculate the statistics of a rolling window. 
#
# In this question, construct a 7-day rolling window (including the day and 6 days before) to calculate the rolling sum of the daily occurences for each hashtag.
#
# Your results should be like:
# - For the hashtag `brexit`:
#
# ```
# +---+----+-----+-------+-----------+
# |day|week|count|hashtag|rolling_sum|
# +---+----+-----+-------+-----------+
# |122|  17|  171| brexit|        171|
# |123|  18|  107| brexit|        278|
# |124|  18|  183| brexit|        461|
# |125|  18|  207| brexit|        668|
# |126|  18|  163| brexit|        831|
# |127|  18|  226| brexit|       1057|
# |128|  18|  149| brexit|       1206|
# |129|  18|  202| brexit|       1237|
# |130|  19|  330| brexit|       1460|
# |131|  19|  315| brexit|       1592|
# +---+----+-----+-------+-----------+
# ```

# %% language="spark"
# rolling_window = Window.partitionBy('hashtag').orderBy(F.asc('day')).rowsBetween(-6, 0)
# rolling_sum = F.sum('count') \
#                .over(rolling_window) \
#                .alias('rolling_sum')
#
# rs_counts = day_counts.select('day', 'week', 'count', 'hashtag', rolling_sum)

# %% language="spark"
# rs_counts.filter('hashtag == "brexit"').show(n=10)

# %% language="spark"
# rs_counts.filter('hashtag == "election"').show(n=10)

# %% [markdown]
# ### g) DIY - 15/30
#
# Use window functions (or other techniques!) to produce lists of top few trending tweets for each week. What's a __"trending"__ tweet? Something that seems to be __suddenly growing very rapidly in popularity__. 
#
# You should be able to identify, for example, Oscars-related hashtags, the United States presidential elections, Brexit, etc.
#
# The final listing should be clear and concise and the flow of your analysis should be easy to follow. If you make an implementation that is not immediately obvious, make sure you provide comments either in markdown cells or in comments in the code itself.

# %% language="spark"
# ### Defining the window 
# from pyspark.sql import Window
# Windowspec=Window.partitionBy('week', 'hashtag').orderBy(F.asc('day'))
#
# ### Calculating lag of price at each day level
# prev_day_count= day_counts.withColumn('prev_day_count',
#                          F.lag(day_counts['count'],1,0)
#                                 .over(Windowspec))
#
# ### Calculating the average                                  
# result = prev_day_count.withColumn('daily_return', 
#         (prev_day_count['count'] - prev_day_count['prev_day_count']) / 
#         prev_day_count['count'] )
# result = prev_day_count.withColumn('next_day_increase', 
#            (prev_day_count['count'] - prev_day_count['prev_day_count']))
# result.sort(F.desc("next_day_increase")).show()

# %% language="spark"
# result = result.groupBy('week', 'hashtag').agg(F.max('next_day_increase').alias("next_day_increase"))
# result = result.sort(F.desc("next_day_increase"))
# result.show()

# %% language="spark"
# weekly_window = Window.partitionBy('week').orderBy(F.desc('next_day_increase'))
# weekly_rank = F.rank() \
#                .over(weekly_window) \
#                .alias('rank')
# trending = result.select("hashtag", "week", "next_day_increase", weekly_rank)
# trending = trending.filter(trending.rank < 4).sort(F.asc('week'),F.asc('rank'))
# trending.show(15)

# %% [markdown]
# ## PART III: Hashtag clustering (25 points)

# %% [markdown]
# ### a) Feature vector - 3/25
#
# - Create a dataframe `daily_hashtag_matrix` that consists of hashtags as rows and daily counts as columns (hint: use `groupBy` and methods of `GroupedData`). Each row of the matrix represents the time series of daily counts of one hashtag. Cache the result.
#
# - Create the feature vector which consists of daily counts using the [`VectorAssembler`](https://spark.apache.org/docs/2.3.2/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler) from the Spark ML library. Cache the result.

# %% language="spark"
# daily_hashtag_matrix = day_counts.select('hashtag', 'day', 'count').groupBy('hashtag').pivot("day").sum('count').na.fill(0).cache()
# daily_hashtag_matrix.show(n=1, truncate=False)

# %% language="spark"
# from pyspark.ml.feature import VectorAssembler
# inputCols = [col for col in daily_hashtag_matrix.columns if col != 'hashtag']
# outputCol = "features"
# df_va = VectorAssembler(inputCols = inputCols, outputCol = outputCol)
# df = df_va.transform(daily_hashtag_matrix).cache()
# df.select(['features']).toPandas().head(5)

# %% [markdown]
# ### b) Visualization - 2/25
#
# Visualize the time sereis you just created. 
#
# - Select a few interesting hashtags you identified above. `isin` method of DataFrame columns might be useful.
# - Retrieve the subset DataFrame using sparkmagic
# - Plot the time series for the chosen hashtags with matplotlib.

# %% language="spark"
# hashtags = ['metgala', 'brexit', 'eurovision', 'mtvhottest']


# %% magic_args="-o df_plot" language="spark"
# df_plot = daily_hashtag_matrix.filter(daily_hashtag_matrix.hashtag.isin(hashtags))

# %%
# %matplotlib inline
import matplotlib
import matplotlib.pylab as plt

plt.rcParams['figure.figsize'] = (30,8)
plt.rcParams['font.size'] = 12
plt.style.use('fivethirtyeight')
df_plot.set_index('hashtag').T.rename_axis(None, axis=1).plot()
plt.legend()
plt.xlabel('day')
plt.ylabel('count')
plt.show()


# %% [markdown]
# ### c) KMeans clustering - 20/25
#
# Use KMeans to cluster hashtags based on the daily count timeseries you created above. Train the model and calculate the cluster membership for all hashtags. Again, be creative and see if you can get meaningful hashtag groupings. 
#
# Validate your results by showing certain clusters, for example, those including some interesting hashtags you identified above. Do they make sense?
#
# Make sure you document each step of your process well so that your final notebook is easy to understand even if the result is not optimal or complete. 
#
# __Note:__ 
# - Additional data cleaning, feature engineering, dimension reduction, etc. might be necessary to get meaningful results from the model. 
# - For available methods, check [pyspark.sql.functions documentation](https://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#module-pyspark.sql.functions), [Spark MLlib Guide](https://spark.apache.org/docs/2.3.2/ml-guide.html) and [pyspark.ml documentation](https://spark.apache.org/docs/2.3.2/api/python/pyspark.ml.html).

# %% language="spark"
# from pyspark.ml.clustering import KMeans
# from pyspark.ml.feature import StandardScaler
# from pyspark.ml.evaluation import ClusteringEvaluator

# %% language="spark"
# scale=StandardScaler(inputCol='features',outputCol='standardized')
# data_scale=scale.fit(df)
# data_scale_output=data_scale.transform(df)
# data_scale_output.show(1)

# %% magic_args="-o df_silhouette" language="spark"
# silhouette_score=[]
# evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized')
#
# for i in range(2,10):
#
#     KMeans_algo=KMeans(featuresCol='standardized', k=i)
#      
#     KMeans_fit=KMeans_algo.fit(data_scale_output)
#      
#     output=KMeans_fit.transform(data_scale_output)    
#   
#     score=evaluator.evaluate(output)
#     
#     silhouette_score.append((i,score))
#
#     print("Silhouette Score:",score)
#     
# df_silhouette = spark.createDataFrame(data=silhouette_score, schema=['k', 'score'])

# %%
#Visualizing the silhouette scores in a plot
import matplotlib.pyplot as plt
fig, ax = plt.subplots(1,1, figsize =(8,6))
ax.plot(df_silhouette.k,df_silhouette.score)
ax.set_xlabel('k')
ax.set_ylabel('score')
plt.show()

# %% [markdown]
# We select a k with a high silhouette score.

# %% language="spark"
# kmeans = KMeans(k=4)
# kmeans.setSeed(1)
# kmeans.setMaxIter(10)
# model = kmeans.fit(df)
# centers = model.clusterCenters()
# summary = model.summary
# print(summary.k)
# print(summary.clusterSizes)

# %% [markdown]
# We can see that our clusters are not really meaningful since almost all the points end up in the same cluster. 

# %% [markdown]
# # That's all, folks!

# %%
# %spark cleanup

# %%
