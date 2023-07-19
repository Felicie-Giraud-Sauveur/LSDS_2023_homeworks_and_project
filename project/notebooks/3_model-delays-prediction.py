# # 3. Make model for delays prediction

# **We use the real-world SBB data to train the delay prediction model.**  
# We use the following features of a trip to train the model:
#
# 1. Stop id
# 2. Product id (bus, tram, zug)
# 3. Minute of the sheduled arrival time
# 4. Hour of the sheduled arrival time
# 5. Month
# 6. Year
# 7. Week of the year
# 8. Day of the week
# 9. Day of the year
#
# We used a Logistic Regression model. We put the delays into 16 bins of <0 min, 0-1min, 1-2min, 2-3min, 3-4min, 4-5min, 5-6min, 6-7min, 7-8min, 8-9min, 9-10min, 10-15min, 15-20min, 20-30min, >30min. 
#
# Our trained model  predicts the probability of the given trip to have a delay lying in the corresponding bin. We use these probabilities in our overall algorithm to predict the probability of success of the overall trip.

# ## Spark model

# %load_ext sparkmagic.magics

# +
import os
from IPython import get_ipython
username = os.environ['RENKU_USERNAME']
server = "http://iccluster044.iccluster.epfl.ch:8998"

# set the application name as "<your_gaspar_id>-project"
get_ipython().run_cell_magic(
    'spark',
    line='config', 
    cell="""{{ "name": "{0}-project", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G" }}""".format(username)
)
# -

get_ipython().run_line_magic("spark", f"""add -s {username}-project -l python -u {server} -k""")

# + language="spark"
# print('We are using Spark %s' % spark.version)

# + language="spark"
# # use when %%send_to_spark does not work
# username = 'rochinha'
# -

# %%send_to_spark -i username -t str -n username

# + language="spark"
# import pyspark.sql.functions as F
# from pyspark.ml.feature import OneHotEncoder, VectorAssembler, StandardScaler
# from pyspark.ml.classification import LogisticRegression
# from pyspark.sql.functions import when, col
# from pyspark.sql.types import IntegerType

# + language="spark"
# spark.sql(f"use {username}")
# tables = spark.sql("show tables").show()

# + language="spark"
# sbb = spark.read.orc(f'/user/{username}/work')
# sbb.printSchema()

# + language="spark"
# # Open the table in Spark
# #sbb = spark.read.orc(f'/user/{username}/work')
# sbb = spark.sql(f"SELECT * FROM {username}.proba_models_table WHERE year=2022 AND month=1")
# sbb = sbb.drop("trip_id", "linien_id", "linien_text", "additional_trip", "failure", "day_name", "stop_lat", "stop_lon", "distance_km", "stop_name", "parent_station", "location_type", "departure_time_shedule", "departure_time_actual")
# sbb = sbb.withColumn("stop_id", F.substring(F.col("stop_id"), 0, 7).cast("int")) # Some stop_id are of the form 8590275:0:A
# sbb = sbb.withColumn("transport_type", F.lower(F.col("vehicule"))).drop("vehicule")
# sbb = sbb.dropna(how="any")
# sbb = sbb.dropDuplicates()
# sbb.printSchema()

# + language="spark"
# # Transform times and dates to spark format
# sbb = sbb.withColumn("date_trip", F.to_date(F.col("date_trip"), 'dd.MM.yyyy'))
# sbb = sbb.withColumn("arrival_time_actual", F.to_timestamp(F.col("arrival_time_actual"), 'dd.MM.yyyy HH:mm:ss'))
# sbb = sbb.withColumn("arrival_time_shedule", F.to_timestamp(F.col("arrival_time_shedule"), 'dd.MM.yyyy HH:mm'))

# + language="spark"
# # Compute delay
# sbb = sbb.withColumn("delay", F.col("arrival_time_actual").cast("long") - F.col("arrival_time_shedule").cast("long"))
# sbb = sbb.withColumn("delay", F.when(F.col("delay") < 0, 0).otherwise(F.col("delay") / 60))

# + language="spark"
# # Compute the model's features
# sbb = sbb.withColumn("day_of_week", F.dayofweek(F.col("date_trip")))
# sbb = sbb.withColumn("day_of_year", F.dayofyear(F.col("date_trip")))
# sbb = sbb.withColumn("week_of_year", F.weekofyear(F.col("date_trip")))
# sbb = sbb.withColumn("hour", F.hour(F.col("arrival_time_shedule")))
# sbb = sbb.withColumn("minute", F.minute(F.col("arrival_time_shedule")))

# + language="spark"
# # We map the transport type into a unique id
# transport_id_map = {
#     '': 0,
#     'zug': 1,
#     'bus': 2,
#     'tram': 3,
# }
#
# sbb = sbb.withColumn("transport_type", F.udf(lambda x: transport_id_map.get(x, 0), IntegerType())(F.col("transport_type")))
# sbb = sbb.dropna(how="any")
# sbb = sbb.dropDuplicates().cache()
# sbb.count()

# + language="spark"
# # We One-Hot encode the transport type and stop_id as allows us to represent categorical variables in a format that is more suitable for a Logistic Regression model and gives more prediting power to the model.
# transport_type_ohe = OneHotEncoder(inputCol="transport_type", outputCol="transport_type_ohe", dropLast=False)
# sbb = transport_type_ohe.transform(sbb)
# stop_id_ohe = OneHotEncoder(inputCol="stop_id", outputCol="stop_id_ohe", dropLast=False)
# sbb = stop_id_ohe.transform(sbb)

# + language="spark"
# # Write model to hdfs
# transport_type_ohe.write().overwrite().save(f'/user/{username}/spark_models/transport_type_ohe')
# stop_id_ohe.write().overwrite().save(f'/user/{username}/spark_models/stop_id_ohe')

# + language="spark"
# inputCols = ["minute", "hour", "month", "year", "week_of_year", "day_of_year", "day_of_week", "transport_type_ohe", "stop_id_ohe"]
#
# assembler = VectorAssembler(inputCols = inputCols, outputCol = 'features')
#
# data = assembler.transform(sbb).select('features', 'delay')

# + language="spark"
# # We scale our data as it often give better predictions.
# scaler = StandardScaler(inputCol='features', outputCol="scaled_features", withStd=True, withMean=True)
# scaler_model = scaler.fit(data)
# data = scaler_model.transform(data).cache()

# + language="spark"
# # Write model to hdfs
# scaler.write().overwrite().save(f'/user/{username}/spark_models/delays_scaler')
# scaler_model.write().overwrite().save(f'/user/{username}/spark_models/delays_scaler_model')

# + language="spark"
# # Split delay into 15 bins
# def label_bin(x):
#     bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30]
#     bins_id = range(len(bins) + 1)
#     for i, b in enumerate(bins):
#         if x <= b:
#             return bins_id[i]
#     return bins_id[-1]
#         
# label_bin_udf = F.udf(label_bin, LongType())

# + language="spark"
# data = data.withColumn('delay_bin', label_bin_udf(F.col('delay')))

# + language="spark"
# # Train our Logistic Regression model
# log = LogisticRegression(featuresCol='scaled_features', labelCol='delay_bin', regParam=0.1)
# log_model = log.fit(data)

# + language="spark"
# # Write model to hdfs
# log.write().overwrite().save(f'/user/{username}/spark_models/log')
# log_model.write().overwrite().save(f'/user/{username}/spark_models/log_model')

# + language="spark"
# data_pred = log_model.transform(data)
# data_pred.show()
# -

# %spark cleanup

# ## Scikit learn model

# +
import os
import pandas as pd
import pickle

username = os.environ['RENKU_USERNAME']
hiveaddr = os.environ['HIVE_SERVER2']
(hivehost,hiveport) = hiveaddr.split(':')
print("Operating as: {0}".format(username))

# +
from pyhive import hive

# create connection
conn = hive.connect(host=hivehost, port=hiveport, username=username)

# create cursor
cur = conn.cursor()
# -

# 
df = pd.read_sql(f"""SELECT date_trip, year, month, arrival_time_shedule, arrival_time_actual, stop_id, vehicule FROM {username}.proba_models_table WHERE year=2022 AND month=1 AND arrival_time_shedule != '' AND arrival_time_actual != '' ORDER BY RAND() LIMIT 2000000""", conn)
df = df.rename(columns={"vehicule": "transport_type"})
df.head()

df.info()

df.isna().sum()

df.count()

# Change type the pandas date time
df.date_trip = pd.to_datetime(df.date_trip, format='%d.%m.%Y')
df.arrival_time_shedule = pd.to_datetime(df.arrival_time_shedule, format='%d.%m.%Y %H:%M')
df.arrival_time_actual = pd.to_datetime(df.arrival_time_actual, format='%d.%m.%Y %H:%M:%S')
df.info()

# Format stop_id
df.stop_id = df.stop_id.str.strip('Parent')
df.stop_id = df.stop_id.str.slice(stop=7)
df.head()

df['delay'] = (df.arrival_time_actual - df.arrival_time_shedule).dt.total_seconds() / 60
df.head()


def label_bin(x):
    
    bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30]
    bins_id = range(len(bins) + 1)
    for i, b in enumerate(bins):
        if x <= b:
            return bins_id[i]
    return bins_id[-1]


df["delay_bins"] = df.delay.apply(label_bin)
df.head()

df["minute"] = df.arrival_time_shedule.dt.minute
df["hour"] = df.arrival_time_shedule.dt.hour
df["year"] = df.date_trip.dt.year
df["week_of_year"] = df.date_trip.dt.isocalendar().week
df["day_of_year"] = df.date_trip.dt.day_of_year
df["day_of_week"] = df.date_trip.dt.day_of_week
df.head()

from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_validate
from scipy.sparse import hstack

inputCols = ["minute", "hour", "month", "year", "week_of_year", "day_of_year", "day_of_week"]
ohe_cols = ["transport_type", "stop_id"]
y = df.delay_bins
X = df[inputCols].to_numpy(dtype=int)
X

# +
# We scale the numerical feature to have 0-mean and 1-std
scaler = StandardScaler().fit(X)

# Save model for inference
with open(f'../models/scaler.pkl','wb') as f:
    pickle.dump(scaler, f)
    
with open(f'../models/scaler.pkl','rb') as f:
    scaler = pickle.load(f)
    
X = scaler.transform(X)
# +
# We One-Hot encode the transport type and stop_id as allows us to represent categorical variables in a format that 
# is more suitable for a Logistic Regression model and gives more prediting power to the model.
# We do not scale the categorical feactures as they are represanted as sparse matrix and the data would not be sparse anymore. 

ohe = OneHotEncoder(handle_unknown='ignore', dtype=int).fit(df[["transport_type"]])

# Save model for inference
with open(f'../models/transport_type_ohe.pkl','wb') as f:
    pickle.dump(ohe, f)
    
with open(f'../models/transport_type_ohe.pkl','rb') as f:
    ohe = pickle.load(f)
    
transformed = ohe.transform(df[["transport_type"]])
X = hstack((X, transformed), format='csr')
X

# +
ohe = OneHotEncoder(handle_unknown='ignore', dtype=int).fit(df[["stop_id"]])

# Save model for inference
with open(f'../models/stop_id_ohe.pkl','wb') as f:
    pickle.dump(ohe, f)
    
with open(f'../models/stop_id_ohe.pkl','rb') as f:
    ohe = pickle.load(f)
    
transformed = ohe.transform(df[["stop_id"]])
X = hstack((X, transformed), format='csr')
X

# +
log = LogisticRegression(random_state=0, C=0.1, penalty="l2", max_iter=100, solver='saga').fit(X, y)

# Save model for inference
with open(f'../models/log.pkl','wb') as f:
    pickle.dump(log, f)

with open(f'../models/log.pkl','rb') as f:
    log = pickle.load(f)
# -

log.predict_proba(X[0, :])

# Take a small data sample to do cross validation on the penalty type and value.
_, X_test, _, y_test= train_test_split(X, y, test_size=10000)

for c in [0.01, 0.1, 1]:
    log = LogisticRegression(random_state=0, C=c, penalty="l2", solver="saga").fit(X_test, y_test)
    print(f"L2: {c}", cross_validate(log, X_test, y_test, cv=4, scoring="accuracy"))
    
    log = LogisticRegression(random_state=0, C=c, penalty="l1", solver="saga").fit(X_test, y_test)
    print(f"L1: {c}", cross_validate(log, X_test, y_test, cv=4, scoring="accuracy"))

# +
# Code used to do inference

# loading models
with open(f'../models/transport_type_ohe.pkl','rb') as f:
    transport_type_ohe = pickle.load(f)

with open(f'../models/stop_id_ohe.pkl','rb') as f:
    stop_id_ohe = pickle.load(f)

with open(f'../models/scaler.pkl','rb') as f:
    scaler = pickle.load(f)

with open(f'../models/log.pkl','rb') as f:
    log = pickle.load(f)

# Give the bin for the delay
def label_bin(x):
    bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30]
    bins_id = range(len(bins) + 1)
    for i, b in enumerate(bins):
        if x <= b:
            return bins_id[i]
    return bins_id[-1]

def delay_proba(delay_max, arrival_time_shedule, transport_type, stop_id):
        
    try:
        df = pd.DataFrame(columns=['arrival_time_shedule', 'transport_type', 'stop_id'])
        df.loc[0] = [arrival_time_shedule, str(transport_type), str(stop_id)]

        df.arrival_time_shedule = pd.to_datetime(df.arrival_time_shedule, format='%d.%m.%Y %H:%M')

        # Compute the predictors
        df["minute"] = df.arrival_time_shedule.dt.minute
        df["hour"] = df.arrival_time_shedule.dt.hour
        df["month"] = df.arrival_time_shedule.dt.month
        df["year"] = df.arrival_time_shedule.dt.year
        df["week_of_year"] = df.arrival_time_shedule.dt.isocalendar().week
        df["day_of_year"] = df.arrival_time_shedule.dt.day_of_year
        df["day_of_week"] = df.arrival_time_shedule.dt.day_of_week

        inputCols = ["minute", "hour", "month", "year", "week_of_year", "day_of_year", "day_of_week"]
        ohe_cols = ["transport_type", "stop_id"]
        X = df[inputCols].to_numpy(dtype=int)
        
        # Scale data
        X = scaler.transform(X)
        
        # One hot encode transport type
        transformed = transport_type_ohe.transform(df[[ohe_cols[0]]])
        X = hstack((X, transformed), format='csr')

        # One hot encode stop id
        transformed = stop_id_ohe.transform(df[[ohe_cols[1]]])
        X = hstack((X, transformed), format='csr')

        # Prediect the probability distribution
        prob = log.predict_proba(X)[0]

        # Get the delay bin
        delay_bin = label_bin(delay_max)

        # The probability of arrival time below delay_max is the sum of the prediected probability for delay <= delay_max
        total_proba = sum(prob[:delay_bin + 1])

        return total_proba
    
    # If the model fail for some raison
    except:
        return min(0.99 ** (10 - delay_max), 1)


# -

delay_proba(0, df.arrival_time_shedule.iloc[0], df.transport_type.iloc[0], df.stop_id.iloc[0])

delay_proba(1, df.arrival_time_shedule.iloc[0], df.transport_type.iloc[0], df.stop_id.iloc[0])

delay_proba(2, df.arrival_time_shedule.iloc[0], df.transport_type.iloc[0], df.stop_id.iloc[0])

delay_proba(5, df.arrival_time_shedule.iloc[0], df.transport_type.iloc[0], df.stop_id.iloc[0])
