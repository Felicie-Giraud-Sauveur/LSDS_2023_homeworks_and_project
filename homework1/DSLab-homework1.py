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
# # DSLab Homework 1 - Data Science with CO2
#
# ## Hand-in Instructions
#
# - __Due: 21.03.2023 23h59 CET__
# - `git push` your final verion to the master branch of your group's Renku repository before the due
# - check if `Dockerfile`, `environment.yml` and `requirements.txt` are complete written
# - add necessary comments and discussion to make your codes readable

# %% [markdown]
# ## Carbosense
#
# The project Carbosense establishes a uniquely dense CO2 sensor network across Switzerland to provide near-real time information on man-made emissions and CO2 uptake by the biosphere. The main goal of the project is to improve the understanding of the small-scale CO2 fluxes in Switzerland and concurrently to contribute to a better top-down quantification of the Swiss CO2 emissions. The Carbosense network has a spatial focus on the City of Zurich where more than 50 sensors are deployed. Network operations started in July 2017.
#
# <img src="http://carbosense.wdfiles.com/local--files/main:project/CarboSense_MAP_20191113_LowRes.jpg" width="500">
#
# <img src="http://carbosense.wdfiles.com/local--files/main:sensors/LP8_ZLMT_3.JPG" width="156">  <img src="http://carbosense.wdfiles.com/local--files/main:sensors/LP8_sensor_SMALL.jpg" width="300">

# %% [markdown]
# ## Description of the homework
#
# In this homework, we will curate a set of **CO2 measurements**, measured from sensors, that have been deployed in the city of Zurich from the Carbosense project. The goal of the exercise is twofold: 
#
# 1. Learn how to deal with real world sensor timeseries data, and organize them efficiently using python dataframes.
#
# 2. Apply data science tools to model the measurements, and use the learned model to process them (e.g., detect drifts in the sensor measurements). 
#
# The sensor network consists of 46 sites, located in different parts of the city. Each site contains three different sensors measuring (a) **CO2 concentration**, (b) **temperature**, and (c) **humidity**. Beside these measurements, we have the following additional information that can be used to process the measurements: 
#
# 1. The **altitude** at which the CO2 sensor is located, and the GPS coordinates (latitude, longitude).
#
# 2. A clustering of the city of Zurich in 17 different city **zones** and the zone in which the sensor belongs to. Some characteristic zones are industrial area, residential area, forest, glacier, lake, etc.
#
# ## Prior knowledge
#
# The average value of the CO2 in a city is approximately 400 ppm. However, the exact measurement in each site depends on parameters such as the temperature, the humidity, the altitude, and the level of traffic around the site. For example, sensors positioned in high altitude (mountains, forests), are expected to have a much lower and uniform level of CO2 than sensors that are positioned in a business area with much higher traffic activity. Moreover, we know that there is a strong dependence of the CO2 measurements on temperature and humidity.
#
# Given this knowledge, you are asked to define an algorithm that curates the data, by detecting and removing potential drifts. **The algorithm should be based on the fact that sensors in similar conditions are expected to have similar measurements.** 
#
# ## To start with
#
# The following csv files in the `../data/carbosense-raw/` folder will be needed: 
#
# 1. `CO2_sensor_measurements.csv`
#     
#    __Description__: It contains the CO2 measurements `CO2`, the name of the site `LocationName`, a unique sensor identifier `SensorUnit_ID`, and the time instance in which the measurement was taken `timestamp`.
#     
# 2. `temperature_humidity.csv`
#
#    __Description__: It contains the temperature and the humidity measurements for each sensor identifier, at each timestamp `Timestamp`. For each `SensorUnit_ID`, the temperature and the humidity can be found in the corresponding columns of the dataframe `{SensorUnit_ID}.temperature`, `{SensorUnit_ID}.humidity`.
#     
# 3. `sensor_metadata_updated.csv`
#
#    __Description__: It contains the name of the site `LocationName`, the zone index `zone`, the altitude in meters `altitude`, the longitude `LON`, and the latitude `LAT`. 
#
# Import the following python packages:

# %%
import pandas as pd
import numpy as np
import sklearn
import plotly.express as px
import plotly.graph_objects as go
import os

# %%
pd.options.mode.chained_assignment = None

# %% [markdown]
# ## PART I: Handling time series with pandas (10 points)

# %% [markdown]
# ### a) **8/10**
#
# [Merge](https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html) the `CO2_sensor_measurements.csv`, `temperature_humidity.csv`, and `sensors_metadata_updated.csv`, into a single dataframe. 
#
# * The merged dataframe contains:
#     - index: the time instance `timestamp` of the measurements
#     - columns: the location of the site `LocationName`, the sensor ID `SensorUnit_ID`, the CO2 measurement `CO2`, the `temperature`, the `humidity`, the `zone`, the `altitude`, the longitude `lon` and the latitude `lat`.
#
# | timestamp | LocationName | SensorUnit_ID | CO2 | temperature | humidity | zone | altitude | lon | lat |
# |:---------:|:------------:|:-------------:|:---:|:-----------:|:--------:|:----:|:--------:|:---:|:---:|
# |    ...    |      ...     |      ...      | ... |     ...     |    ...   |  ... |    ...   | ... | ... |
#
#
#
# * For each measurement (CO2, humidity, temperature), __take the average over an interval of 30 min__. 
#
# * If there are missing measurements, __interpolate them linearly__ from measurements that are close by in time.
#
# __Hints__: The following methods could be useful
#
# 1. ```python 
# pandas.DataFrame.resample()
# ``` 
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.resample.html
#     
# 2. ```python
# pandas.DataFrame.interpolate()
# ```
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.interpolate.html
#     
# 3. ```python
# pandas.DataFrame.mean()
# ```
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.mean.html
#     
# 4. ```python
# pandas.DataFrame.append()
# ```
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.append.html

# %%
FOLDER = '../data/carbosense-raw/'

# %%
CO2_sensor_measurements = pd.read_csv(FOLDER + 'CO2_sensor_measurements.csv', sep ='\t') 
CO2_sensor_measurements.head(2)

# %%
CO2_sensor_measurements.timestamp = pd.to_datetime(CO2_sensor_measurements.timestamp)
CO2_sensor_measurements = CO2_sensor_measurements.set_index('timestamp')

# %%
CO2_sensor_measurements = CO2_sensor_measurements.groupby(['LocationName', 'SensorUnit_ID']).resample('30T').agg({'CO2': 'mean'}).reset_index(['LocationName', 'SensorUnit_ID'])
CO2_sensor_measurements = CO2_sensor_measurements.interpolate(method='linear')

# %%
CO2_sensor_measurements.index.min()

# %%
CO2_sensor_measurements.index.max()

# %%
temperature_humidity = pd.read_csv(FOLDER + 'temperature_humidity.csv', sep ='\t').rename(columns={'Timestamp':'timestamp', 'LON':'lon'})
temperature_humidity.head(2)

# %%
temperature_humidity.timestamp = pd.to_datetime(temperature_humidity.timestamp)

# %%
temperature_humidity2 = temperature_humidity.melt(id_vars='timestamp')
temperature_humidity2['SensorUnit_ID'] = temperature_humidity2.variable.apply(lambda x: int(x[:4]))
temperature_humidity2.head(5)

# %%
temperature = temperature_humidity2.loc[temperature_humidity2['variable'].str.contains("temperature", case=False)]
temperature = temperature.drop(columns='variable')
temperature = temperature.rename(columns={"value": "temperature"})
temperature.head(5)

# %%
humidity = temperature_humidity2.loc[temperature_humidity2['variable'].str.contains("humidity", case=False)]
humidity = humidity.drop(columns='variable')
humidity = humidity.rename(columns={"value": "humidity"})
humidity.head(5)

# %%
temperature_humidity_clean = pd.merge(temperature, humidity, left_on=['timestamp', 'SensorUnit_ID'], right_on=['timestamp', 'SensorUnit_ID'], how='outer').sort_values(by=['timestamp','SensorUnit_ID']).reset_index(drop=True)
temperature_humidity_clean.head(5)


# %%
temperature_humidity_clean = temperature_humidity_clean.set_index('timestamp')

# %%
temperature_humidity_clean = temperature_humidity_clean.groupby('SensorUnit_ID').resample('30T').agg({'temperature': 'mean', 'humidity': 'mean'}).reset_index('SensorUnit_ID')
temperature_humidity_clean = temperature_humidity_clean.interpolate(method='linear')
temperature_humidity_clean.head(5)

# %%
C02_temp_hum = pd.merge(CO2_sensor_measurements.reset_index(), temperature_humidity_clean.reset_index(), left_on=['timestamp', 'SensorUnit_ID'], right_on=['timestamp', 'SensorUnit_ID'], how='outer')
C02_temp_hum = C02_temp_hum.sort_values(by=['SensorUnit_ID', 'timestamp']).reset_index(drop=True)
C02_temp_hum.head(5)

# %%
sensors_metadata_updated = pd.read_csv(FOLDER + 'sensors_metadata_updated.csv').drop(columns=['Unnamed: 0']).rename(columns={'LAT':'lat', 'LON':'lon'})
sensors_metadata_updated.head(5)

# %%
df = pd.merge(C02_temp_hum, sensors_metadata_updated, left_on='LocationName', right_on='LocationName').set_index('timestamp')
df = df[['LocationName', 'SensorUnit_ID', 'CO2', 'temperature', 'humidity', 'zone', 'altitude', 'lon', 'lat']]
df.head(5)


# %%
df.isna().sum().sum()

# %% [markdown]
# ### b) **2/10** 
#
# Export the curated and ready to use timeseries to a csv file, and properly push the merged csv to Git LFS.

# %%
df.to_csv(FOLDER + "merged_CO2.csv")

# %%
# #!git lfs track ../data/carbosense-raw/merged_CO2.csv
# #!git add .gitattributes
# #!git commit -m 'merged_CO2.csv'
# #!git push

# %% [markdown]
# ## PART II: Data visualization (15 points)

# %% [markdown]
# ### a) **5/15** 
# Group the sites based on their altitude, by performing K-means clustering. 
# - Find the optimal number of clusters using the [Elbow method](https://en.wikipedia.org/wiki/Elbow_method_(clustering)). 
# - Write out the formula of metric you use for Elbow curve. 
# - Perform clustering with the optimal number of clusters and add an additional column `altitude_cluster` to the dataframe of the previous question indicating the altitude cluster index. 
# - Report your findings.
#
# __Note__: [Yellowbrick](http://www.scikit-yb.org/) is a very nice Machine Learning Visualization extension to scikit-learn, which might be useful to you. 

# %%
from sklearn.cluster import KMeans
from sklearn.datasets import make_blobs
from yellowbrick.cluster import KElbowVisualizer

# %%
# Get altitude values
X = df.altitude.values.reshape(-1, 1)

# Search optimal number of clusters for K-means clustering

    # Instantiate the clustering model and visualizer
model = KMeans(init="k-means++", n_init=10)
visualizer = KElbowVisualizer(model, k=(2,10), timings=False)

    # Fit the data to the visualizer
visualizer.fit(X)  

    # Finalize and render the figure
visualizer.show()        

# %% [markdown]
# Here the scoring parameter metric was set to **distortion**, which computes the **sum of squared distances from each point to its assigned center**.   
# We will use **k=4**.

# %%
# Perform clustering with k=4
kmeans = KMeans(n_clusters=4, random_state=0, n_init=10, init="k-means++").fit(X)

# Add an additional column altitude_cluster to the dataframe of the previous question indicating the altitude cluster index
df['altitude_cluster'] = kmeans.labels_
df.head(5)


# %%
# Report findings: see what LocationName in which cluster
df[['LocationName', 'altitude_cluster']].reset_index(drop=True).drop_duplicates().groupby(by='altitude_cluster').agg({'LocationName': lambda x: x.values})

# %% [markdown]
# ### b) **4/15** 
#
# Use `plotly` (or other similar graphing libraries) to create an interactive plot of the monthly median CO2 measurement for each site with respect to the altitude. 
#
# Add proper title and necessary hover information to each point, and give the same color to stations that belong to the same altitude cluster.

# %%
# Calculate the monthly median CO2 measurement for each site with respect to the altitude and round the result
df_monthly_median_co2 = df.groupby(['LocationName', 'altitude']).resample('1M').agg({'CO2': 'median', 'altitude_cluster': 'mean'}).reset_index()
df_monthly_median_co2 = df_monthly_median_co2.round({'CO2':1})
df_monthly_median_co2.head()

# %%
# Plot the monthly median CO2 measurement for each site with respect to the altitude

fig = px.scatter(df_monthly_median_co2, x='altitude', y='CO2', color='altitude_cluster', 
                 labels={'altitude':'Altitude','median_CO2':'CO2', "altitude_cluster":"Cluster", "LocationName":"Name"}, 
                 hover_data=['LocationName'])

fig.update_layout(yaxis_title='Median CO2 over the month',
                xaxis_title='Altitude',
                title='CO2 on each site with clustering based on the altitude',
                hovermode="x")

# %% [markdown]
# ### c) **6/15**
#
# Use `plotly` (or other similar graphing libraries) to plot an interactive time-varying density heatmap of the mean daily CO2 concentration for all the stations. Add proper title and necessary hover information.
#
# __Hints:__ Check following pages for more instructions:
# - [Animations](https://plotly.com/python/animations/)
# - [Density Heatmaps](https://plotly.com/python/mapbox-density-heatmaps/)

# %%
# Calculate the mean daily CO2 concentration for all the stations
df_day_co2 = df.groupby('LocationName').resample('1D').agg({'CO2': 'mean', 'lat': 'mean', 'lon': 'mean', 'SensorUnit_ID': 'first', 'altitude': 'mean', 'altitude_cluster': 'first'}).reset_index().sort_values(by=['timestamp','LocationName']).reset_index(drop=True)
df_day_co2['day'] = df_day_co2.timestamp.dt.day
df_day_co2.head()

# %%
# Plot the time-varying density heatmap of the mean daily CO2 concentration for all the stations

fig = px.density_mapbox(df_day_co2, lat='lat', lon='lon', z='CO2', radius=20, center=dict(lat=df_day_co2.lat[0], lon=df_day_co2.lon[0]), zoom=10, mapbox_style='stamen-terrain', animation_frame='day', 
                        title='Density heatmap of the mean daily CO2 concentration for all the stations during October 2017', hover_name='SensorUnit_ID', hover_data=['altitude', 'altitude_cluster', 'LocationName'])

fig.update_layout(autosize=False, width=900, height=600)
fig.show()

# %% [markdown]
# ## PART III: Model fitting for data curation (35 points)

# %% [markdown]
# ### a) **2/35**
#
# The domain experts in charge of these sensors report that one of the CO2 sensors `ZSBN` is exhibiting a drift on Oct. 24. Verify the drift by visualizing the CO2 concentration of the drifting sensor and compare it with some other sensors from the network. 

# %%
drifting_sensor = CO2_sensor_measurements[(CO2_sensor_measurements['LocationName'].isin(['ZSBN','ZPFW']))]

# %%
fig=px.line(drifting_sensor,x=drifting_sensor.index,y='CO2',color ='LocationName',labels={
                'timestamp':'Date',
                'CO2':'CO2 (ppm)',
                'LocationName':'Sensors',
            },title='CO2 Level in October 2017')
fig.show()

# %% [markdown]
# ### b) **8/35**
#
# The domain experts ask you if you could reconstruct the CO2 concentration of the drifting sensor had the drift not happened. You decide to:
# - Fit a linear regression model to the CO2 measurements of the site, by considering as features the covariates not affected by the malfunction (such as temperature and humidity)
# - Create an interactive plot with `plotly` (or other similar graphing libraries):
#     - the actual CO2 measurements
#     - the values obtained by the prediction of the linear model for the entire month of October
#     - the __confidence interval__ obtained from cross validation
# - What do you observe? Report your findings.
#
# __Note:__ Cross validation on time series is different from that on other kinds of datasets. The following diagram illustrates the series of training sets (in orange) and validation sets (in blue). For more on time series cross validation, there are a lot of interesting articles available online. scikit-learn provides a nice method [`sklearn.model_selection.TimeSeriesSplit`](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html).
#
# ![ts_cv](https://player.slideplayer.com/86/14062041/slides/slide_28.jpg)

# %%
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import TimeSeriesSplit


# %%
def bootstrap(values):
    x = np.random.choice(values, size=(len(values), 10000), replace=True)
    m = np.mean(x, axis=0)
    lower, upper = np.quantile(m, q=[0.025, 0.975])
    return values.mean(), lower, upper


# %%
df_ZSBN = df[df.LocationName == 'ZSBN']

X = df_ZSBN[['humidity', 'temperature']]
y = df_ZSBN.CO2.values

X_train, y_train = X[df_ZSBN.index < '2017-10-24'], y[df_ZSBN.index < '2017-10-24']

df_ZSBN['reg_CO2'] = LinearRegression().fit(X_train, y_train).predict(X)


# %%
def cv_reg(X_train, y_train, df_ZSBN):

    # Timeseries cross validation
    reg = LinearRegression(n_jobs=-1)
    splitter = TimeSeriesSplit(n_splits=20)

    errors = []
    for i, (train_index, test_index) in enumerate(splitter.split(X_train)):
        # Select the train and test 
        X_train_cv = X_train.iloc[train_index]
        y_train_cv = y_train[train_index]

        X_test_cv = X_train.iloc[test_index]
        y_test_cv = y_train[test_index]

        # Train and compute the errors 
        lin_reg = reg.fit(X_train_cv, y_train_cv)
        y_pred = lin_reg.predict(X_test_cv)

        errors.append(np.sqrt(mean_squared_error(y_test_cv, y_pred)))

    # Compute 95% CI via bootstrap
    ci = bootstrap(np.array(errors))
    
        # Plot
    fig = px.line(df_ZSBN, x=df_ZSBN.index, y=['CO2', 'reg_CO2'],
                  title='CO2 data for ZSBN station in function of time')

    fig.add_traces([go.Scatter(x = df_ZSBN.index, y = df_ZSBN['reg_CO2'] + (ci[1] - ci[0]),
                               mode = 'lines', line_color = 'rgba(0,0,0,0)',
                               showlegend = False),
                    go.Scatter(x = df_ZSBN.index, y = df_ZSBN['reg_CO2'] + (ci[2] - ci[0]),
                               mode = 'lines', line_color = 'rgba(0,0,0,0)',
                               name = '95% confidence interval',
                               fill='tonexty', fillcolor = 'rgba(255, 0, 0, 0.2)')])

    fig.show()
    
    print(f'95% Confidence Interval for Root Mean Square Error : [{ci[1]:2f}; {ci[2]:2f}]')
    print(f'Average Root Mean Square Error : {ci[0]:2f}')


# %%
cv_reg(X_train, y_train, df_ZSBN)

# %% [markdown]
# ### c) **10/35**
#
# In your next attempt to solve the problem, you decide to exploit the fact that the CO2 concentrations, as measured by the sensors __experiencing similar conditions__, are expected to be similar.
#
# - Find the sensors sharing similar conditions with `ZSBN`. Explain your definition of "similar condition".
# - Fit a linear regression model to the CO2 measurements of the site, by considering as features:
#     - the information of provided by similar sensors
#     - the covariates associated with the faulty sensors that were not affected by the malfunction (such as temperature and humidity).
# - Create an interactive plot with `plotly` (or other similar graphing libraries):
#     - the actual CO2 measurements
#     - the values obtained by the prediction of the linear model for the entire month of October
#     - the __confidence interval__ obtained from cross validation
# - What do you observe? Report your findings.

# %%
similar_sensors = df.loc[(df.altitude_cluster == df_ZSBN['altitude_cluster'][0]) & (df.zone == df_ZSBN['zone'][0])]
similar_sensors.head()

# %%
X = similar_sensors[['LocationName', 'CO2', 'humidity', 'temperature', 'altitude', 'lon', 'lat']].reset_index()
y = df_ZSBN.CO2.values

X = X.pivot(index='timestamp', columns='LocationName').drop(columns=[('CO2', 'ZSBN')])

X_train, y_train = X[df_ZSBN.index < '2017-10-24'], y[df_ZSBN.index < '2017-10-24']

df_ZSBN['reg_CO2'] = LinearRegression().fit(X_train, y_train).predict(X)

# %%
cv_reg(X_train, y_train, df_ZSBN)

# %% [markdown]
# ### d) **10/35**
#
# Now, instead of feeding the model with all features, you want to do something smarter by using linear regression with fewer features.
#
# - Start with the same sensors and features as in question c)
# - Leverage at least two different feature selection methods
# - Create similar interactive plot as in question c)
# - Describe the methods you choose and report your findings

# %%
from sklearn.feature_selection import SelectFromModel
from sklearn.feature_selection import RFE

# %%
lreg = LinearRegression().fit(X_train, y_train)
model = SelectFromModel(lreg, prefit=True)
X_train_SFM = pd.DataFrame(model.transform(X_train), columns=X_train.columns[model.get_support()])
X_SFM = pd.DataFrame(model.transform(X), columns=X.columns[model.get_support()])
X_SFM.head()

# %%
df_ZSBN['reg_CO2'] = LinearRegression().fit(X_train_SFM, y_train).predict(X_SFM)

# %%
cv_reg(X_train_SFM, y_train, df_ZSBN)

# %%
rfe = RFE(estimator=lreg, n_features_to_select=3, step=1)
rfe = rfe.fit(X_train, y_train)
X_train_RFE = pd.DataFrame(rfe.transform(X_train), columns=X_train.columns[rfe.get_support()])
X_RFE = pd.DataFrame(rfe.transform(X), columns=X.columns[rfe.get_support()])
X_RFE.head()

# %%
df_ZSBN['reg_CO2'] = LinearRegression().fit(X_train_RFE, y_train).predict(X_RFE)

# %%
cv_reg(X_train_RFE, y_train, df_ZSBN)

# %% [markdown]
# ### e) **5/35**
#
# Eventually, you'd like to try something new - __Bayesian Structural Time Series Modelling__ - to reconstruct counterfactual values, that is, what the CO2 measurements of the faulty sensor should have been, had the malfunction not happened on October 24. You will use:
# - the information of provided by similar sensors - the ones you identified in question c)
# - the covariates associated with the faulty sensors that were not affected by the malfunction (such as temperature and humidity).
#
# To answer this question, you can choose between a Python port of the CausalImpact package (such as https://github.com/jamalsenouci/causalimpact).
#
# Before you start, watch first the [presentation](https://www.youtube.com/watch?v=GTgZfCltMm8) given by Kay Brodersen (one of the creators of the causal impact implementation in R), and this introductory [ipython notebook](https://github.com/jamalsenouci/causalimpact/blob/HEAD/GettingStarted.ipynb) with examples of how to use the python package.
#
# Note, if you are having issues installing the package, try `pip install --user causalimpact` and do not forget to update your requirements.txt. 
#
# - Report your findings:
#     - Is the counterfactual reconstruction of CO2 measurements significantly different from the observed measurements?
#     - Can you try to explain the results?

# %%
from causalimpact import CausalImpact

# %%
ts_pre_period = [pd.to_datetime(date) for date in ["2017-10-01", "2017-10-23"]]
ts_post_period = [pd.to_datetime(date) for date in ["2017-10-24", "2017-10-31"]]

# %%
ts_impact = CausalImpact(pd.concat([df_ZSBN.CO2,X], axis=1), ts_pre_period, ts_post_period)
ts_impact.run()

# %%
ts_impact.plot()


# %% [markdown]
# # That's all, folks!

# %%
