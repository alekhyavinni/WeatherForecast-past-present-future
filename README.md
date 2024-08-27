# WeatherForecast-past-present-future

The goal of this project was to pull via API information from third parties regarding the Current Weather, Weather Forecasts, and Historical Weather Data. To accomplish this, the
Open Weather API was used for Current Weather and Weather Forecasts, while the API from
Open Meteo was used for collecting Historical Weather Data. The data for Current Weather and
Weather Forecasts was collected for the day and then replaced. Data pulled via API was
processed by a python script to create appropriate features and then push the processed data
to a Microsoft SQL Server database set up for our MMAI 5100 course. The processed data is
added to tables corresponding to one of the three categories of Current Weather
(Current_Weather_Data), Weather Forecasts (Forecast_Weather_Data), and Historical Data
(Historical_Weather_Data).
Once run, the python script works on a schedule, with updates being pushed to the SQL
database at 6am each day. Our implementation could be useful for researchers requiring daily
and historical meteorological insights. As well as by commercial users in sectors such as
logistics, which may benefit from daily and historical weather insights to inform delivery
planning.
The python script is designed to run autonomously, when ingesting data such as postal
code, latitude, and longitude. It does so from a Microsoft SQL Server database table called
postal_codes under the uploads schema. This way location data can be added, changed, or
deleted without modifying the python script itself. The postal code, latitude, and longitude data
are then used as inputs for the Open Weather API to pull data for Current Weather, Weather
Forecasts, and Historical Weather Data via the Open Meteo API. To improve the insights
gleaned from the data, the feature temp_diff(°C) was engineered. This feature calculates the
difference between the maximum and minimum temperatures for the day, revealing the variation
in temperature during that period. Weather conditions were classified as one of either Clear,
Cloudy, or Rainy with Unknown handling any exceptions to the three categories.
The weather forecast API responses from Open Weather also included the city name
and population in the response. This allowed for adding programmatically for each postal code,
latitude, and longitude the corresponding city and population. Temperature data per postal code
also enabled the creation of informative visualizations that reveal trends over time. Such trend
tracking could inform logistical planning or time-sensitive meteorological research work.
Challenges
While completing this assignment, two challenges arose that complicated our work. The
first challenge was that of duplicate data. The temperature data ingested for Current Weather
was hourly and did not change much, contributing to a collection of redundant data that
provided little additional value. To mitigate this, the mean of these temperature data was
calculated and stored. A second challenge was the differences between the Open Weather API
and Open Meteo API. The Open Meteo API provided fewer additional data points compared to
the Open Weather API. This inconsistency was mitigated by focusing on important temperature
data points such as the maximum, minimum, and mean temperatures for the previous 7 days.
This ensured consistency in regards to temperature data being recorded for all three tables
