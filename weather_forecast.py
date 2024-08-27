# %%
pip install SQLAlchemy

# %%
pip install pyodbc

# %%
import requests
import pandas as pd
from datetime import datetime,timedelta
from collections import Counter
import sqlalchemy as sa
import numpy as np
from matplotlib import pyplot as plt

# %% [markdown]
# # Connecting to Databse using username and password for authentication.

# %%
connection_url = sa.engine.URL.create(
    drivername = "mssql+pyodbc",
    username   = "alekhyae",       ## Please add your user
    password   = "2024!Schulich",
    host       = "mmai2024-ms-sql-server.c1oick8a8ywa.ca-central-1.rds.amazonaws.com",
    port       = "1433",
    database   = "alekhyae_db",       ## Please add your database
    query = {
        "driver" : "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate" : "yes"
    }
)

# %%
my_engine = sa.create_engine(connection_url)

# %%
# Fetch postal codes with lat and lon from the database
postal_codes_df = pd.read_sql('SELECT * FROM uploads.postal_codes', my_engine)

# %%
#create empty lists to store the data
weather_data_list = []
forecasting_data_list=[]

# %% [markdown]
# # Current Weather

# %%

api_url = 'https://api.openweathermap.org/data/2.5/weather'

#loop through every postal code to fetch and store current weather data
for index, row in postal_codes_df.iterrows():
        my_query_parameters = {
              'lat': row['latitude'],
              'lon': row['longitude'],
        'units': 'metric',
            'appid': '51e1b785d1c53673d1ab963a4ec63b88'
        }

        my_request_headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15'
        }

        response = requests.get(url=api_url, params=my_query_parameters, headers=my_request_headers)
        data = response.json()

        # Print the response data for checking
        print(f"Response for {row['postal_code']}:\n", data, "\n")

        dt = datetime.utcfromtimestamp(data["dt"]).date()
            # Prepare data for DataFrame
        weather_data = {
                "postal_code": row['postal_code'],
                "date": dt,
                "city": data["name"],
                "temperature (°C)": data["main"]["temp"],
                "humidity(%)": data["main"]["humidity"],
                "wind_speed(m/s)": data["wind"]["speed"],
                "weather_description": data["weather"][0]["description"],
                "feels_like(°C)": data["main"]["feels_like"],
                "temp_min(°C)": data["main"]["temp_min"],
                "temp_max(°C)": data["main"]["temp_max"],
                "atmospheric_pressure(hPa)": data["main"]["pressure"],
                "visibility_distance(m)": data["visibility"],
                "cloudiness_percentage": data["clouds"]["all"]
            }

   # Append the weather data to the list
        weather_data_list.append(weather_data)
 

# %%
weather_df = pd.DataFrame(weather_data_list)

# %%
weather_df.head(16)

# %%
weather_df.info()

# %%
weather_df.describe()

# %% [markdown]
# #Feature Engineering

# %%
#creating a  new feature that represents the difference between the maximum and minimum temperatures of the day.
weather_df['temp_diff(°C)'] = weather_df['temp_max(°C)'] - weather_df['temp_min(°C)']

# %%
weather_df.head()

# %% [markdown]
# Catergorizing the weather description into 3 categories

# %%
weather_df['weather_description'].unique()

# %%
def categorize_weather(description):
    if description in ['clear sky']:
        return 'Clear'
    elif description in ['few clouds', 'broken clouds', 'scattered clouds', 'overcast clouds', 'fog', 'haze']:
        return 'Cloudy'
    elif description in ['light rain']:
        return 'Rainy'
    else:
        return 'Unknown'

# %%
weather_df['weather_today'] = weather_df['weather_description'].apply(categorize_weather)

# %%
weather_df.head()

# %%
weather_df.info()

# %% [markdown]
# # Ingesting the data into database in our Microsoft SQL Server

# %%

weather_df.to_sql(

    name   = 'Current_Weather_Data',
    con    = my_engine,
    schema = 'uploads',
    if_exists = 'replace',
    index  = False,
    dtype  = {
        'postal_code':sa.types.VARCHAR[50],
        'date': sa.types.DATE,
        'city' :sa.types.VARCHAR[50],
        'temperature (°C)': sa.types.DECIMAL(10,2),
        'humidity(%)': sa.types.INTEGER,
        'wind_speed(m/s)':sa.types.DECIMAL(10,2),
        'weather_description': sa.types.VARCHAR[50],
        'feels_like(°C)': sa.types.DECIMAL(10,2),
        'temp_min(°C)': sa.types.DECIMAL(10,2) ,
        'temp_max(°C)': sa.types.DECIMAL(10,2) , 
        'atmospheric_pressure(hPa)': sa.types.INTEGER,
        'visibility_distance(m)': sa.types.INTEGER,
        'cloudiness_percentage': sa.types.INTEGER,
        'temp_diff(°C)': sa.types.DECIMAL(10,2) ,
        'weather_today': sa.types.VARCHAR[50],

    },
    method = 'multi'
)

# %%
#function to request the data
my_query = sa.text("SELECT * FROM uploads.Current_Weather_Data;")

with my_engine.connect() as my_connection:
    my_data = pd.read_sql(sql=my_query, con=my_connection)

# %%
my_data

# %% [markdown]
# 

# %% [markdown]
# # Future Weather Forecast

# %%

# API credentials
api_url = 'https://api.openweathermap.org/data/2.5/forecast'


# Loop through each postal code to fetch forecast data
for index, row in postal_codes_df.iterrows():
        my_query_parameters = {
              'lat': row['latitude'],
              'lon': row['longitude'],
              'units': 'metric',
              'appid': '51e1b785d1c53673d1ab963a4ec63b88'
        }

        my_request_headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15'
        }

        response = requests.get(url=api_url, params=my_query_parameters, headers=my_request_headers)
        data1 = response.json()


        city_name = data1['city']['name']  # Get the city name from the response
        population = data1['city']['population']
        sunrise = datetime.utcfromtimestamp(data1['city']['sunrise']).strftime('%H:%M:%S')
        sunset = datetime.utcfromtimestamp(data1['city']['sunset']).strftime('%H:%M:%S')
            

        # Print the response data for checking
        print(f"Response for {row['postal_code']}:\n", data1, "\n")

        for forecast in data1['list']:
                forecast_data = {
                    "postal_code": row['postal_code'],
                    "city": city_name,
                    "date": forecast['dt_txt'].split()[0],  # Extract the date part
                    "temperature(°C)": forecast["main"]["temp"],
                    "humidity(%)": forecast["main"]["humidity"],
                    "wind_speed(m/s)": forecast["wind"]["speed"],
                    "weather_description": forecast["weather"][0]["description"],
                    "feels_like(°C)":  forecast["main"]["feels_like"],
                    "temp_min(°C)": forecast["main"]["temp_min"],
                    "temp_max(°C)": forecast["main"]["temp_max"],
                    "atmospheric_pressure(hPa)": forecast["main"]["pressure"],
                    "cloudiness_percentage": forecast["clouds"]["all"],
                    "population": population,
                    "sunrise": sunrise,
                    "sunset": sunset
                }

                # Append the weather data to the list
                forecasting_data_list.append(forecast_data)

# Create a DataFrame from the weather data list
forecast_df = pd.DataFrame(forecasting_data_list)



# %%
# Aggregate data to display daily weather instead of hourly
def most_common(lst):
    return Counter(lst).most_common(1)[0][0]

# various weather metrics such as temperature, feels like temperature, minimum and maximum temperature, atmospheric pressure, humidity, and wind speed are calculated by  their mean values 
future_forecast_df = forecast_df.groupby(['postal_code', 'city', 'date', 'population', 'sunrise', 'sunset']).agg({
    'temperature(°C)': 'mean',
    'feels_like(°C)' : 'mean',
    "temp_min(°C)"   : 'mean',
    "temp_max(°C)"   : 'mean' ,
    'atmospheric_pressure(hPa)' : 'mean',
    'humidity(%)': 'mean',
    'wind_speed(m/s)': 'mean',
    'cloudiness_percentage':'mean',
    'weather_description': lambda x: most_common(x)
}).reset_index()

# Display the combined weather data for the next 6 days
print(future_forecast_df)

# %%
future_forecast_df.head()

# %%
future_forecast_df.info()

# %%
plt.figure(figsize=(12, 7))

for postal_code in future_forecast_df['postal_code'].unique():
    subset = future_forecast_df[future_forecast_df['postal_code'] == postal_code]
    plt.plot(subset['date'], subset['temperature(°C)'], marker='o', label=postal_code)

plt.title('Temperature Over Time by Postal Code')
plt.xlabel('Date')
plt.ylabel('Temperature (°C)')
plt.legend(title='Postal Code')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %% [markdown]
# The temperature trends over the past week for multiple postal codes in Toronto reveal a consistent pattern. Initial temperatures hovered around 24°C, experiencing a notable dip to around 21°C by August 6, before gradually rising again towards 23°C by August 8. This data suggests a temporary cooling period, potentially influenced by weather fronts or precipitation events. For businesses relying on weather data, such as logistics and retail, this trend indicates a need for adaptive planning to mitigate any potential impacts on operations, customer behavior, and supply chain logistics.

# %%
#categorize the temperature into 2 kinds
future_forecast_df['typeofday'] = future_forecast_df['temperature(°C)'].apply(lambda x: 'warm day' if x >= 20 else 'Chilly Day')

# %%
#dividing the weather description into 3 categories
def categorize_weather(description):
    if description in ['clear sky']:
        return 'Clear'
    elif description in ['few clouds', 'broken clouds', 'scattered clouds', 'overcast clouds', 'fog', 'haze']:
        return 'Cloudy'
    elif description in ['light rain']:
        return 'Rainy'
    else:
        return 'Unknown'

# %%
future_forecast_df['weather_today'] = future_forecast_df['weather_description'].apply(categorize_weather)

# %% [markdown]
# The heat index, which combines air temperature and relative humidity to determine the apparent temperature felt by humans, indicates that despite the temperature variations, there were significant fluctuations in how hot it felt across different areas.

# %%
# Function to calculate heat index
def calculate_heat_index(temp, humidity):
    c1 = -8.78469475556
    c2 = 1.61139411
    c3 = 2.33854883889
    c4 = -0.14611605
    c5 = -0.012308094
    c6 = -0.0164248277778
    c7 = 0.002211732
    c8 = 0.00072546
    c9 = -0.000003582
    heat_index = (c1 + (c2 * temp) + (c3 * humidity) + (c4 * temp * humidity) +
                  (c5 * temp**2) + (c6 * humidity**2) +
                  (c7 * temp**2 * humidity) + (c8 * temp * humidity**2) +
                  (c9 * temp**2 * humidity**2))
    return round(heat_index, 2)

# %%
#Appending new heat index column to the data frame

future_forecast_df['heat_index(°C)'] = future_forecast_df.apply(lambda row: calculate_heat_index(row['temperature(°C)'], row['humidity(%)']), axis=1)


# %%
future_forecast_df.head()

# %%
plt.figure(figsize=(12, 7))

for postal_code in future_forecast_df['postal_code'].unique():
    subset = future_forecast_df[future_forecast_df['postal_code'] == postal_code]
    plt.plot(subset['date'], subset['heat_index(°C)'], marker='o', label=postal_code)

plt.title('Heat Index Over Time by Postal Code')
plt.xlabel('Date')
plt.ylabel('Heat_index(°C)')
plt.legend(title='Postal Code')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %% [markdown]
# The heat index trends over the past week for various postal codes in Toronto show a peak around August 3rd and 4th, reaching approximately 26°C, before experiencing a decline to around 24°C by August 6th, and a slight recovery afterwards.

# %%
future_forecast_df.info()

# %%
my_engine = sa.create_engine(connection_url)

# %%
from sqlalchemy import Table, MetaData, Column, String, Float, Integer, Date, DECIMAL
from datetime import datetime

# Create the metadata object
metadata = MetaData()

# Define the table structure
table = Table('Forecast_Weather_Data', metadata,
    Column('postal_code', String(50)),
    Column('city', String(50)),
    Column('date', Date),
    Column('population', Integer),
    Column('sunrise', String(50)),
    Column('sunset', String(50)),
    Column('temperature(°C)', DECIMAL(10,2)),
    Column('feels_like(°C)', DECIMAL(10,2)),
    Column('temp_min(°C)', DECIMAL(10,2)),
    Column('temp_max(°C)', DECIMAL(10,2)),
    Column('atmospheric_pressure(hPa)', DECIMAL(10,2)),
    Column('humidity(%)', DECIMAL(10,2)),
    Column('wind_speed(m/s)', DECIMAL(10,2)),
    Column('cloudiness_percentage', DECIMAL(10,2)),
    Column('weather_description', String(50)),
    Column('typeofday', String(50)),
    Column('weather_today', String(50)),
    Column('heat_index(°C)', DECIMAL(10,2)),
    schema='uploads'
)

# Insert the data
with my_engine.connect() as conn:
    # Start a transaction
    with conn.begin():
        for _, row in future_forecast_df.iterrows():
            # Convert the row to a dictionary
            row_dict = row.to_dict()
            
            # Convert the date string to a datetime object
            row_dict['date'] = datetime.strptime(row_dict['date'], '%Y-%m-%d').date()
            
            # Insert the row
            ins = table.insert().values(**row_dict)
            conn.execute(ins)

print("Data insertion completed successfully.")

# %%
#function to request the data
my_query = sa.text("SELECT * FROM uploads.Forecast_Weather_Data;")

with my_engine.connect() as my_connection:
    my_forecastdata = pd.read_sql(sql=my_query, con=my_connection)

# %%
my_forecastdata

# %% [markdown]
# # Historic Weather Data

# %%
#create empty lists to store the data
historical_data_list=[]

# %%

# Function to generate the API URL for historical data
def generate_hist_api_url(latitude, longitude, start_date, end_date, variables, timezone="auto"):
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    variable_params = ",".join(variables)
    api_url = f"{base_url}?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&daily={variable_params}&timezone={timezone}"
    return api_url


# %%
# Define function to fetch historical weather data from the API
def fetch_hist_weather_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    data = response.json()
    print("Returned data:", data)
    return data

# Function to extract relevant historical data from the API response
def extract_hist_weather_data(weather_data, postal_code):
    weather_data_list = []

    if 'daily' not in weather_data or 'time' not in weather_data['daily']:
        raise ValueError("No valid data returned from the API.")

    timezone = weather_data.get('timezone', 'Unknown')

    for i, day in enumerate(weather_data['daily']['time']):
        weather_data_list.append({
            "postal_code": postal_code,
            "city": timezone,
            "date": day,
            "max_temp(°C)": weather_data['daily']['temperature_2m_max'][i],
            "min_temp(°C)": weather_data['daily']['temperature_2m_min'][i],
            "mean_temp(°C)": weather_data['daily'].get('temperature_2m_mean', [None]*len(weather_data['daily']['time']))[i],
        })

    return weather_data_list

# Main function to fetch and clean historical weather data for multiple postal codes
def fetch_and_clean_hist_weather_data(postal_codes_df, variables, timezone="auto"):
    # Calculate date range for the past 7 days, excluding the current day
    end_date = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now().date() - timedelta(days=8)).strftime('%Y-%m-%d')

    all_weather_data = []

    for index, entry in postal_codes_df.iterrows():
        api_url = generate_hist_api_url(entry["latitude"], entry["longitude"], start_date, end_date, variables, timezone)
        weather_data = fetch_hist_weather_data(api_url)

        if weather_data:
            try:
                extracted_data = extract_hist_weather_data(weather_data, entry["postal_code"])
                all_weather_data.extend(extracted_data)
            except ValueError as e:
                print(f"Data processing error for postal code {entry['postal_code']}: {e}")

    # Convert to DataFrame
    historical_data_df = pd.DataFrame(all_weather_data)
    return historical_data_df

# Function to generate the API URL for historical data
def generate_hist_api_url(latitude, longitude, start_date, end_date, variables, timezone="auto"):
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    variable_params = ",".join(variables)
    api_url = f"{base_url}?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&daily={variable_params}&timezone={timezone}"
    return api_url

variables = ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]

# Fetch and clean historical weather data
historical_data_df = fetch_and_clean_hist_weather_data(postal_codes_df, variables)

# Check if the DataFrame 'historical_data_df' is not empty and print its contents
if historical_data_df is not None and not historical_data_df.empty:
    # Modify to the desired number of entries
    see_entries = 30
    print(historical_data_df.head(see_entries))
else:
    print("The DataFrame is empty.")

# %%
historical_data_df.head()

# %%
historical_data_df.info()

# %%
# Fill missing values with the mean of the column
historical_data_df['max_temp(°C)'].fillna(historical_data_df['max_temp(°C)'].mean(), inplace=True)
historical_data_df['min_temp(°C)'].fillna(historical_data_df['min_temp(°C)'].mean(), inplace=True)
historical_data_df['mean_temp(°C)'].fillna(historical_data_df['mean_temp(°C)'].mean(), inplace=True)

# %%
# plot graph for teperature over different postal codes
plt.figure(figsize=(12, 7))

for postal_code in historical_data_df['postal_code'].unique():
    subset = historical_data_df[historical_data_df['postal_code'] == postal_code]
    plt.plot(subset['date'], subset['mean_temp(°C)'], marker='o', label=postal_code)

plt.title('Temperature Over Time by Postal Code')
plt.xlabel('Date')
plt.ylabel('Mean Temperature (°C)')
plt.legend(title='Postal Code')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %% [markdown]
# The temperature trends across multiple postal codes in the area demonstrate consistent daily fluctuations over the observed period. Notably, there is a significant peak in mean temperature on July 31, 2024, suggesting a particularly warm day. 

# %%
#Ingesting the data into database in our Microsoft SQL Server
historical_data_df.to_sql(
    name   = 'Historic_Weather_Data',
    con    = my_engine,
    schema = 'uploads',
    if_exists = 'replace',
    index  = False,
    dtype  = {
        'postal_code':sa.types.VARCHAR[50],
        'city' :sa.types.VARCHAR[50],
        'date' :sa.types.DATE,
        'max_temp(°C)': sa.types.DECIMAL(10,2) ,
        'min_temp(°C)': sa.types.DECIMAL(10,2) , 
        'mean_temp(°C)' : sa.types.DECIMAL(10,2)

    },
    method = 'multi'
)

# %%
#function to request the data
my_query = sa.text("SELECT * FROM uploads.Historic_Weather_Data;")

with my_engine.connect() as my_connection:
    my_historicdata = pd.read_sql(sql=my_query, con=my_connection)

# %%
my_historicdata


