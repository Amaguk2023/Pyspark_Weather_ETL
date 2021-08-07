import requests
from pyspark.sql import SparkSession
import json
import weather_transform
import weather_load
import datetime
from datetime import datetime
from pyspark.sql.functions import lit

#API WEBPAGE -> https://rapidapi.com/weatherbit/api/weather/

if __name__=='__main__':

	ss = SparkSession.builder.appName('Weather ETL').getOrCreate()
	#CONSTRAINTS
	TOKEN = ''
	HOST = ''
	URL = 'https://weatherbit-v1-mashape.p.rapidapi.com/current'

	#API CREDENTIALS
	headers = {
		"x-rapidapi-key": "{TOKEN}".format(TOKEN=TOKEN),
		"x-rapidapi-host": "{HOST}".format(HOST=HOST)
	}

	location_1_data = {
		"lon": " ",
		"lat": " "
	}

	location_2_data = {
		"lon": " ",
		"lat": " "
	}

	#LOCATION 1 CURRENT WEATHER REQUEST
	location_1 = requests.get(URL, headers=headers, params=location_1_data).json()

	#SAVE LOCATION 1 GET REQUEST 
	loc1_json = open('location_1_weather.json', 'w')
	json.dump(location_1, loc1_json, indent=2)
	loc_1_json.close()

    #CREATE LOCATION 1 PYSPARK DF FROM JSONFILE
	location_1_json = ss.read.option("multiline","true").json('location_1_weather.json')
	loc_1 = location_1_json.select("data.temp", "data.weather.description", "data.timezone") 
	loc_1 = loc_1.withColumn("datetime", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) 
	loc_1.show(truncate=0)


	#CHANGE PYSPARK DF TO PANDAS DF
	loc_1_p = loc_1.toPandas()
	loc_1_p['temp'] = loc_1_p['temp'].str.get(0)
	loc_1_p['description'] = loc_1_p['description'].str.get(0)
	loc_1_p['timezone'] = loc_1_p['timezone'].str.get(0)
	
	
	#VALIDATION STAGE
	if weather_transform.data_validation(loc_1_p):
		weather_load.location_1_postgre_engine_load(loc_1_p)
	

	#LOCATION 2 CURRENT WEATHER REQUEST
	location_2 = requests.get(URL, headers=headers,params=sdq_data).json()

	#SAVE LOCATION 2 GET REQUEST 
	loc2_json = open('location_2_weather.json', 'w')
	json.dump(location_2, loc2_json, indent=2)
	loc_2_json.close()

	#CREATE LOCATION 2 PYSPARK DF FROM JSONFILE
	location_2_json = ss.read.option("multiline", "true").json('location_2_weather.json')
	loc_2 = location_2_json.select("data.temp", "data.weather.description", "data.timezone")
	loc_2 = loc_2.withColumn("datetime", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) 
	loc_2.show(truncate=0)
	
	
	#CHANGE PYSPARK DF TO PANDAS DF
	loc_2_p = loc_1.toPandas()
	loc_2_p['temp'] = loc_2_p['temp'].str.get(0)
	loc_2_p['description'] = loc_2_p['description'].str.get(0)
	loc_2_p['timezone'] = loc_2_p['timezone'].str.get(0)
	

	#VALIDATION STAGE
	if weather_transform.data_validation(loc_2_p):
		weather_load.location_2_postgre_engine_load(loc_2_p)



	


	
