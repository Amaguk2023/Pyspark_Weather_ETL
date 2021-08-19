import requests
from pyspark.sql import SparkSession
import json
from Weather_B import weather_transform
from Weather_B import weather_load
import datetime
from datetime import datetime
from pyspark.sql.functions import lit



def wextraction():
	ss = SparkSession.builder.appName('Weather ETL').getOrCreate()
	#CONSTRAINTS
	TOKEN = ''
	HOST = ''
	URL = 'https://weatherbit-v1-mashape.p.rapidapi.com/current'

	#Weather B, SPAIN
	headers = {
		"x-rapidapi-key": "{TOKEN}".format(TOKEN=TOKEN),
		"x-rapidapi-host": "{HOST}".format(HOST=HOST)
	}

	wb_data = {
		"lon": "",
		"lat": ""
	}

	#Weather B CURRENT WEATHER REQUEST
	weather_b = requests.get(URL, headers=headers, params=wb_data).json()

	#SAVE Weather B GET REQUEST 
	wb_json = open('.json', 'w')
	json.dump(weather_b, wb_json, indent=2)
	wb_json.close()

    #CREATE Weather B PYSPARK DF FROM JSONFILE
	weather_b_json = ss.read.option("multiline","true").json('.json')
	wbdf = wb_json.select("data.temp", "data.weather.description", "data.timezone") 
	wbdf = wbdf.withColumn("datetime", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) #ADD NEW COLUMN
	wbdf.show(truncate=0)


	#CHANGE PYSPARK DF TO PANDAS
	wbdf_p = wbdf.toPandas()
	wbdf_p['temp'] = wbdf_p['temp'].str.get(0)
	wbdf_p['description'] = wbdf_p['description'].str.get(0)
	wbdf_p['timezone'] = wbdf_p['timezone'].str.get(0)
	

	#VALIDATION STAGE
	if weather_transform.data_validation(wbdf_p):
		weather_load.madrid_postgre_engine_load(wbdf_p)

wextraction()



	


	
