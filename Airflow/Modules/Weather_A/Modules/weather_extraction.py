import requests
from pyspark.sql import SparkSession
import json
from Weather_A import weather_transform
from Weather_A import weather_load
import datetime
from datetime import datetime
from pyspark.sql.functions import lit

def wextraction():
	ss = SparkSession.builder.appName('Weather ETL').getOrCreate()
	#CONSTRAINTS
	TOKEN = ''
	HOST = ''
	URL = ''

	#Weather A, SPAIN
	headers = {
		"x-rapidapi-key": "{TOKEN}".format(TOKEN=TOKEN),
		"x-rapidapi-host": "{HOST}".format(HOST=HOST)
	}

	wa_data = {
		"lon": "",
		"lat": ""
	}

	#Weather A CURRENT WEATHER REQUEST
	weather_a = requests.get(URL, headers=headers,params=sdq_data).json()

	#SAVE Weather A GET REQUEST 
	wa_json = open('.json', 'w')
	json.dump(weather_a, wa_json, indent=2)
	wa_json.close()

	#CREATE Weather A PYSPARK DF FROM JSONFILE
	weather_a_json = ss.read.option("multiline", "true").json('wa_weather.json')
	wa = weather_a_json.select("data.temp", "data.weather.description", "data.timezone", "data.ob_time")
	#wa = wa.withColumn("datetime", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) #ADD NEW COLUMN TO PYSPARK
	wa.show(truncate=0)


	#CHANGE PYSPARK DF TO PANDAS
	wa_p = wa.toPandas()
	wa_p['temp'] = wa_p['temp'].str.get(0)
	wa_p['description'] = wa_p['description'].str.get(0)
	wa_p['timezone'] = wa_p['timezone'].str.get(0)
	wa_p['ob_time'] = wa_p["ob_time"].str_get(0)


	#VALIDATION STAGE
	if weather_transform.data_validation(wa_p):
		weather_load.wa_postgre_engine_load(wa_p)

wextraction() 