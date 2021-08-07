from sqlalchemy import create_engine
from sqlalchemy import exc
import pandas as pd

#DF LOAD
def location_1_postgre_engine_load(loc_1_p):
    location_1_engine = create_engine('postgresql+psycopg2://', connect_args={'sslmode':'allow'})

    try:
        loc_1_p.to_sql("loc_1_weather", location_1_engine, index=False, if_exists='append')
        print('\n* Location 1 weather ETL process complete *\n')
    except exc.IntegrityError: 
        print('* Location 1 data already exists in the database, terminating ETL process *\n')  

def location_2_postgre_engine_load(loc_2_p):
    location_2_engine = create_engine('postgresql+psycopg2://', connect_args={'sslmode':'allow'})
    try:
        sdf.to_sql("location_2_weather", location_2_engine, index=False, if_exists='append')
        print('\n* Location 2 weather ETL process complete *\n')
    except exc.IntegrityError: 
        print('* Location 2 data already exists in the database, terminating ETL process *\n')  

