from sqlalchemy import create_engine
from sqlalchemy import exc
import pandas as pd

#LOADS DFS TO THE SAME TABLE
def wb_postgre_engine_load(wbdf_p):
    wb_engine = create_engine('postgresql+psycopg2://', connect_args={'sslmode':'allow'})

    try:
        wbdf_p.to_sql("wb_weather", wb_engine, index=False, if_exists='append')
        print('\n* wb weather ETL process complete *\n')
    except exc.IntegrityError: 
        print('* wb data already exists in the database, terminating ETL process *\n')  
 

