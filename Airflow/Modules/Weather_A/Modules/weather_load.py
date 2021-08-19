from sqlalchemy import create_engine
from sqlalchemy import exc
import pandas as pd

#LOADS DFS TO THE SAME TABLE

def wa_postgre_engine_load(wa_p):
    wa_engine = create_engine('postgresql+psycopg2://', connect_args={'sslmode':'allow'})
    try:
        wa_p.to_sql("wa_weather", santo_domingo_engine, index=False, if_exists='append')
        print('\n* wa weather ETL process complete *\n')
    except exc.IntegrityError: 
        print('* wa data already exists in the database, terminating ETL process *\n') 