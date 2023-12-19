import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.point import Point
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time
import dask.dataframe as dd
from geopy.extra.rate_limiter import RateLimiter

start_time=time.time()

geolocator=Nominatim(user_agent='ootodomthisanalysis')
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

engine=create_engine(URL(

                account='#####',
                user='#####',
                password='####',
                database='#####',
                schema='####',
                warehouse='####'))

with engine.connect() as conn:
    try:
        query = """
    SELECT RN, CONCAT(latitude, ',', longitude) AS LOCATION
    FROM (
        SELECT RN,
            SUBSTR(location, REGEXP_INSTR(location, ' ', 1, 4) + 1) AS LATITUDE,
            SUBSTR(location, REGEXP_INSTR(location, ' ', 1, 1) + 1, (REGEXP_INSTR(location, ' ', 1, 2) - REGEXP_INSTR(location, ' ', 1, 1) - 1)) AS LONGITUDE
        FROM OOTODOM_DATA_SHORT_FLATTEN
        WHERE RN BETWEEN 1 AND 100
        ORDER BY RN
    )
"""


        print("-----%s---" % (time.time()-start_time))

        #Result Set query is converted into a pandas dataframe
        df=pd.read_sql(query,conn)

        #The columns need to be converted to uppercase since we have used to_sql funciton which expects all the columns names to be in capital letters.
        df.columns=map(lambda x:str(x).upper(),df.columns)

        #We have used dask dataframe in order to ensure that multiple pandas dataframe run in a single instance.This would help when computations in pandas are performed since a dask dataframe is much faster than pandas dataframe. 
        ddf=dd.from_pandas(df,npartitions=10)
        print(ddf.head(5,npartitions=-1))

        #A new column address is added in this we apply lambda function to the location column with the help of apply method the api geolocator.revrse(x) basiclaly accepts latitutde and longitutde and returns a diciitonary pertaining to that latitude and longititude.
        ddf['ADDRESS']=ddf['LOCATION'].apply(geocode, meta=(None, "str")).apply(lambda x: geolocator.reverse(x).raw['address'],meta=(None,'str'))
        print("-----%s seconds------" % (time.time()-start_time))
        
    
        #Transferring the dask dataframe into a pandas dataframe .
        pandas_df=ddf.compute()
        print(pandas_df.head())
        print("------%s seconds---- " % (time.time()-start_time))

        pandas_df.to_sql('ootodom_data_flatten_address',con=engine,if_exists='append',index=False,chunksize=2000,method=pd_writer)

    except Exception as  e:     #Error catching
        print('---Error----',e)

    finally :
        conn.close()             #Closing connection 

engine.dispose()                 #the engine made gets disposed     

