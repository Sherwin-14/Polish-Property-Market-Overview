import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

start_time=time.time()

engine=create_engine(URL(

                     account='####',
                     user='####',
                     password='####',
                     database='####',
                     schema='####',
                     warehouse='####'))

with engine.connect() as conn:
    try:
        query="""SELECT RN,TITLE FROM OOTODOM_DATA_SHORT_FLATTEN  ORDER BY RN LIMIT 300"""

        df=pd.read_sql(query,conn)

        gc=gspread.service_account()

        loop_counter=0
        chunk_size=10000
        file_name='OOTODOM_ANALYSIS_'
        user_email=''

        for i in range(0,len(df),chunk_size):
            loop_counter+=1
            df_in=df.iloc[i:(i+chunk_size),:]

            spreadsheet_title=file_name +str(loop_counter)
            try:
                locals()['sh'+str(loop_counter)]=gc.open(spreadsheet_title)
            except:
                locals()['sh'+str(loop_counter)]=gc.create(spreadsheet_title)

            locals()['sh'+str(loop_counter)].share(user_email,perm_type='user',role='writer')
            wks=locals()['sh'+str(loop_counter)].get_worksheet(0)
            wks.resize(len(df_in)+1)
            set_with_dataframe(wks,df_in)

            column='C'
            start_row=2
            end_row=wks.row_count
            cell_range=f'{column}{start_row}:{column}{end_row}'
            curr_row=start_row
            cell_list=wks.range(cell_range)

            for cell in cell_list:
                cell.value=f'=GOOGLETRANSLATE(B{curr_row},"pl","en")
                curr_row+=1

            #Update the worksheet with 
            wks.update_cells(cell_list,value_input_option='USER_ENTERED')
            
            print(f'Spreadsheet {spreadsheet_title} created!')
                
            df_log=pd.DataFrame({'ID':[loop_counter],'SPREADSHEET_NAME':[spreadsheet_title]}) 
            df_log.to_sql('OOTODOM_DATA_LOG',con=engine,if_exists='append',index=False,chunksize=160000,method=pd_writer)  

   except Exception as e:
           print('---Error---',e)
   finally:
        conn.close()

engine.dispose()            
                 