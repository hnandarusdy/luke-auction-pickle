import pandas as pd
from sqlalchemy import create_engine
import json 
import logging

logger = logging.Logger('catch_all')

def generate_file(filename="rad200.xlsx"):
    try:
        db = MySecondDB()
        query = """
            SELECT 
                author as Author, 
                senderName as 'Sender Name',
                body as Message,
                DATE_FORMAT(from_unixtime(time), '%%m/%%d/%%Y %%H:%%i') as Time,
                CASE 
                    WHEN body like '%%create%%' THEN 'Create'
                    WHEN body like '%%remove%%' THEN 'Remove'
                END as 'Create or Remove',
                rego_number as 'PPSR Reg No',
                vin as Vin,
                status as Status
            FROM defaultdb.whatsapp_log
        """
        df = db.read_sql(query)
    except Exception as e: 
        print ("Error")
        print (e)
        print ("Also, close the excel file when running this file, please!")
    
    df.to_excel (filename, "w", index=False)
    return df 

def load_config():
    try:
        with open ("db-config.json", "r") as f:
            data = json.loads(f.read())
    except:
        print ("Error reading file from db-config.json")
        x = input ("Press Enter To Continue")

    return data

CONFIG = load_config()

ssh_host = CONFIG['ssh_host']
ssh_username = CONFIG['ssh_username']
ssh_password = CONFIG['ssh_password']
database_username = CONFIG['database_username']
database_password = CONFIG['database_password']
database_name = CONFIG['database_name']
localhost = CONFIG['localhost']

DIGITAL_OCEAN_HOST = CONFIG['digital_ocean']['host']
DIGITAL_OCEAN_USER = CONFIG['digital_ocean']['user']
DIGITAL_OCEAN_PWD = CONFIG['digital_ocean']['pwd']
DIGITAL_OCEAN_DB = CONFIG['digital_ocean']['db']



class MySecondDB():
    def __init__ (self):
        self.engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                        .format(host=DIGITAL_OCEAN_HOST,
                                user=DIGITAL_OCEAN_USER,
                                pw=DIGITAL_OCEAN_PWD,
                                db=DIGITAL_OCEAN_DB))
    def read_sql (self, query):
        df = pd.read_sql(query, self.engine)
        return df

    def write_or_replace_sql(self, df, table_name, primary_key, how='append', index=False):
        ids = df[primary_key].to_list()
        id = ids[0]

        query = f"delete from {table_name} WHERE {primary_key}='{id}'"
        self.engine.execute (query)

        self.write_to_sql(df, table_name)

    def write_to_sql (self, df, table_name, how='append', index=False):
        try:
            df.to_sql(con=self.engine, name=table_name, if_exists=how, index=index) #Append to add
        except Exception as e:
            logger.error(e, exc_info=True)

    def execute_query(self, query):
        self.engine.execute(query)

    def execute_query_return_rows(self, query):
        x = self.engine.execute(query)
        
        #return number of affected rows
        return x.rowcount

    # def check_wassenger_id_exists(self, wassenger_id):
    def check_wassenger_id_exists(self, wassenger_id):
        query = f"""
            SELECT id FROM wassenger_logs
            WHERE id = '{wassenger_id}'
            LIMIT 1
        """
        df = self.read_sql(query)
        
        if df is not None and not df.empty:
            return True
        else:
            return False

    def close(self):
        self.engine.dispose()

    