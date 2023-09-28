import json
import pickle
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas, pd_writer
import getpass as gt
import pandas as pd
from config import CONFIG
import os

CONN = None

def get_connection_args():
    args = {}
    if CONFIG['AUTHENTICATOR'] == 'snowflake':
        args = {
            'user': CONFIG['USER'],
            'password': CONFIG['PASSWORD'],
            'account': CONFIG['ACCOUNT'],
        }
    else:
        args = {
            'user': gt.getuser()
        }
    args.update({
        'account': CONFIG['ACCOUNT'],
        'database': CONFIG['DATABASE'],
        'schema': CONFIG['SCHEMA'],
        'authenticator': CONFIG['AUTHENTICATOR'], 
        'warehouse': CONFIG['WAREHOUSE'],
        'role': CONFIG['ROLE']
    })
    return args
    
def get_connection():
    global CONN
    if CONN is None:
        # CONN = snowflake.connector.connect(**get_connection_args())
        CONN = snowflake.connector.connect(
            user=CONFIG['USER'],
            password=CONFIG['PASSWORD'],
            account=CONFIG['ACCOUNT'],
            warehouse=CONFIG['WAREHOUSE'],
            database=CONFIG['DATABASE'],
            schema=CONFIG['SCHEMA']
        )
    return CONN

def execute_sql(sql, bindings=None):
    return read_sql(sql, bindings)

def read_sql(sql, bindings=None):
    conn = get_connection()
    cur = conn.cursor()
    results = cur.execute(sql, bindings)
    return pd.DataFrame(results, columns=[c[0].lower() for c in cur.description])

def write_sql(df, name, if_exists='append'):
    from sqlalchemy import create_engine
    args = get_connection_args()
    user = args.pop('user')
    account = args.pop('account')
    database = args.pop('database')
    schema = args.pop('schema')
    db_url = f"snowflake://{user}@{account}/{database}/{schema}"
    engine = create_engine(db_url, connect_args=args)
    df.columns = [str(x).upper() for x in df.columns]
    print(df.columns)
    df.to_sql(name, con=engine, if_exists=if_exists, index=False, method=pd_writer)
    return "success"
    

def close_connection():
    global CONN
    if CONN is not None:
        CONN.close()

# def main():
#     conn = get_connection()
#     sql = '''SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();''' # check to make sure set up is okay!
#     res = read_sql(sql)
#     print(res)
#     close_connection()

# if __name__ == '__main__':
#     main()