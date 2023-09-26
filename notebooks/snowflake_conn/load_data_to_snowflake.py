from snowflake_helpers import write_sql
import pandas as pd
import glob
import os

files = glob.glob('../seeds/*.csv')
for f in files:
    base_name = os.path.basename(f)
    base_name = os.path.splitext(base_name)[0]
    df = pd.read_csv(f'../seeds/{base_name}.csv', low_memory=False)
    print(base_name, df.shape)

    write_sql(df, base_name, if_exists='replace')