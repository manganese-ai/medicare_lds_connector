from snowflake_helpers import write_sql
import pandas as pd
import glob
import os

lds = pd.read_csv('../../seeds/lds_format.csv') # in schmidt-futures repo
files = [f for f in glob.glob('../../seeds/*.csv') if 'lds_format' not in f]
fd = {
    'carrier_base_claim':'Carrier',
    'carrier_claim_line':'Carrier',
    'dme_base_claim':'DME',
    'dme_claim_line':'DME',
    'hha_base_claim':'Home Health',
    'hha_revenue_center':'Home Health',
    'hospice_base_claim':'Hospice',
    'hospice_revenue_center':'Hospice',
    'inpatient_base_claim':'Inpatient',
    'inpatient_revenue_center':'Inpatient',
    'master_beneficiary_summary':'Beneficiary',
    'outpatient_base_claim':'Outpatient',
    'outpatient_revenue_center':'Outpatient',
    'snf_base_claim':'SNF',
    'snf_revenue_center':'SNF',
}

def change_dtypes(data_type, df, lds):
    subset = lds.loc[(lds.Data == data_type),:]

    d = dict(zip(subset.Name, subset.Type))

    for c in df.columns.tolist():
        orig_type = df[c].dtype
        lds_type = d[c]

        if lds_type == 'CHAR':
            df[c] = df[c].astype(str).replace({'nan':None})
        elif lds_type == 'DATE':
            df[c] = df[c].astype(str).replace({'nan':None}) #pd.to_datetime(df[c])
        else:
            df[c] = df[c].astype(float)

        new_type = df[c].dtype
    return df

for f in files:
    base_name = os.path.basename(f)
    base_name = os.path.splitext(base_name)[0]
    df = pd.read_csv(f'{f}', low_memory=False)
    df = change_dtypes(fd[base_name], df, lds)
    
    print(base_name, df.shape)
    write_sql(df, base_name, if_exists='replace')