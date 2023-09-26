import pandas as pd
import os
import time
from datetime import date
import numpy as np
import json

def discard_info(pt, med, diag, proc, rx, inp, lab):
    """
    From the embeddings paper:
        The data of each individual is in a structured format which contains information including diagnosis codes (ICD-9), 
        medical visits, lab test results (LOINC), and drug usage (NDC)

        Note: not sure what the "medical visits" info included -- paper is ambiguous
    """

    pt = pt[['Patid','Gdr_Cd','Yrdob']].drop_duplicates().reset_index(drop=True)
    med = med[['Patid','Clmid','Clmseq','Conf_Id','Fst_Dt','Lst_Dt','Proc_Cd','Ndc']].drop_duplicates().reset_index(drop=True)
    diag = diag[['Patid','Clmid','Fst_Dt','Diag_Position','Diag']].drop_duplicates().reset_index(drop=True)
    proc = proc[['Patid','Clmid','Fst_Dt','Proc_Position','Proc']].drop_duplicates().reset_index(drop=True)
    rx = rx[['Patid','Clmid','Ndc']].drop_duplicates().reset_index(drop=True)
    inp = inp[['Patid','Conf_Id','Admit_Date','Disch_Date','Diag1','Diag2','Diag3','Diag4','Diag5','Proc1','Proc2','Proc3','Proc4','Proc5']].drop_duplicates().reset_index(drop=True)
    lab = lab[['Patid','Fst_Dt','Loinc_Cd']].drop_duplicates().reset_index(drop=True)

    return pt, med, diag, proc, rx, inp, lab

def inp_processing(inp):
    """
    Inpatient processing: Create separate dataframes for diagnoses and procedures
    """
    inp1 = pd.wide_to_long(
        inp[[c for c in inp.columns if not c.startswith('Proc') and c not in ['Drg']]],
        stubnames='Diag',
        i=['Patid','Conf_Id','Admit_Date','Disch_Date'],
        j='Num'
    ).reset_index(drop=False)

    inp2 = pd.wide_to_long(
        inp[[c for c in inp.columns if not c.startswith('Diag') and c not in ['Drg']]],
        stubnames='Proc',
        i=['Patid','Conf_Id','Admit_Date','Disch_Date'],
        j='Num'
    ).reset_index(drop=False)

    inp1 = inp1.rename(columns={'Diag':'Name','Admit_Date':'Fst_Dt','Disch_Date':'Lst_Dt'})
    inp2 = inp2.rename(columns={'Diag':'Name','Admit_Date':'Fst_Dt','Disch_Date':'Lst_Dt'})

    inp1['Type'] = 'Diagnosis (ICD-CM)'
    inp2['Type'] = 'Procedure (ICD-PCS)'

    return inp1, inp2

def single_pt_wl(p, med, diag, proc, rx, inp, lab):
    """
    Wide to long
    """
    p = float(p)
    med1 = med.loc[med.Patid == p, [c for c in med.columns if c not in ['Ndc']]]
    med2 = med.loc[med.Patid == p, [c for c in med.columns if c not in ['Proc_Cd']]]

    med1 = med1.rename(columns={'Proc_Cd':'Name','Clmseq':'Num'})
    med2 = med2.rename(columns={'Ndc':'Name','Clmseq':'Num'})

    med1['Type'] = 'Procedure (CPT)'
    med2['Type'] = 'Drug (NDC)'

    diag = diag.loc[diag.Patid == p,:].rename(columns={'Diag':'Name','Diag_Position':'Num'})
    diag['Type'] = 'Diagnosis (ICD-CM)'

    proc = proc.loc[proc.Patid == p,:].rename(columns={'Proc':'Name','Proc_Position':'Num'})
    proc['Type'] = 'Procedure (CPT)'

    rx = rx.loc[rx.Patid == p,:].rename(columns={'Ndc':'Name'})
    rx['Type'] = 'Drug (NDC)'

    inp1, inp2 = inp_processing(inp.loc[lab.Patid == p,:])

    lab = lab.loc[lab.Patid == p,:].rename(columns={'Loinc_Cd':'Name'})
    lab['Type'] = 'Lab Results (LOINC)'

    df = pd.concat([med1, med2, diag, proc, rx, inp1, inp2, lab], ignore_index=True)

    return df.dropna(subset=['Name','Fst_Dt']).reset_index(drop=True)

def get_partitions(df, time_groups):
    """
    This is not efficient but works for now.
    """
    dt_dict = {}
    for d in df.Fst_Dt.unique():
        for i in range(len(time_groups)-1):
            if (d >= time_groups[i]) & (d < time_groups[i+1]):
                dt_dict[d] = i+1
    return dt_dict

def single_pt_preprocess(df, t=4):
    """
    Inputs:
        df: long version of all patient data
        t: time interval (in months) usef for partitioning the data (e.g., 4 months for 1/3 of a year)

    Per the embeddings paper, each patient's data is preprocessed as follows:

    1. Arrange patient data longitudinally, regardless of type of code (ICD, CPT, LOINC, or DNC). Example:
        Data point 1 = screening procedure for the flu (CPT 87804)
        Data point 2 = prescription of acetaminophen (NDC 57344-0001)
        Data point 3 = lab result of elevated white count (LOINC 26464-8)
        Data point 4 = diagnosis of acute leukemia (ICD-9 208.0)
        Data point 5 = chemotherapy administration (CPT 96416)

    2. Use the time interval to partition the data into intervals of size T, resulting in a set of partitions e_i

    3. Remove duplicate mentions of a concept within each partition

    4. Randomly shuggle the concepts within each partition, resulting in a (random) sequence of concepts for each partition

    5. Treat each PARTITION as a single sentence to be given to word2vec
    """

    # check to ensure inputs exist
    if len(df) == 0:
        return []
    
    # arrange longitudinally
    cols = ['Patid','Fst_Dt','Clmid','Conf_Id','Num','Lst_Dt','Name','Type']
    df = df.dropna(subset=['Fst_Dt']).sort_values(cols).reset_index(drop=True)

    # partition into time groups
    df['Fst_Dt'] = pd.to_datetime(df['Fst_Dt'])

    time_groups = pd.date_range(
        date(df.Fst_Dt.min().year,1,1),
        date(df.Fst_Dt.max().year+1,1,1),
        freq=f'{t}MS'
    )

    d = get_partitions(df, time_groups)
    df['Partition'] = df['Fst_Dt'].map(d)

    # remove duplicates within each partition
    df = df.drop_duplicates(subset=['Partition','Name']).reset_index(drop=True)

    # treat each partition as a sentence (one list for every partition)
    sent = df.groupby('Partition')['Shuffled'].apply(list).values

    return list(sent)

def all_pt(pats, med, diag, proc, rx, inp, lab):
    start = time.time()
    docs = {}
    for i,p in enumerate(pats):
        df = single_pt_wl(p, med, diag, proc, rx, inp, lab)
        sent = single_pt_preprocess(df)
        if len(sent) > 0:
            docs[float(p)] = sent
        finish = time.time()
    print(f"time: {int((finish-start))} seconds")
    return docs

