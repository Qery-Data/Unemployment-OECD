from pyjstat import pyjstat
import requests
import os
import json
import numpy as np
from datetime import datetime
import locale
import io
import pandas as pd
os.makedirs('data', exist_ok=True)

#OECD Rename Dict

rename_columns = {
    "AUS": "Australia",
    "AUT": "Austria",
    "BEL": "Belgium",
    "CAN": "Canada",
    "CHE": "Switerland",
    "CHL": "Chile",
    "COL": "Colombia",
    "CRI": "Costa Rica",
    "CZE": "Czechia",
    "DNK": "Denmark",
    "EST": "Estonia",
    "EA20": "Euro area",
    "EU27_2020": "EU27",
    "FIN": "Finland",
    "FRA": "France",
    "DEU": "Germany",
    "GRC": "Greece",
    "HUN": "Hungary",
    "ISL": "Iceland",
    "IRL": "Ireland",
    "ISR": "Israel",
    "ITA": "Italy",
    "JPN": "Japan",
    "KOR": "Korea",
    "LVA": "Latvia",
    "LTU": "Lithuania",
    "LUX": "Luxembourg",
    "MEX": "Mexico",
    "NLD": "Netherlands",
    "NOR": "Norway",
    "NZL": "New Zealand",
    "POL": "Poland",
    "PRT": "Portugal",
    "SVK": "Slovakia",
    "SVN": "Slovenia",
    "ESP": "Spain",
    "SWE": "Sweden",
    "TUR": "Türkiye",
    "GBR": "United Kingdom",
    "USA": "United States",
}
rename_columns_to_flags = {
    "AUS": ":au:",
    "AUT": ":at:",
    "BEL": ":be:",
    "CAN": ":ca:",
    "CHE": ":ch:",
    "CHL": ":cl:",
    "COL": ":co:",
    "CRI": ":cr:",
    "CZE": ":cz:",
    "DNK": ":dk:",
    "EST": ":ee:",
    "FIN": ":fi:",
    "FRA": ":fr:",
    "DEU": ":de:",
    "GRC": ":gr:",
    "HUN": ":hu:",
    "ISL": ":is:",
    "IRL": ":ie:",
    "ISR": ":il:",
    "ITA": ":it:",
    "JPN": ":jp:",
    "KOR": ":kr:",
    "LVA": ":lv:",
    "LTU": ":lt:",
    "LUX": ":lu:",
    "MEX": ":mx:",
    "NLD": ":nl:",
    "NZL": ":nz:",
    "NOR": ":no:",
    "POL": ":pl:",
    "PRT": ":pt:",
    "SVK": ":sk:",
    "SVN": ":si:",
    "ESP": ":es:",
    "SWE": ":se:",
    "CHE": ":ch:", 
    "TUR": ":tr:",
    "GBR": ":gb:",
    "USA": ":us:",
}

#Monthly data latest Value
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC/.UNE_LF_M...Y._T.Y_GE15..M?startPeriod=2018-01'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME_PERIOD', columns='REF_AREA', values='OBS_VALUE')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns=rename_columns)
df_new.index.rename("TIME", inplace=True)
df_new = df_new.drop(["BGR"],axis=1)
df_new.to_csv('data/OECD_MEI_Unemployment_Last_5Y.csv', index=True)

#Quarterly data latest value
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC/OECD+CHE+NZL.UNE_LF_M...Y._T.Y_GE15..Q?startPeriod=2018-01'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME_PERIOD', columns='REF_AREA', values='OBS_VALUE')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns=rename_columns)
df_new.index.rename("TIME", inplace=True)
df_new.to_csv('data/OECD_MEI_Unemployment_Last_5Y_Q.csv', index=True)

#Monthly data latest Value
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC/.UNE_LF_M...Y._T.Y_GE15..M?startPeriod=2023-01'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns=rename_columns)
df_new_most_recent_value_month = pd.DataFrame(columns=['Most Recent Value', 'Most Recent Period'], index=df_new.index)

# For each row (country) in df_new, identify the most recent year with a value
for country, row in df_new.iterrows():
    # Drop NaN values for the row and get the most recent year
    most_recent_period = row.dropna().last_valid_index()
    most_recent_value = row[most_recent_period] if most_recent_period else None

    # Assign the values to the new dataframe
    df_new_most_recent_value_month.loc[country] = [most_recent_value, most_recent_period]

#Quarterly data latest value (NZL+CHE)
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC/CHE+NZL.UNE_LF_M...Y._T.Y_GE15..Q?startPeriod=2018-01'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.round(decimals=1)
df_new_most_recent_value_qtr = pd.DataFrame(columns=['Most Recent Value', 'Most Recent Period'], index=df_new.index)
# For each row (country) in df_new, identify the most recent year with a value
for country, row in df_new.iterrows():
    # Drop NaN values for the row and get the most recent year
    most_recent_period = row.dropna().last_valid_index()
    most_recent_value = row[most_recent_period] if most_recent_period else None

    # Assign the values to the new dataframe
    df_new_most_recent_value_qtr.loc[country] = [most_recent_value, most_recent_period]

#Merge the two
merged_df = pd.concat([df_new_most_recent_value_month, df_new_most_recent_value_qtr])
merged_df = merged_df.rename(index=rename_columns)
merged_df.index.rename("Country", inplace=True)
merged_df = df_new.drop(["BGR"],axis=0)
merged_df.sort_values(by='Most Recent Value', ascending=False, inplace=True)
merged_df['Most Recent Value'] = pd.to_numeric(merged_df['Most Recent Value'], errors='coerce')
merged_df.dropna(subset=['Most Recent Value'], inplace=True)
colors = ['#F3F3FE', '#DADAFD', '#C2C2FC', '#9E9EFA', '#7979F9', '#5757F7']
merged_df['color'] = pd.qcut(
    merged_df['Most Recent Value'], 
    q=6, 
    labels=colors
)
merged_df.to_csv('data/OECD_MEI_Unemployment_Lastest_Value.csv', index=True)

# OECD Unemployment Rate Last 12 months and change
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC/.UNE_LF_M...Y._T.Y_GE15..M?lastNObservations=13'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.sort_index(axis=1, ascending=True)
# Initialize new columns
df_new['Change previous month'] = None
df_new['Change last 12 months'] = None

# Loop through each row to calculate the change
for index, row in df_new.iterrows():
    # Drop NA values and get the last and second last date (if available)
    non_na = row.dropna()
    if len(non_na) > 1:
        last_date = non_na.index[-1]
        second_last_date = non_na.index[-2]
        change_previous_month = non_na[last_date] - non_na[second_last_date]
        df_new.at[index, 'Change previous month'] = change_previous_month

    # Calculate change compared to 12 months ago (if data is available)
    if len(non_na) > 12:
        twelfth_last_date = non_na.index[-13]  # 12 months ago
        change_last_12_months = non_na[last_date] - non_na[twelfth_last_date]
        df_new.at[index, 'Change last 12 months'] = change_last_12_months

# Convert new columns to appropriate data type
df_new['Change previous month'] = df_new['Change previous month'].astype('float')
df_new['Change last 12 months'] = df_new['Change last 12 months'].astype('float')
df_new = df_new.rename(index=rename_columns)
df_new.index.rename("Country", inplace=True)
df_new = df_new.drop(["G7", "OECD", "EU27","Euro area","BGR"], axis=0)
df_new = df_new.round(decimals=1)
df_new.to_csv('data/OECD_MEI_Unemployment_Last_12M.csv', index=True)

