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

#OECD Unemployment Rate Latest data (flags and hex codes)

#Monthly data latest Value
oecd_url='https://stats.oecd.org/SDMX-JSON/data/STLABOUR/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EU27_2020+EA20+G-7+OECD.LRHUTTTT.STSA.M/all?startTime=2023'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(index={"Euro area (19 countries)": "Euro area", "European Union â 27 countries (from 01/02/2020)": "EU27", "TÃ¼rkiye": "Türkiye"})
df_new_most_recent_value_month = pd.DataFrame(columns=['Most Recent Value', 'Most Recent Period'], index=df_new.index)

# For each row (country) in df_new, identify the most recent year with a value
for country, row in df_new.iterrows():
    # Drop NaN values for the row and get the most recent year
    most_recent_period = row.dropna().last_valid_index()
    most_recent_value = row[most_recent_period] if most_recent_period else None

    # Assign the values to the new dataframe
    df_new_most_recent_value_month.loc[country] = [most_recent_value, most_recent_period]

#Quarterly data latest value (NZL+CHE)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/STLABOUR/NZL+CHE.LRHUTTTT.STSA.Q/all?startTime=2023'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
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
country_flag_codes = pd.DataFrame({
    'Country': ['Australia', 'Austria', 'Belgium', 'Canada', 'Chile', 'Colombia', 'Costa Rica', 'Czech Republic', 'Denmark', 'Estonia', 'Euro area', 'EU27', 'Finland', 'France', 'G7', 'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Israel', 'Italy', 'Japan', 'Korea', 'Latvia', 'Lithuania', 'Luxembourg', 'Mexico', 'Netherlands', 'New Zealand', 'Norway', 'OECD - Total', 'Poland', 'Portugal', 'Slovak Republic', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Türkiye', 'United Kingdom', 'United States'],
    'Flag_Code': [':au:', ':at:', ':be:', ':ca:', ':cl:', ':co:', ':cr:', ':cz:', ':dk:', ':ee:', '', '', ':fi:', ':fr:', '', ':de:', ':gr:', ':hu:', ':is:', ':ie:', ':il:', ':it:', ':jp:', ':kr:', ':lv:', ':lt:', ':lu:', ':mx:', ':nl:',':nz:',':no:', '', ':pl:', ':pt:', ':sk:', ':si:', ':es:', ':se:', ':ch:',':tr:', ':gb:', ':us:']
})
merged_df_with_flags = merged_df.reset_index().merge(country_flag_codes, on='Country').set_index('Country')
merged_df_with_flags = merged_df_with_flags.drop(["G7", "OECD - Total", "EU27"], axis=0)
merged_df_with_flags.sort_values(by='Most Recent Value', ascending=False, inplace=True)
merged_df_with_flags['Most Recent Value'] = pd.to_numeric(merged_df_with_flags['Most Recent Value'], errors='coerce')
merged_df_with_flags.dropna(subset=['Most Recent Value'], inplace=True)
colors = ['#F3F3FE', '#DADAFD', '#C2C2FC', '#9E9EFA', '#7979F9', '#5757F7']
merged_df_with_flags['color'] = pd.qcut(
    merged_df_with_flags['Most Recent Value'], 
    q=6, 
    labels=colors
)
merged_df_with_flags.to_csv('data/OECD_MEI_Unemployment_Lastest_Value.csv', index=True)

# OECD Unemployment Rate Last 12 months and change
oecd_url='https://stats.oecd.org/SDMX-JSON/data/STLABOUR/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EU27_2020+EA20+G-7+OECD.LRHUTTTT.STSA.M/all?startTime=2022-06'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
df_new = df_new.rename(index={"Euro area (20 countries)": "Euro area", "European Union â 27 countries (from 01/02/2020)": "EU27", "TÃ¼rkiye": "Türkiye"})
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
df_new = df_new.drop(["G7", "OECD - Total", "EU27","Euro area"], axis=0)
df_new = df_new.round(decimals=1)
country_flag_codes = pd.DataFrame({
    'Country': ['Australia', 'Austria', 'Belgium', 'Canada', 'Chile', 'Colombia', 'Costa Rica', 'Czech Republic', 'Denmark', 'Estonia', 'Euro area', 'EU27', 'Finland', 'France', 'G7', 'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Israel', 'Italy', 'Japan', 'Korea', 'Latvia', 'Lithuania', 'Luxembourg', 'Mexico', 'Netherlands', 'New Zealand', 'Norway', 'OECD - Total', 'Poland', 'Portugal', 'Slovak Republic', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Türkiye', 'United Kingdom', 'United States'],
    'Flag_Code': [':au:', ':at:', ':be:', ':ca:', ':cl:', ':co:', ':cr:', ':cz:', ':dk:', ':ee:', '', '', ':fi:', ':fr:', '', ':de:', ':gr:', ':hu:', ':is:', ':ie:', ':il:', ':it:', ':jp:', ':kr:', ':lv:', ':lt:', ':lu:', ':mx:', ':nl:',':nz:',':no:', '', ':pl:', ':pt:', ':sk:', ':si:', ':es:', ':se:', ':ch:',':tr:', ':gb:', ':us:']
})
df_new_with_flags = df_new.reset_index().merge(country_flag_codes, on='Country').set_index('Country')
df_new_with_flags.to_csv('data/OECD_MEI_Unemployment_Last_12M.csv', index=True)

#Unemployment last 5 years monthly data
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA20+EU27_2020+G4E+G-7+NAFTA+OECDE+G-20+OECD+OXE+SDR+ONM+A5M+NMEC+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF+BRIICS.LRHUTTTT.STSA.M/all?startTime=2018'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns={"Euro area (20 countries)": "Euro area", "European Union â 27 countries (from 01/02/2020)": "EU27", "TÃ¼rkiye": "Türkiye"})
df_new.to_csv('data/OECD_MEI_Unemployment_Last_5Y.csv', index=True)

#Unemployment last 5 years quarterly data
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA20+EU27_2020+G4E+G-7+NAFTA+OECDE+G-20+OECD+OXE+SDR+ONM+A5M+NMEC+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF+BRIICS.LRHUTTTT.STSA.Q/all?startTime=2018'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns={"Euro area (20 countries)": "Euro area", "European Union â 27 countries (from 01/02/2020)": "EU27", "TÃ¼rkiye": "Türkiye"})
df_new.to_csv('data/OECD_MEI_Unemployment_Last_5Y_Q.csv', index=True)
