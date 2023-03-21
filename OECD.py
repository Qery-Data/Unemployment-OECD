from pyjstat import pyjstat
import requests
import os
import json
import numpy as np
from datetime import datetime
import locale
import io
import pandas as pd
import pycountry
os.makedirs('data', exist_ok=True)

#Unemployment MEI 5Y Monthly Complete
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU28+EU27_2020+G4E+G-7+NAFTA+OECDE+G-20+OECD+OXE+SDR+ONM+A5M+NMEC+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF+BRIICS.LRHUTTTT.STSA.M/all?startTime=2016'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns={"Euro area (19 countries)": "Euro area", "European Union â 27 countries (from 01/02/2020)": "EU27", "TÃ¼rkiye": "Türkiye"})
df_new.to_csv('data/OECD_MEI_Unemployment_Last_5Y.csv', index=True)

#OECD Unemployment MEI 5Y Monthly (flags and hex codes)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU28+EU27_2020+G4E+G-7+NAFTA+OECDE+G-20+OECD+OXE+SDR+ONM+A5M+NMEC+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF+BRIICS.LRHUTTTT.STSA.M/all?startTime=2016'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(index={"Euro area (19 countries)": "Euro area", "European Union â 27 countries (from 01/02/2020)": "EU27", "TÃ¼rkiye": "Türkiye"})
country_flag_codes = pd.DataFrame({
    'Country': ['Australia', 'Austria', 'Belgium', 'Canada', 'Chile', 'Colombia', 'Costa Rica', 'Czech Republic', 'Denmark', 'Estonia', 'Euro area', 'EU27', 'Finland', 'France', 'G7', 'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Israel', 'Italy', 'Japan', 'Korea', 'Latvia', 'Lithuania', 'Luxembourg', 'Mexico', 'Netherlands', 'Norway', 'OECD - Total', 'Poland', 'Portugal', 'Slovak Republic', 'Slovenia', 'Spain', 'Sweden', 'Türkiye', 'United Kingdom', 'United States'],
    'Flag_Code': [':au:', ':at:', ':be:', ':ca:', ':cl:', ':co:', ':cr:', ':cz:', ':dk:', ':ee:', '', '', ':fi:', ':fr:', '', ':de:', ':gr:', ':hu:', ':is:', ':ie:', ':il:', ':it:', ':jp:', ':kr:', ':lv:', ':lt:', ':lu:', ':mx:', ':nl:', ':no:', '', ':pl:', ':pt:', ':sk:', ':si:', ':es:', ':se:', ':tr:', ':gb:', ':us:']
})
df_new_with_flags = df_new.reset_index().merge(country_flag_codes, on='Country').set_index('Country')
df_new_with_flags = df_new_with_flags.dropna(subset=['2023-01'])
rows_to_remove = ["G7", "OECD - Total", "Euro area", "EU27"]
df_new_with_flags = df_new_with_flags.drop(rows_to_remove, axis=0)
brackets = pd.qcut(df_new_with_flags['2023-01'], q=6, labels=False)
colors = ['#F3F3FE', '#DADAFD', '#C2C2FC', '#9E9EFA', '#7979F9', '#5757F7']
df_new_with_flags['color'] = brackets.map(lambda x: colors[int(x)] if not np.isnan(x) else np.nan)
df_new_with_flags.to_csv('data/OECD_MEI_Unemployment_Last_5Y_Monthly_Flag_Table.csv', index=True)