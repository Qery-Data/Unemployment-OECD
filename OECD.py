from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import io
import pandas as pd
import pycountry
os.makedirs('data', exist_ok=True)

#Unemployment MEI
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU28+EU27_2020+G4E+G-7+NAFTA+OECDE+G-20+OECD+OXE+SDR+ONM+A5M+NMEC+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF+BRIICS.LRHUTTTT.STSA.M/all?startTime=2016'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns={"Euro area (19 countries)": "Euro area", "European Union â 27 countries (from 01/02/2020)": "EU27", "TÃ¼rkiye": "Türkiye"})
df_new.to_csv('data/OECD_MEI_Unemployment_Last_5Y.csv', index=True)