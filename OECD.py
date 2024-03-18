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
    "CHL": "Chile",
    "COL": "Colombia",
    "CRI": "Costa Rica",
    "CZE": "Czechia",
    "DNK": "Denmark",
    "EST": "Estonia",
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
    "POL": "Poland",
    "PRT": "Portugal",
    "SVK": "Slovak Republic",
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
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_INDIC,1.0/.UNE_LF_M...Y._T.Y_GE15..M?startPeriod=2018-01'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME_PERIOD', columns='REF_AREA', values='OBS_VALUE')
df_new = df_new.round(decimals=1)
df_new = df_new.rename(columns=rename_columns)
df_new.to_csv('data/OECD_MEI_Unemployment_Last_5Y.csv', index=True)

