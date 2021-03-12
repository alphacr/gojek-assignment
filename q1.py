import pandas as pd
from pathlib import Path
import json

data_source_path = r'L:\Github\gojek-assignment\q1_data_source.json'

df = pd.read_json(data_source_path, lines=True)
print(df)

df.to_csv('q1_result.csv', index=False)

