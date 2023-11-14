import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_csv_to_parquet(input_file_path, output_file_path, drop_option):
    df = pd.read_csv(input_file_path)
    
    # drop_option : 'row' or 'column'
    if drop_option == 'row':
        df = df.dropna()
    if drop_option == 'column':
        df = df.dropna(axis=1)
        
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file_path)
    
    table = pq.read_table(output_file_path)
    
    df = table.to_pandas()
    
    print(df.head())
    
    return df

input_file_path = ''
output_file_path = ''
# options : 'row' or 'column'
drop_option = ''

df = convert_csv_to_parquet(input_file_path, output_file_path, drop_option)