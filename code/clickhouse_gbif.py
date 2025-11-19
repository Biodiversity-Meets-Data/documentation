import subprocess
import pandas as pd
import io

def clickhouse_query(query):
    """Run ClickHouse query and return pandas DataFrame"""
    cmd = ['./clickhouse', 'local', '-q', query, '--format', 'CSVWithNames']
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        df = pd.read_csv(io.StringIO(result.stdout))
        return df
    else:
        print(f"Error: {result.stderr}")
        return None

# Your query
query = """
SELECT countrycode, countDistinct(specieskey) AS distinct_species_count 
FROM s3('https://gbif-open-data-eu-central-1.s3.eu-central-1.amazonaws.com/occurrence/2025-11-01/occurrence.parquet/*', Parquet) 
GROUP BY countrycode 
ORDER BY distinct_species_count DESC 
LIMIT 10
"""

df = clickhouse_query(query)
print(df)
