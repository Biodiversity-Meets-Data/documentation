import duckdb

def read_gbif_from_s3(snapshot_date="2025-10-01", limit=10):
    """
    Read GBIF occurrence data from S3 bucket using DuckDB.
    
    Args:
        snapshot_date: Date of the GBIF snapshot (format: YYYY-MM-DD)
                      Use a valid past date like "2023-06-01" or "2024-01-01"
        limit: Number of rows to retrieve (use None for all rows)
    
    Returns:
        pandas DataFrame with the query results
    """
    # Create DuckDB connection
    conn = duckdb.connect()
    
    try:
        # Install and load the httpfs extension for S3 access
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure for anonymous S3 access
        conn.execute("SET s3_region='us-east-1';")
        
        # Note: Fixed typo - it's "occurrence" not "occurence"
        s3_path = f's3://gbif-open-data-us-east-1/occurrence/{snapshot_date}/occurrence.parquet/*'
        
        # Build query
        query = f"SELECT * FROM read_parquet('{s3_path}')"
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query and return as pandas DataFrame
        result = conn.execute(query).df()
        
        print(f"Successfully read {len(result)} rows from GBIF")
        return result
        
    except Exception as e:
        print(f"Error reading from S3: {e}")
        print("\nTroubleshooting tips:")
        print("1. Ensure the snapshot date exists (e.g., '2023-06-01', '2024-01-01')")
        print("2. Check your internet connection")
        print("3. Verify the S3 path is correct")
        return None
        
    finally:
        conn.close()


def get_top_countries_by_species(snapshot_date="2025-10-01", region="eu-central-1", limit=10):
    """
    Get top countries by distinct species count.
    
    Args:
        snapshot_date: Date of the GBIF snapshot (format: YYYY-MM-DD)
        region: AWS region ('us-east-1' or 'eu-central-1')
        limit: Number of top countries to return
    
    Returns:
        pandas DataFrame with countries and their distinct species counts
    """
    conn = duckdb.connect()
    
    try:
        # Install and load the httpfs extension for S3 access
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3 region
        conn.execute(f"SET s3_region='{region}';")
        
        # Build S3 path
        s3_path = f's3://gbif-open-data-{region}/occurrence/{snapshot_date}/occurrence.parquet/*'
        
        # Query: Top countries by distinct species count
        query = f"""
        SELECT 
            countrycode, 
            COUNT(DISTINCT specieskey) AS distinct_species_count 
        FROM read_parquet('{s3_path}')
        WHERE countrycode IS NOT NULL
        GROUP BY countrycode 
        ORDER BY distinct_species_count DESC 
        LIMIT {limit}
        """
        
        print(f"Querying GBIF data from {region}...")
        result = conn.execute(query).df()
        
        print(f"\nTop {limit} countries by distinct species count:")
        return result
        
    except Exception as e:
        print(f"Error querying S3: {e}")
        print("\nTroubleshooting tips:")
        print(f"1. Verify snapshot date '{snapshot_date}' exists")
        print(f"2. Check region '{region}' is correct (us-east-1 or eu-central-1)")
        print("3. Ensure internet connection is stable")
        return None
        
    finally:
        conn.close()


def query_gbif_with_filter(snapshot_date="2025-10-01", region="eu-central-1", country_code=None, species=None, limit=100):
    """
    Query GBIF data with optional filters.
    
    Args:
        snapshot_date: Date of the GBIF snapshot
        region: AWS region ('us-east-1' or 'eu-central-1')
        country_code: ISO country code to filter by (e.g., 'US', 'GB')
        species: Species name to filter by
        limit: Number of rows to retrieve
    
    Returns:
        pandas DataFrame with filtered results
    """
    conn = duckdb.connect()
    
    try:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute(f"SET s3_region='{region}';")
        
        s3_path = f's3://gbif-open-data-{region}/occurrence/{snapshot_date}/occurrence.parquet/*'
        
        # Build query with filters
        query = f"SELECT * FROM read_parquet('{s3_path}') WHERE 1=1"
        
        if country_code:
            query += f" AND countrycode = '{country_code}'"
        
        if species:
            query += f" AND species LIKE '%{species}%'"
        
        if limit:
            query += f" LIMIT {limit}"
        
        result = conn.execute(query).df()
        print(f"Query returned {len(result)} rows")
        return result
        
    except Exception as e:
        print(f"Error: {e}")
        return None
        
    finally:
        conn.close()


# Example usage
if __name__ == "__main__":
    print("Querying GBIF occurrence data from S3...\n")
    
    # Example 1: Top countries by distinct species count (your query!)
    df_top_countries = get_top_countries_by_species(
        snapshot_date="2025-10-01",
        region="eu-central-1",
        limit=10
    )
    if df_top_countries is not None:
        print("\n" + "="*50)
        print(df_top_countries.to_string(index=False))
        print("="*50)
    
    # Example 2: Read first 5 rows (uncomment to use)
    # df = read_gbif_from_s3(snapshot_date="2023-06-01", limit=5)
    # if df is not None:
    #     print("\nFirst few rows:")
    #     print(df.head())
    #     print(f"\nColumns: {list(df.columns)}")
    
    # Example 3: Query with filters (uncomment to use)
    # df_filtered = query_gbif_with_filter(
    #     snapshot_date="2025-10-01",
    #     region="eu-central-1",
    #     country_code="US",
    #     limit=10
    # )
    # if df_filtered is not None:
    #     print("\nFiltered results:")
    #     print(df_filtered)
