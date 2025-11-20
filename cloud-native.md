
# Cloud-Native GBIF Data Access with DuckDB & ClickHouse

This document shows how to query GBIF data directly from cloud-native Parquet files stored in S3, using Python, DuckDB, and ClickHouse.
It also covers local caching strategies, performance benchmarks, and the reason to use Parquet/S3 instead of CSV downloads and API-based workflows.

Reference: https://data-blog.gbif.org/post/aws-and-gbif/.
See python examples in the code folder. 

---

## Why Cloud-Native Formats?

Traditional approaches (CSV files or API requests) often limits how modern data analytics can be done. 
CSV files can be slow and you must download entire dataset locally. 
APIs often impose rate limit, has limited query options. 


### Cloud-native Parquet + S3

* Columnar (reads only required columns)
* Techniques to skip / predicate pushdown (see https://pola.rs/posts/predicate-pushdown-query-optimizer) 
* Highly compressed
* Distributed across thousands of partition files
* Query directly on S3: no download needed
* Works with high-performance engines (DuckDB, ClickHouse, Arrow, Spark, etc.)


---

## DuckDB Workflow (Python)

R users: identical workflows exist via `arrow`, `duckdb`, and `DBI` connectors.


DuckDB can query Parquet files stored in S3 directly:

```python
import duckdb

con = duckdb.connect()

q = """
SELECT
    countrycode,
    count(DISTINCT specieskey) AS distinct_species_count
FROM read_parquet(
    's3://gbif-open-data-eu-central-1/occurrence/2025-11-01/occurrence.parquet/*'
)
GROUP BY countrycode
ORDER BY distinct_species_count DESC
LIMIT 10;
"""

print(con.execute(q).df())
```

### DuckDB performance (benchmark)

 **~11 minutes** for a full GBIF snapshot scan

---

## ⚡ ClickHouse Workflow

Install locally (macOS):

```bash
curl https://clickhouse.com/ | sh
sudo ./clickhouse install
```

Query GBIF directly from S3:

```bash
./clickhouse local -q "
SELECT
  countrycode,
  countDistinct(specieskey) AS distinct_species_count
FROM s3(
  'https://gbif-open-data-eu-central-1.s3.eu-central-1.amazonaws.com/occurrence/2025-11-01/occurrence.parquet/*',
  Parquet
)
GROUP BY countrycode
ORDER BY distinct_species_count DESC
LIMIT 10;"
```

### ClickHouse performance (benchmark)

 **~6 minutes**


---
Clickhouse can be installed a server as well. 


##  Local Caching Strategy (Recommended)

GBIF snapshots are large (hundreds of GB), so caching locally produces huge speed gains.

### For exploration / development

- Use **DuckDB**
- Cache a few Parquet partition files locally
- Query locally (milliseconds instead of minutes)


### For production / full scans

- **ClickHouse**
- Parallel processing + optimized IO
  
### Best workflow

1. Download once (specific partitions or entire snapshot)
2. Cache locally (DuckDB or simple shell script)
3. Run complex analytics with ClickHouse or DuckDB
4. Refresh cache only when GBIF releases new monthly datasets

---

## Speed-up Techniques 

* Sampling (`sample_limit=1_000_000`) – avoid scanning billions of rows
* Partition pruning – scan only one Parquet partition for quick checks
* Local caching – download once, reuse forever
* Parallelism – DuckDB uses 4 threads, ClickHouse uses many

## Other examples: OBIS 

https://github.com/iobis/speciesgrids 

```
aws s3 cp --recursive s3://obis-products/speciesgrids/h3_7 . --no-sign-request
```

Then using python libraries: 

```
import geopandas
import lonboard
import seaborn as sns

filters = [("genus", "==", "Gadus")]
gdf = geopandas.read_parquet("../h3_7/", filters=filters)[["cell", "records", "geometry", "species"]]

def generate_colors(unique_species):
    palette = sns.color_palette("Paired", len(unique_species))
    rgb_colors = [[int(r*255), int(g*255), int(b*255)] for r, g, b in palette]
    color_map = dict(zip(unique_species, rgb_colors))
    colors = lonboard.colormap.apply_categorical_cmap(gdf["species"], color_map)
    return colors

point_layer = lonboard.ScatterplotLayer.from_geopandas(gdf)
point_layer.get_radius = 10000
point_layer.radius_max_pixels = 2
point_layer.get_fill_color = generate_colors(gdf["species"].unique())
lonboard.Map([point_layer])
```

