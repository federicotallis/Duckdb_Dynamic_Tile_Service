"""
Download Netherlands buildings from Overture Maps to local parquet file.
"""
import duckdb
import time
import os

# Netherlands bounding box (approximate)
NL_BBOX = {
    'min_lon': 3.35,
    'max_lon': 7.25,
    'min_lat': 50.75,
    'max_lat': 53.55
}

# Overture Maps S3 path
OVERTURE_PATH = "s3://overturemaps-us-west-2/release/2025-12-17.0/theme=buildings/type=building/*"

# Output file
OUTPUT_FILE = "nl_buildings.parquet"


def main():
    print("=" * 60)
    print("Downloading Netherlands buildings from Overture Maps")
    print("=" * 60)
    print(f"Bounding box: {NL_BBOX}")
    print(f"Output file: {OUTPUT_FILE}")
    print()

    # Check if file already exists
    if os.path.exists(OUTPUT_FILE):
        size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
        print(f"Warning: {OUTPUT_FILE} already exists ({size_mb:.1f} MB)")
        response = input("Overwrite? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return

    # Connect to DuckDB
    print("Initializing DuckDB...")
    con = duckdb.connect(":memory:")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")

    # First, count the buildings
    print("\nCounting buildings in Netherlands (this may take a few minutes)...")
    count_start = time.perf_counter()

    count_query = f"""
        SELECT COUNT(*) as count
        FROM read_parquet('{OVERTURE_PATH}', hive_partitioning=1)
        WHERE bbox.xmin <= {NL_BBOX['max_lon']}
          AND bbox.xmax >= {NL_BBOX['min_lon']}
          AND bbox.ymin <= {NL_BBOX['max_lat']}
          AND bbox.ymax >= {NL_BBOX['min_lat']}
    """
    result = con.execute(count_query).fetchone()
    count = result[0]
    count_time = time.perf_counter() - count_start

    print(f"Found {count:,} buildings in {count_time:.1f}s")

    # Download and save to parquet
    print(f"\nDownloading and saving to {OUTPUT_FILE}...")
    print("This may take 5-15 minutes depending on your connection...")
    download_start = time.perf_counter()

    download_query = f"""
        COPY (
            SELECT
                id,
                geometry,
                bbox,
                names.primary as name,
                height,
                class,
                subtype,
                num_floors,
                roof_shape,
                roof_color,
                facade_color
            FROM read_parquet('{OVERTURE_PATH}', hive_partitioning=1)
            WHERE bbox.xmin <= {NL_BBOX['max_lon']}
              AND bbox.xmax >= {NL_BBOX['min_lon']}
              AND bbox.ymin <= {NL_BBOX['max_lat']}
              AND bbox.ymax >= {NL_BBOX['min_lat']}
        ) TO '{OUTPUT_FILE}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    con.execute(download_query)
    download_time = time.perf_counter() - download_start

    # Check file size
    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)

    print()
    print("=" * 60)
    print("Download complete!")
    print("=" * 60)
    print(f"Buildings: {count:,}")
    print(f"File size: {size_mb:.1f} MB")
    print(f"Download time: {download_time:.1f}s")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
