"""
Build a DuckDB database with spatial index from the NL buildings parquet file.
"""
import duckdb
import time
import os

PARQUET_FILE = "nl_buildings.parquet"
DB_FILE = "nl_buildings.duckdb"


def main():
    print("=" * 60)
    print("Building indexed DuckDB database")
    print("=" * 60)

    if not os.path.exists(PARQUET_FILE):
        print(f"Error: {PARQUET_FILE} not found. Run download_nl_buildings.py first.")
        return

    # Remove existing database
    if os.path.exists(DB_FILE):
        print(f"Removing existing {DB_FILE}...")
        os.remove(DB_FILE)

    # Connect to new database
    print(f"\nCreating {DB_FILE}...")
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    # Load data from parquet
    print("\nLoading data from parquet file...")
    load_start = time.perf_counter()

    con.execute(f"""
        CREATE TABLE buildings AS
        SELECT
            id,
            geometry,
            bbox,
            name,
            height,
            class,
            subtype,
            num_floors
        FROM '{PARQUET_FILE}'
    """)

    load_time = time.perf_counter() - load_start
    print(f"Data loaded in {load_time:.1f}s")

    # Count rows
    count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    print(f"Total buildings: {count:,}")

    # Create spatial index on geometry
    print("\nCreating spatial index on geometry column...")
    print("(This may take a few minutes...)")
    index_start = time.perf_counter()

    con.execute("CREATE INDEX buildings_geo_idx ON buildings USING RTREE (geometry)")

    index_time = time.perf_counter() - index_start
    print(f"Spatial index created in {index_time:.1f}s")

    # Note: bbox index not needed since we use the spatial R-tree index

    # Close and check file size
    con.close()

    size_mb = os.path.getsize(DB_FILE) / (1024 * 1024)

    print()
    print("=" * 60)
    print("Database build complete!")
    print("=" * 60)
    print(f"Buildings: {count:,}")
    print(f"Database size: {size_mb:.1f} MB")
    print(f"Indexes: geometry (R-tree), bbox")
    print(f"Output: {DB_FILE}")


if __name__ == "__main__":
    main()
