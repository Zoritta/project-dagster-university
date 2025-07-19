import dagster as dg
import csv
import matplotlib.pyplot as plt
import geopandas as gpd
from datetime import datetime, timedelta
import duckdb
import os

from dagster_essentials.defs.assets import constants


@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)

@dg.asset(
    deps=["taxi_trips"]
)
def trips_by_week() -> None:
    # Define your date range (adjust as needed)
    start_date = datetime(2023, 3, 1)
    end_date = datetime(2023, 3, 31)

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))

    # Open CSV for writing
    with open(constants.TRIPS_BY_WEEK_FILE_PATH, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["period", "num_trips", "passenger_count", "total_amount", "trip_distance"])

        current = start_date
        while current <= end_date:
            week_end = current + timedelta(days=6)
            # DuckDB query for this week
            query = f"""
                SELECT
                    '{week_end.strftime('%Y-%m-%d')}' AS period,
                    COUNT(*) AS num_trips,
                    SUM(passenger_count) AS passenger_count,
                    SUM(total_amount) AS total_amount,
                    SUM(trip_distance) AS trip_distance
                FROM trips
                WHERE pickup_datetime >= '{current.strftime('%Y-%m-%d')}'
                  AND pickup_datetime < '{(week_end + timedelta(days=1)).strftime('%Y-%m-%d')}'
            """
            row = conn.execute(query).fetchone()
            writer.writerow(row)
            current = week_end + timedelta(days=1)