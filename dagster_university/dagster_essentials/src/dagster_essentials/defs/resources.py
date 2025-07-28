import dagster as dg
from dagster_duckdb import DuckDBResource

# Define the resource
database_resource = DuckDBResource(
    database="data/staging/data.duckdb"
)

# Register the resource in Definitions
@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "database_resource": database_resource
        }
    )
