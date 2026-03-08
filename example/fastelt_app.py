"""Example: Simple fastELT app — CSV data into DuckDB via dlt.

Usage:  python example/fastelt_app.py
"""

import csv

from fastelt import FastELT, Source


# Source for local file data
local_data = Source(name="local")


@local_data.resource(primary_key="name", write_disposition="replace")
def users():
    """Extract users from a CSV file."""
    with open("example/users.csv") as f:
        for row in csv.DictReader(f):
            yield row


# Wire up
app = FastELT(pipeline_name="local_pipeline", destination="duckdb")
app.include_source(local_data)

if __name__ == "__main__":
    info = app.run()
    print(f"Done! {info}")
    print("Data loaded into local_pipeline.duckdb")
