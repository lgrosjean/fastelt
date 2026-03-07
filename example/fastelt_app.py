from pydantic import BaseModel

from fastelt import FastELT
from fastelt.extractors.csv import csv_extractor
from fastelt.loaders.parquet import parquet_loader


class User(BaseModel):
    name: str
    email: str
    age: int
    city: str


app = FastELT()
app.include(csv_extractor(User))
app.include(parquet_loader(User))

if __name__ == "__main__":
    app.run(
        extractor="csv",
        loader="parquet",
        extractor_config={"path": "example/users.csv"},
        loader_config={"path": "example/users.parquet"},
    )
    print("Done! Written to example/users.parquet")
