"""Example: Pokémon REST API source — declarative extraction to local JSONL.

Uses RESTAPISource to pull from the PokéAPI. No auth needed.
Data is saved as JSONL files in the default `outputs/` directory.

Usage:  python example/pokemon_rest_api.py
"""

from fastelt import FastELT
from fastelt.sources.rest_api import RESTAPISource

pokemon = RESTAPISource(
    name="pokemon",
    base_url="https://pokeapi.co/api/v2/",
    resource_defaults={
        "endpoint": {"params": {"limit": 1000}},
    },
    resources=[
        {"name": "pokemon"},
    ],
)

app = FastELT(pipeline_name="pokemon_pipeline")
app.include_source(pokemon)

if __name__ == "__main__":
    info = app.run()
    print(f"Pipeline completed: {info}")
