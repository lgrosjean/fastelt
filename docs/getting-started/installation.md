# Installation

## Basic Install

```bash
pip install fastelt
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add fastelt
```

## Optional Extras

FastELT provides optional extras for additional functionality:

=== "CLI support"

    ```bash
    pip install fastelt[cli]
    ```

    Adds the `fastelt` command-line interface powered by [Typer](https://typer.tiangolo.com/).

=== "Parquet support"

    ```bash
    pip install fastelt[parquet]
    ```

    Adds Parquet read/write via [PyArrow](https://arrow.apache.org/docs/python/).

=== "All extras"

    ```bash
    pip install fastelt[cli,parquet]
    ```

## Requirements

- Python >= 3.12
- [Pydantic](https://docs.pydantic.dev/) >= 2.0 (installed automatically)
- [Loguru](https://loguru.readthedocs.io/) >= 0.7 (installed automatically)
