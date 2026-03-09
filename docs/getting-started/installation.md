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

=== "REST API source"

    ```bash
    pip install fastelt[rest_api]
    ```

    Enables `RESTAPISource` for declarative REST API extraction via dlt's `rest_api` source.

=== "Filesystem sources"

    ```bash
    pip install fastelt[filesystem]
    ```

    Enables `LocalFileSystemSource` and `GCSFileSystemSource` for loading files from disk or cloud storage.

=== "All extras"

    ```bash
    pip install fastelt[cli,rest_api,filesystem]
    ```

## Requirements

- Python >= 3.12
- [dlt](https://dlthub.com/) (installed automatically)
- [Pydantic](https://docs.pydantic.dev/) >= 2.0 (installed automatically)
- [Loguru](https://loguru.readthedocs.io/) >= 0.7 (installed automatically)
