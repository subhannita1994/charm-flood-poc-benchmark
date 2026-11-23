"""CLI entrypoint for the flood scoring pipeline package."""

from __future__ import annotations

import typer

from .commands.run_cgic_home_flood_pipeline import (
    app as run_cgic_home_flood_pipeline,
)

app = typer.Typer(
    help="Utilities for running and validating the flood scoring pipeline package.",
    no_args_is_help=True,
)
app.add_typer(run_cgic_home_flood_pipeline, name="run-cgic-home-flood-pipeline")


def main() -> None:
    """Run the package CLI."""
    try:
        app()
    except SystemExit as ex:
        if ex.code == 0:
            return
        raise


if __name__ == "__main__":
    main()
