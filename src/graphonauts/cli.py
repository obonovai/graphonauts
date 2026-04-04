"""Command-line interface for the Graphonauts benchmarking framework.

Thin entry point that registers subcommands from the commands package.
Each command is implemented in its own module under graphonauts.commands.

Usage examples:
    python -m graphonauts load neo4j
    python -m graphonauts verify neo4j
    python -m graphonauts benchmark neo4j --memory --category join --query 2
    python -m graphonauts benchmark neo4j --time --runs 10
    python -m graphonauts benchmark neo4j --all
    python -m graphonauts compare neo4j memgraph --metric mean
    python -m graphonauts save neo4j
    python -m graphonauts summarize neo4j
"""

import click

from graphonauts.commands.benchmark import benchmark
from graphonauts.commands.compare import compare
from graphonauts.commands.load import load
from graphonauts.commands.save import save
from graphonauts.commands.summarize import summarize
from graphonauts.commands.verify import verify


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def main() -> None:
    """Graphonauts: Graph database benchmarking framework using TPC-H queries."""


main.add_command(load)
main.add_command(verify)
main.add_command(benchmark)
main.add_command(compare)
main.add_command(save)
main.add_command(summarize)


if __name__ == "__main__":
    main()
