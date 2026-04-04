"""Neo4j database module configuration.

Container: neo4j:5.26.6-community with APOC plugin
Connection: Bolt protocol on localhost:7687
"""

from graphonauts.base.config import DatabaseConfig

config = DatabaseConfig(
    name="neo4j",
    container_name="neo4j-graphonaut",
    health_check_indicators=[
        "Started.",
        "Remote interface available at",
        "Bolt enabled on",
        "Started HTTP",
    ],
    health_check_timeout=150.0,
)
