# Neo4j Experiment

This guide explains how to use the Neo4j experiment in this project.

## Steps to Run the Experiment

1. **Start the Neo4j Database**
   - Navigate to the `neo4jdb` directory:
     ```bash
     cd src/neo4jdb
     ```
   - Run the following command to start the Neo4j database using Docker Compose:
     ```bash
     docker compose up -d
     ```

2. **Load the TPC-H Data**
   - Once the database is running, execute the `load.py` script to load the TPC-H data into Neo4j:
     ```bash
     python load.py
     ```

3. **Run Testing Queries**
   - After the data is loaded, execute the `queries.py` script to run the testing queries on the Neo4j database:
     ```bash
     python queries.py
     ```

4. **Stop the Neo4j Database**
   - Once you have finished running the experiment, stop the Neo4j database by running:
     ```bash
     docker compose down
     ```

## Notes
- Ensure that Docker is installed and running on your system before starting the experiment.
- Python dependencies should be installed as specified in the `pyproject.toml` file.
- For more information, refer to the [official Python Neo4j documentation](https://neo4j.com/docs/python-manual/current/).
