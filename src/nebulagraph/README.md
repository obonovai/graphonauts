
# NebulaGraph Experiment

This guide explains how to use the NebulaGraph experiment in this project.

## Steps to Run the Experiment

1. **Start the NebulaGraph Database**
   - Follow instructions in https://docs.nebula-graph.io/3.0.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/3.deploy-nebula-graph-with-docker-compose/.

2. **Load the TPC-H Data**
   - Once the database is running, execute the `load.py` script to load the TPC-H data into NebulaGraph:
     ```bash
     python load.py
     ```

3. **Run Testing Queries**
   - After the data is loaded, execute the `queries.py` script to run the testing queries on the NebulaGraph database:
     ```bash
     python queries.py
     ```

4. **Stop the NebulaGraph Database**
   - Once you have finished running the experiment, stop the NebulaGraph database by running:
     ```bash
     docker compose down
     ```

## Notes
- Ensure that Docker is installed and running on your system before starting the experiment.
- Python dependencies should be installed as specified in the `pyproject.toml` file.
- For more information, refer to the [official Python NebulaGraph repository](https://github.com/vesoft-inc/nebula-python).
