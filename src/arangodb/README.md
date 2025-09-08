# ArangoDB Experiment

This guide explains how to use the ArangoDB experiment in this project.

## Steps to Run the Experiment

1. **Start the ArangoDB Database**
   - Navigate to the `arangodb` directory:
     ```bash
     cd src/arangodb
     ```
   - Run the following command to start the ArangoDB database using Docker Compose:
     ```bash
     docker compose up -d
     ```

2. **Load the TPC-H Data**
   - Once the database is running, execute the `load.py` script to load the TPC-H data into ArangoDB:
     ```bash
     python load.py
     ```

3. **Run Testing Queries**
   - After the data is loaded, execute the `queries.py` script to run the testing queries on the ArangoDB database:
     ```bash
     python queries.py
     ```

4. **Stop the ArangoDB Database**
   - Once you have finished running the experiment, stop the ArangoDB database by running:
     ```bash
     docker compose down
     ```

## Notes
- Ensure that Docker is installed and running on your system before starting the experiment.
- Python dependencies should be installed as specified in the `pyproject.toml` file.
- For more information, refer to the [official Python ArangoDB Async documentation](https://python-arango-async.readthedocs.io/en/latest/).
