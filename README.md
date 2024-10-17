## Airflow based on Python venv

1. Create a virtual environment.
```bash
python3 -m venv my-venv
```

2. Activate the virtual environment.
```bash
source my-venv/bin/activate
```

3. Install the required packages.
```bash
pip3 install pyspark
```
```bash
pip3 install apache-airflow
```
> ETL coding and then add the dags file(`airflow_dag_demo.py`) to the `dags` folder(~/airflow/dags)
> Ensure the dags file can be executed at the airflow path.

4. Start the Airflow web server.
```bash
airflow standalone
```

## Tips

[Data source](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)

