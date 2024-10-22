## Airflow based on Python3.9.6 venv

1. Create a virtual environment in the current path.
```bash
python3 -m venv my-venv
```

2. Activate the virtual environment.
```bash
source my-venv/bin/activate

export AIRFLOW_HOME=$(pwd)/airflow
```

3. Setup the airflow environment variables and install it
```shell
AIRFLOW_VERSION=2.10.2

PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

4. Install the required packages.

```bash
pip3 install pyspark

pip3 install pandas
```
5. Start the Airflow web server.

```bash
airflow standalone
```

## Tips

[Data source](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)

Exit python venv with the `deactivate` command

## TroubleShooting

SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC

```shell
pip3 install urllib3 --upgrade
```

java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver

```
add mysql jar in my-venv/lib/python3.9/site-packages/pyspark/jars directory
```
