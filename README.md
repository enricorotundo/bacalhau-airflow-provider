# Bacalhau Airflow Provider

> :construction: This repo is work-in-progres

Inspired by the [dbt](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/_api/airflow/providers/dbt/cloud/index.html) provider, this is preliminary work for adding pipelines to [bacalhau](https://github.com/filecoin-project/bacalhau).
Find the related design doc [here](https://hackmd.io/@usN-geg4Q_iFcXZ-UCZpoQ/rkW5FE3Mj).


## Pre-requistes

```bash
conda create --name bacalhau python=3.9

conda activate bacalhau

AIRFLOW_VERSION=2.4.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Then install Bacalhau cli.

```bash
export PREDICTABLE_API_PORT=1
bacalhau devstack
```
