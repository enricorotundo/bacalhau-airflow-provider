# Bacalhau Airflow Provider

> :construction: This repo is meant for demoing purposes - don't use in production.

Inspired by the [dbt-Airflow](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/_api/airflow/providers/dbt/cloud/index.html) provider, this is an exploratory effort for adding pipelines to [bacalhau](https://github.com/filecoin-project/bacalhau).
Find the related design doc [here](https://hackmd.io/@usN-geg4Q_iFcXZ-UCZpoQ/rkW5FE3Mj).

Tested on Bacalhau v0.3.6.

## Pre-requistes

Install Airflow:

```bash
conda create --name bacalhau python=3.9

conda activate bacalhau

AIRFLOW_VERSION=2.4.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

e
airflow db init
```

To install this provider we need to place it within the `AIRFLOW_HOME/dags` folder:

```
git clone https://github.com/enricorotundo/bacalhau-airflow-provider.git
ln -s <PATH_TO_THIS_REPO>/bacalhau-airflow-provider/ ${AIRFLOW_HOME}/dags
```

## Launch Airflow UI

In a separate terminal:

```bash
export AIRFLOW_HOME=~/airflow
airflow standalone
```

## Run pipelines

```bash
airflow dags test bacalhau-image-processing
airflow dags test bacalhau-integer-sum
```

Head over to http://0.0.0.0:8080 to inspect the running dags.
