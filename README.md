# Bacalhau Airflow Provider

> :construction: This repo is meant for demoing purposes - don't use in production.

Inspired by the [dbt](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/_api/airflow/providers/dbt/cloud/index.html) provider, this is preliminary work for adding pipelines to [bacalhau](https://github.com/filecoin-project/bacalhau).
Find the related design doc [here](https://hackmd.io/@usN-geg4Q_iFcXZ-UCZpoQ/rkW5FE3Mj).

Tested on Bacalhau v0.3.6.

## Pre-requistes

```bash
conda create --name bacalhau python=3.9

conda activate bacalhau

AIRFLOW_VERSION=2.4.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

export AIRFLOW_HOME=~/airflow
ln -s /Users/enricorotundo/winderresearch/ProtocolLabs/bacalhau-airflow-provider/ ./dags
airflow db init
```

Then install:

* Bacalhau cli
* Docker Engine
* IPFS cli

## Run services

```bash
PREDICTABLE_API_PORT=1 LOG_LEVEL=debug bacalhau devstack
```

```bash
export AIRFLOW_HOME=~/airflow
airflow db clean --clean-before-timestamp '2022-12-31 00:00:00+01:00' --yes
airflow standalone
```

http://0.0.0.0:8080

## Run pipeline

```bash
airflow dags test --conf '{"ipfs_address":"${BACALHAU_IPFS_0}"}' bacalhau-integer-sum
```
