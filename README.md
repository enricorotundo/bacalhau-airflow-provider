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

## Run pipeline

```bash
PREDICTABLE_API_PORT=1 LOG_LEVEL=debug bacalhau devstack
```

```bash
export AIRFLOW_HOME=~/airflow
airflow db clean --clean-before-timestamp '2022-12-31 00:00:00+01:00' --yes
airflow standalone
```

http://0.0.0.0:8080


```bash
export BACALHAU_IPFS_0=/ip4/0.0.0.0/tcp/54979/p2p/QmeTsxTdejRqqoLuTV3ffbbHEZRbywkyuiXVP6azudiJRx
ipfs --api ${BACALHAU_IPFS_0} add ./test-data/numbers-00.txt
ipfs --api ${BACALHAU_IPFS_0} add ./test-data/numbers-01.txt
ipfs --api ${BACALHAU_IPFS_0} add ./test-data/numbers-02.txt
ipfs --api ${BACALHAU_IPFS_0} add ./test-data/numbers-03.txt
...
airflow dags test bacalhau-helloworld
```

## TODO

- [x] Abort if jobs fails
- [ ] Easy UX
- [ ] Fan out/in example
