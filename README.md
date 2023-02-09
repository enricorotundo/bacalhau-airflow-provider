# Bacalhau Airflow Provider

> :construction: This repo is meant for demoing purposes - don't use in production.

Inspired by the [dbt-Airflow](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/_api/airflow/providers/dbt/cloud/index.html) provider, this is an exploratory effort for adding pipelines to [bacalhau](https://github.com/filecoin-project/bacalhau).
Find the related design doc [here](https://hackmd.io/@usN-geg4Q_iFcXZ-UCZpoQ/rkW5FE3Mj).

Tested on Bacalhau version >= v0.3.6.

## Pre-requistes

Install Airflow:

```bash
conda create --name bacalhau python=3.9

conda activate bacalhau

AIRFLOW_VERSION=2.4.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

export AIRFLOW_HOME=~/airflow
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

## OpenLineage integration

For data lineage, the Bacalhau operator integrates the methods `get_openlineage_facets_on_start` and
`get_openlineage_facets_on_complete`. It allows it to be compliant with `openlineage-airflow` standards to collect metadata

### Configuration

Along an Airflow instance, a client needs an OpenLineage compatible backend. In our case we will be focusing on [Marquez](https://github.com/MarquezProject/marquez),
an open source solution for he collection, aggregation, and visualization of a data ecosystem's metadata.

To setup a Marquez backend, it is advised to follow the [Quickstart](https://github.com/MarquezProject/marquez#quickstart) 
from their README.

Once the backend runs, an environment variable needs to be set to our Airflow components, `OPENLINEAGE_URL`. This variable
will tell the `openlineage-airflow` library where to write our executions metadata. For a complete list of the available 
variables, see [OpenLineage doc](https://openlineage.io/docs/integrations/airflow/usage#environment-variables).


### How it works

The `openlineage-airflow` library integrated in will run the methods as so:
1. On TaskInstance start, collect metadata for each task
2. Collect task input / output metadata (source, schema, etc) with the `get_openlineage_facets_on_start` method
3. Collect task run-level metadata (execution time, state, parameters, etc) with `get_openlineage_facets_on_complete` method
4. On TaskInstance complete, also mark the task as complete in Marquez

### Extracted metadata

The information that we are extracted through this Operator are:
- ID: Unique global ID of this job in the bacalhau network.
- ClientID: ID of the client that created this job.
- Inputs: Data volumes read in the job
- Outputs: Data volumes we will write in the job

In the future it would be nice to also support:
- APIVersion: APIVersion of the Job
- CreatedAt: Time the job was submitted to the bacalhau network
- [Spec fields in the Job model](https://github.com/filecoin-project/bacalhau/blob/main/pkg/model/job.go#L211)

### Contributing

Before contributing, refer to [OpenLineage doc](https://openlineage.io/docs/integrations/airflow/operator) to understand
the data schemas that can be used in OpenLineage.

### Questions?

If you have any questions or feedback, please reach out to `enricorotundo` on the `#bacalhau` channel in [Filecoin Slack](https://filecoin.io/slack).
