# prefect-armada

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-armada/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-armada?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect/tree/main/src/integrations/prefect-armada" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect?color=0052FF&labelColor=090422" /></a>
</p>

`prefect-armada` runs Prefect flow runs on [Armada](https://armadaproject.io/), a
multi-cluster batch scheduler for Kubernetes.

## Getting started

### Prerequisites

- An Armada server you can reach over gRPC.
- An Armada queue that your credentials are permitted to submit to.

### Installation

Install `prefect-armada` with `pip`:

```bash
pip install prefect-armada
```

### Running flow runs as Armada jobs

Create an Armada work pool and start a worker:

```bash
prefect work-pool create --type armada my-armada-pool
prefect worker start --pool my-armada-pool
```

By default the worker connects to `localhost:50051` and submits to the `prefect`
queue. Connection details can be set on the work pool (via an
`ArmadaClusterConfig` or `ArmadaCredentials` block) or in the worker's
environment:

```bash
export PREFECT_INTEGRATIONS_ARMADA_CONNECTION_HOST="armada.example.com"
export PREFECT_INTEGRATIONS_ARMADA_CONNECTION_PORT="50051"
export PREFECT_INTEGRATIONS_ARMADA_CONNECTION_DISABLE_SSL="false"
```

The `ARMADA_SERVER` and `ARMADA_PORT` variables used by Armada's own tooling are
also honored.

Flows can also be sent to an Armada work pool directly, without a deployment:

```python
from prefect import flow
from prefect_armada.decorators import armada


@armada(work_pool="my-armada-pool")
@flow
def my_flow():
    print("Hello from Armada!")


my_flow()
```

### Submitting jobs from a flow

`ArmadaJob` submits an arbitrary Armada job and waits for it to finish:

```python
from prefect import flow
from prefect_armada import ArmadaJob
from prefect_armada.credentials import ArmadaCredentials


@flow
def run_a_job():
    job = ArmadaJob(
        credentials=ArmadaCredentials(),
        queue="prefect",
        job_request={
            "priority": 1,
            "podSpec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "prefect-job",
                        "image": "docker.io/library/ubuntu:latest",
                        "args": ["echo", "hello from armada"],
                        "resources": {
                            "requests": {"cpu": "120m", "memory": "510Mi"},
                            "limits": {"cpu": "120m", "memory": "510Mi"},
                        },
                    }
                ],
            },
        },
    )

    job_run = job.trigger()
    job_run.wait_for_completion()
    return job_run.fetch_result()
```

Individual Armada API calls are also available as tasks in
`prefect_armada.jobs`, `prefect_armada.jobsets`, `prefect_armada.queues`,
`prefect_armada.events`, and `prefect_armada.logs`.

## How it works

Armada is asynchronous: the worker submits a job and Armada schedules it onto one
of its Kubernetes clusters later. The worker therefore returns as soon as Armada
accepts the job, and an **observer** thread watches the job set's event stream to

- replicate Armada job events (`prefect.armada.job.*`) into Prefect's event
  system for use in Automations, and
- mark a flow run as `Crashed` when its Armada job fails before the flow run can
  report its own state, forwarding the job's logs to the flow run.

Each flow run is submitted to its own job set by default, so the observer's watch
ends when the flow run's job finishes. The observer can be disabled with
`PREFECT_INTEGRATIONS_ARMADA_OBSERVER_ENABLED=false`, and job sets submitted by
other processes can be watched with
`PREFECT_INTEGRATIONS_ARMADA_OBSERVER_JOB_SETS="my-queue/my-job-set"`.

## How the work pool UI finds Armada

Armada is not published to Prefect's collections registry, so the Prefect UI only
knows about it because the package is installed next to the server. On startup the
server loads every `prefect.collections` entry point, and `ArmadaWorker` supplies
its own presentation to the work pool creation screen:

```python
class ArmadaWorker(BaseWorker[...]):
    type: str = "armada"
    _display_name = "Armada"
    _description = "Execute flow runs within jobs scheduled on an Armada cluster..."
    _documentation_url = "https://docs.prefect.io/integrations/prefect-armada"
    _logo_url = "https://raw.githubusercontent.com/armadaproject/armada/master/logo.svg"
    _logo_resource = "frontend/armada.svg"
```

`_logo_resource` names an image inside this package (see
[`prefect_armada/frontend/`](prefect_armada/frontend/)), which the server reads and
serves as a data URL, so the logo does not depend on an external host being
reachable from the browser. `_logo_url` stays as the fallback if the packaged file
cannot be read.

The configuration fields on the work pool form come from `ArmadaWorkerVariables`
and `ArmadaWorkerJobConfiguration`. There is no second copy of that schema: editing
those Pydantic models is what changes the form.

Because the entry point is loaded at import time, **installing or upgrading
`prefect-armada` requires restarting the Prefect server** before its work pool type
appears, and a browser reload to pick up the new metadata. The UI itself does not
need rebuilding.

See [Contribute to integrations](https://docs.prefect.io/contribute/contribute-integrations)
for the general convention and its limits.

## Resources

Refer to the [Prefect Armada docs](https://docs.prefect.io/integrations/prefect-armada)
and the [Armada docs](https://armadaproject.io/) for more information.

### Feedback

If you encounter any bugs while using `prefect-armada`, feel free to open an
issue in the [prefect](https://github.com/PrefectHQ/prefect) repository.

### Contributing

If you'd like to help contribute to fix an issue or add a feature to
`prefect-armada`, please
[propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).
