import uuid
from unittest.mock import MagicMock

import anyio.abc
import grpc
import pytest
from armada_client.armada import submit_pb2
from conftest import FakeRpcError, make_job_submit_response
from prefect_armada import ArmadaWorker
from prefect_armada.credentials import ArmadaClusterConfig, ArmadaCredentials
from prefect_armada.worker import ArmadaWorkerJobConfiguration
from pydantic import ValidationError

from prefect.client.schemas import FlowRun
from prefect.exceptions import InfrastructureError, InfrastructureNotFound
from prefect.utilities.dockerutils import get_prefect_image_name


@pytest.fixture(autouse=True)
def mock_observer(monkeypatch: pytest.MonkeyPatch):
    mocks = MagicMock()
    monkeypatch.setattr("prefect_armada.worker.start_observer", mocks.start)
    monkeypatch.setattr("prefect_armada.worker.stop_observer", mocks.stop)
    monkeypatch.setattr("prefect_armada.worker.observe_job_set", mocks.observe)
    return mocks


@pytest.fixture
def flow_run():
    return FlowRun(flow_id=uuid.uuid4(), name="my-flow-run-name")


@pytest.fixture
async def default_configuration():
    return await ArmadaWorkerJobConfiguration.from_template_and_values(
        ArmadaWorker.get_default_base_job_template(), {}
    )


class TestArmadaWorkerJobConfiguration:
    def test_default_template_is_a_job_submit_request(self):
        template = ArmadaWorker.get_default_base_job_template()
        job_manifest = template["job_configuration"]["job_manifest"]

        assert job_manifest["podSpec"]["restartPolicy"] == "Never"
        assert job_manifest["podSpec"]["containers"][0]["name"] == "prefect-job"
        assert set(template["variables"]["properties"]) >= {
            "queue",
            "job_set_id",
            "namespace",
            "priority",
            "image",
            "cpu_request",
            "memory_limit",
            "cluster_config",
            "credentials",
        }

    async def test_validates_missing_pod_spec(self):
        with pytest.raises(ValidationError, match="missing required attributes"):
            ArmadaWorkerJobConfiguration(job_manifest={"namespace": "default"})

    async def test_validates_incompatible_restart_policy(self):
        with pytest.raises(ValidationError, match="incompatible values"):
            ArmadaWorkerJobConfiguration(
                job_manifest={
                    "podSpec": {
                        "restartPolicy": "Always",
                        "containers": [{"name": "prefect-job"}],
                    }
                }
            )

    async def test_accepts_the_protobuf_pod_spec_field_name(self):
        configuration = ArmadaWorkerJobConfiguration(
            job_manifest={
                "pod_spec": {
                    "restartPolicy": "Never",
                    "containers": [{"name": "prefect-job"}],
                }
            }
        )

        assert "podSpec" in configuration.job_manifest
        assert "pod_spec" not in configuration.job_manifest

    async def test_namespace_is_added_to_the_manifest(self):
        configuration = ArmadaWorkerJobConfiguration(
            namespace="my-namespace",
            job_manifest={
                "podSpec": {
                    "restartPolicy": "Never",
                    "containers": [{"name": "prefect-job"}],
                }
            },
        )

        assert configuration.job_manifest["namespace"] == "my-namespace"

    async def test_prepare_for_flow_run_populates_the_manifest(
        self, default_configuration, flow_run
    ):
        default_configuration.prepare_for_flow_run(flow_run)
        manifest = default_configuration.job_manifest
        container = manifest["podSpec"]["containers"][0]

        assert container["image"] == get_prefect_image_name()
        assert container["args"] == ["prefect", "flow-run", "execute"]
        assert manifest["namespace"] == "default"
        assert manifest["labels"]["prefect.io/flow-run-id"] == str(flow_run.id)
        assert manifest["annotations"]["prefect.io/flow-run-id"] == str(flow_run.id)
        assert manifest["externalJobUri"] == f"prefect://flow-run/{flow_run.id}"
        assert default_configuration.job_set_id.endswith(str(flow_run.id))

    async def test_prepare_for_flow_run_converts_env_to_a_list(
        self, default_configuration, flow_run
    ):
        default_configuration.env = {"MY_VAR": "my-value"}
        default_configuration.prepare_for_flow_run(flow_run)

        env = default_configuration.job_manifest["podSpec"]["containers"][0]["env"]

        assert {"name": "MY_VAR", "value": "my-value"} in env
        assert all(isinstance(entry, dict) for entry in env)

    async def test_prepare_for_flow_run_preserves_hardcoded_env_entries(self, flow_run):
        template = ArmadaWorker.get_default_base_job_template()
        template["job_configuration"]["job_manifest"]["podSpec"]["containers"][0][
            "env"
        ] = [
            {
                "name": "MY_SECRET",
                "valueFrom": {"secretKeyRef": {"name": "secret", "key": "key"}},
            }
        ]
        configuration = await ArmadaWorkerJobConfiguration.from_template_and_values(
            template, {}
        )

        configuration.prepare_for_flow_run(flow_run)

        env = configuration.job_manifest["podSpec"]["containers"][0]["env"]
        assert {
            "name": "MY_SECRET",
            "valueFrom": {"secretKeyRef": {"name": "secret", "key": "key"}},
        } in env
        assert any(entry.get("name") == "PREFECT__FLOW_RUN_ID" for entry in env)

    async def test_prepare_for_flow_run_slugifies_labels_and_annotations(
        self, default_configuration, flow_run
    ):
        default_configuration.labels = {"my label": "my value"}
        default_configuration.annotations = {"my annotation": "an arbitrary: value"}

        default_configuration.prepare_for_flow_run(flow_run)

        manifest = default_configuration.job_manifest
        assert manifest["labels"]["my-label"] == "my-value"
        # Annotation values are arbitrary strings, so only keys are slugified
        assert manifest["annotations"]["my-annotation"] == "an arbitrary: value"

    async def test_prepare_for_flow_run_keeps_a_configured_job_set_id(
        self, default_configuration, flow_run
    ):
        default_configuration.job_set_id = "my-job-set"
        default_configuration.prepare_for_flow_run(flow_run)

        assert default_configuration.job_set_id == "my-job-set"

    async def test_prepare_for_flow_run_keeps_a_configured_image(self, flow_run):
        configuration = await ArmadaWorkerJobConfiguration.from_template_and_values(
            ArmadaWorker.get_default_base_job_template(),
            {"image": "my-registry/my-image:latest"},
        )

        configuration.prepare_for_flow_run(flow_run)

        assert (
            configuration.job_manifest["podSpec"]["containers"][0]["image"]
            == "my-registry/my-image:latest"
        )

    async def test_resource_requests_are_templated(self, flow_run):
        configuration = await ArmadaWorkerJobConfiguration.from_template_and_values(
            ArmadaWorker.get_default_base_job_template(),
            {"cpu_request": "500m", "memory_limit": "1Gi"},
        )

        configuration.prepare_for_flow_run(flow_run)

        resources = configuration.job_manifest["podSpec"]["containers"][0]["resources"]
        assert resources["requests"]["cpu"] == "500m"
        assert resources["limits"]["memory"] == "1Gi"

    async def test_get_environment_variable_value(
        self, default_configuration, flow_run
    ):
        default_configuration.env = {"MY_VAR": "my-value"}
        default_configuration.prepare_for_flow_run(flow_run)

        assert default_configuration.get_environment_variable_value("MY_VAR") == (
            "my-value"
        )
        assert default_configuration.get_environment_variable_value("NOPE") is None

    def test_get_credentials_defaults_to_the_environment(self, default_configuration):
        credentials = default_configuration.get_credentials()

        assert isinstance(credentials, ArmadaCredentials)
        assert credentials.cluster_config is None

    def test_get_credentials_uses_the_cluster_config(self, default_configuration):
        cluster_config = ArmadaClusterConfig(host="armada.example.com")
        default_configuration.cluster_config = cluster_config

        credentials = default_configuration.get_credentials()

        assert credentials.cluster_config is cluster_config

    def test_get_credentials_prefers_the_credentials_block(self, default_configuration):
        credentials_config = ArmadaClusterConfig(host="from-credentials.example.com")
        default_configuration.credentials = ArmadaCredentials(
            cluster_config=credentials_config, token="abc123"
        )
        default_configuration.cluster_config = ArmadaClusterConfig(
            host="from-configuration.example.com"
        )

        credentials = default_configuration.get_credentials()

        assert credentials.cluster_config is credentials_config

    def test_get_credentials_combines_auth_and_cluster_config(
        self, default_configuration
    ):
        cluster_config = ArmadaClusterConfig(host="armada.example.com")
        default_configuration.credentials = ArmadaCredentials(token="abc123")
        default_configuration.cluster_config = cluster_config

        credentials = default_configuration.get_credentials()

        assert credentials.cluster_config == cluster_config
        assert credentials.token.get_secret_value() == "abc123"


class TestArmadaWorker:
    async def test_submits_a_job_and_reports_its_pid(
        self, default_configuration, flow_run, mock_armada_client, mock_observer
    ):
        mock_armada_client.submit_jobs.return_value = make_job_submit_response()
        default_configuration.prepare_for_flow_run(flow_run)

        async with ArmadaWorker(work_pool_name="test") as worker:
            result = await worker.run(
                flow_run=flow_run, configuration=default_configuration
            )

        expected_pid = f"prefect:{default_configuration.job_set_id}:test-job-id"
        assert result.identifier == expected_pid
        assert result.status_code == 0

        call = mock_armada_client.submit_jobs.call_args[1]
        assert call["queue"] == "prefect"
        assert call["job_set_id"] == default_configuration.job_set_id
        assert (
            call["job_request_items"][0].pod_spec.containers[0].image
            == get_prefect_image_name()
        )

    async def test_task_status_receives_the_job_pid(
        self, default_configuration, flow_run, mock_armada_client
    ):
        mock_armada_client.submit_jobs.return_value = make_job_submit_response()
        default_configuration.prepare_for_flow_run(flow_run)
        fake_status = MagicMock(spec=anyio.abc.TaskStatus)

        async with ArmadaWorker(work_pool_name="test") as worker:
            result = await worker.run(
                flow_run=flow_run,
                configuration=default_configuration,
                task_status=fake_status,
            )

        fake_status.started.assert_called_once_with(result.identifier)

    async def test_registers_the_job_set_with_the_observer(
        self, default_configuration, flow_run, mock_armada_client, mock_observer
    ):
        mock_armada_client.submit_jobs.return_value = make_job_submit_response()
        default_configuration.prepare_for_flow_run(flow_run)

        async with ArmadaWorker(work_pool_name="test") as worker:
            await worker.run(flow_run=flow_run, configuration=default_configuration)

        mock_observer.observe.assert_called_once()
        assert mock_observer.observe.call_args[1]["queue"] == "prefect"
        assert (
            mock_observer.observe.call_args[1]["job_set_id"]
            == default_configuration.job_set_id
        )

    async def test_initiate_run_submits_a_job(
        self, default_configuration, flow_run, mock_armada_client
    ):
        mock_armada_client.submit_jobs.return_value = make_job_submit_response()
        default_configuration.prepare_for_flow_run(flow_run)

        async with ArmadaWorker(work_pool_name="test") as worker:
            await worker._initiate_run(
                flow_run=flow_run, configuration=default_configuration
            )

        mock_armada_client.submit_jobs.assert_awaited_once()

    async def test_raises_infrastructure_error_when_armada_rejects_the_job(
        self, default_configuration, flow_run, mock_armada_client
    ):
        mock_armada_client.submit_jobs.return_value = make_job_submit_response(
            error="pod spec is invalid"
        )
        default_configuration.prepare_for_flow_run(flow_run)

        async with ArmadaWorker(work_pool_name="test") as worker:
            with pytest.raises(InfrastructureError, match="pod spec is invalid"):
                await worker.run(flow_run=flow_run, configuration=default_configuration)

    async def test_raises_infrastructure_error_with_a_hint_for_a_missing_queue(
        self, default_configuration, flow_run, mock_armada_client
    ):
        mock_armada_client.submit_jobs.side_effect = FakeRpcError(
            grpc.StatusCode.NOT_FOUND, "queue not found"
        )
        default_configuration.prepare_for_flow_run(flow_run)

        async with ArmadaWorker(work_pool_name="test") as worker:
            with pytest.raises(
                InfrastructureError, match="Verify that the queue 'prefect' exists"
            ):
                await worker.run(flow_run=flow_run, configuration=default_configuration)

    async def test_raises_infrastructure_error_with_a_hint_for_mismatched_resources(
        self, default_configuration, flow_run, mock_armada_client
    ):
        # Armada rejects a container whose requests and limits differ.
        mock_armada_client.submit_jobs.side_effect = FakeRpcError(
            grpc.StatusCode.INVALID_ARGUMENT,
            "container prefect-job defines different resources for requests and limits",
        )
        default_configuration.prepare_for_flow_run(flow_run)

        async with ArmadaWorker(work_pool_name="test") as worker:
            with pytest.raises(
                InfrastructureError, match="requests to equal its limits"
            ):
                await worker.run(flow_run=flow_run, configuration=default_configuration)

    async def test_retries_job_submission(
        self, default_configuration, flow_run, mock_armada_client, monkeypatch
    ):
        monkeypatch.setenv(
            "PREFECT_INTEGRATIONS_ARMADA_WORKER_SUBMIT_JOB_RETRY_DELAY_SECONDS", "0"
        )
        monkeypatch.setenv(
            "PREFECT_INTEGRATIONS_ARMADA_WORKER_SUBMIT_JOB_RETRY_JITTER_MAX_SECONDS",
            "0",
        )
        mock_armada_client.submit_jobs.side_effect = [
            FakeRpcError(grpc.StatusCode.UNAVAILABLE, "connection refused"),
            make_job_submit_response(),
        ]
        default_configuration.prepare_for_flow_run(flow_run)

        async with ArmadaWorker(work_pool_name="test") as worker:
            result = await worker.run(
                flow_run=flow_run, configuration=default_configuration
            )

        assert result.identifier.endswith("test-job-id")
        assert mock_armada_client.submit_jobs.await_count == 2

    async def test_kill_infrastructure_cancels_the_job(
        self, default_configuration, mock_armada_client
    ):
        mock_armada_client.cancel_jobs.return_value = submit_pb2.CancellationResult(
            cancelled_ids=["test-job-id"]
        )

        async with ArmadaWorker(work_pool_name="test") as worker:
            await worker.kill_infrastructure(
                infrastructure_pid="prefect:my-job-set:test-job-id",
                configuration=default_configuration,
            )

        assert mock_armada_client.cancel_jobs.call_args[1] == {
            "queue": "prefect",
            "job_set_id": "my-job-set",
            "job_id": "test-job-id",
        }

    async def test_kill_infrastructure_raises_when_nothing_was_cancelled(
        self, default_configuration, mock_armada_client
    ):
        mock_armada_client.cancel_jobs.return_value = submit_pb2.CancellationResult()

        async with ArmadaWorker(work_pool_name="test") as worker:
            with pytest.raises(InfrastructureNotFound, match="could not be cancelled"):
                await worker.kill_infrastructure(
                    infrastructure_pid="prefect:my-job-set:test-job-id",
                    configuration=default_configuration,
                )

    async def test_kill_infrastructure_raises_when_the_job_is_not_found(
        self, default_configuration, mock_armada_client
    ):
        mock_armada_client.cancel_jobs.side_effect = FakeRpcError(
            grpc.StatusCode.NOT_FOUND, "no such job"
        )

        async with ArmadaWorker(work_pool_name="test") as worker:
            with pytest.raises(InfrastructureNotFound, match="not found in queue"):
                await worker.kill_infrastructure(
                    infrastructure_pid="prefect:my-job-set:test-job-id",
                    configuration=default_configuration,
                )

    async def test_kill_infrastructure_reraises_other_rpc_errors(
        self, default_configuration, mock_armada_client
    ):
        mock_armada_client.cancel_jobs.side_effect = FakeRpcError(
            grpc.StatusCode.PERMISSION_DENIED, "not allowed"
        )

        async with ArmadaWorker(work_pool_name="test") as worker:
            with pytest.raises(grpc.RpcError):
                await worker.kill_infrastructure(
                    infrastructure_pid="prefect:my-job-set:test-job-id",
                    configuration=default_configuration,
                )

    async def test_kill_infrastructure_rejects_a_malformed_pid(
        self, default_configuration, mock_armada_client
    ):
        async with ArmadaWorker(work_pool_name="test") as worker:
            with pytest.raises(ValueError, match="Invalid infrastructure_pid"):
                await worker.kill_infrastructure(
                    infrastructure_pid="not-a-pid",
                    configuration=default_configuration,
                )

    async def test_observer_starts_and_stops_with_the_worker(self, mock_observer):
        async with ArmadaWorker(work_pool_name="test"):
            mock_observer.start.assert_called_once()
            mock_observer.stop.assert_not_called()

        mock_observer.stop.assert_called_once()

    async def test_observer_can_be_disabled(
        self, mock_observer, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("PREFECT_INTEGRATIONS_ARMADA_OBSERVER_ENABLED", "false")

        async with ArmadaWorker(work_pool_name="test"):
            pass

        mock_observer.start.assert_not_called()
        mock_observer.stop.assert_not_called()
