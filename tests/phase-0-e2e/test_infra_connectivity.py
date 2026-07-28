"""
OmniWatch — Phase 0 E2E Test
Component: Infra Connectivity Validation
Phase: 0
Purpose: Validate all infrastructure services are reachable and functional
Inputs: docker-compose services (local) + Terraform configs (GCP)
Outputs: Test pass/fail results for each service
"""

import os
import sys
import subprocess
import time
import uuid
from pathlib import Path

import pytest
import requests

# =============================================================================
# Constants
# =============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOCKER_COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
TERRAFORM_DIR = PROJECT_ROOT / "config" / "gcp" / "terraform"
K8S_NAMESPACE_FILE = PROJECT_ROOT / "config" / "k8s" / "namespace.yaml"
K8S_INFRA_DIR = PROJECT_ROOT / "k8s" / "infra"

REQUIRED_SERVICES = [
    "zookeeper", "kafka", "clickhouse", "neo4j", "minio",
    "ollama", "opa",
]

SERVICE_PORTS = {
    "kafka":       {"api": 9092,  "type": "kafka"},
    "clickhouse":  {"api": 8123,  "type": "http"},
    "neo4j":       {"api": 7474,  "type": "http"},
    "minio":       {"api": 9001,  "type": "http"},
    "ollama":      {"api": 11434, "type": "http"},
    "opa":         {"api": 8181,  "type": "http"},
}

DOCKER_HOST = os.environ.get("DOCKER_HOST", "localhost")
BASE_URL = f"http://{DOCKER_HOST}"

SERVICE_HEALTH_ENDPOINTS = {
    "clickhouse":  f"{BASE_URL}:8123/ping",
    "neo4j":       f"{BASE_URL}:7474",
    "minio":       f"{BASE_URL}:9001/minio/health/live",
    "ollama":      f"{BASE_URL}:11434/api/tags",
    "opa":         f"{BASE_URL}:8181/health",
}


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def docker_compose_running():
    """Check if docker-compose services are running."""
    try:
        result = subprocess.run(
            ["docker-compose", "ps", "--services", "--filter", "status=running"],
            capture_output=True, text=True, timeout=10,
            cwd=PROJECT_ROOT,
        )
        return result.stdout.strip().splitlines()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pytest.skip("Docker Compose not available")
        return []


# =============================================================================
# Directory & Config Structure Tests
# =============================================================================

class TestPhase0Structure:
    """Validate all Phase 0 directories and config files exist."""

    def test_docker_compose_exists(self):
        assert DOCKER_COMPOSE_FILE.exists(), "docker-compose.yml missing"
        content = DOCKER_COMPOSE_FILE.read_text()
        for svc in REQUIRED_SERVICES:
            assert f"  {svc}:" in content or f"  {svc}_" in content, \
                f"Service {svc} not defined in docker-compose.yml"

    def test_docker_compose_has_all_services(self):
        content = DOCKER_COMPOSE_FILE.read_text()
        for svc in REQUIRED_SERVICES:
            assert f"  {svc}:" in content, f"Service '{svc}' missing from docker-compose.yml"

    def test_env_example_exists(self):
        env_file = PROJECT_ROOT / ".env.example"
        assert env_file.exists(), ".env.example missing"
        content = env_file.read_text()
        assert len(content) > 200, ".env.example is too sparse"
        # Verify key env vars
        required_vars = [
            "KAFKA_HOST", "CLICKHOUSE_HOST", "NEO4J_HOST",
            "MINIO_HOST",
        ]
        for var in required_vars:
            assert var in content, f"Required env var {var} missing from .env.example"

    

    def test_terraform_dir_exists(self):
        assert TERRAFORM_DIR.exists(), "config/gcp/terraform/ missing"
        required_files = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for f in required_files:
            assert (TERRAFORM_DIR / f).exists(), f"Terraform {f} missing"

    def test_k8s_namespace_exists(self):
        assert K8S_NAMESPACE_FILE.exists(), "config/k8s/namespace.yaml missing"
        content = K8S_NAMESPACE_FILE.read_text()
        assert "omniwatch" in content, "namespace.yaml missing 'omniwatch' reference"

    def test_k8s_infra_manifests_exist(self):
        assert K8S_INFRA_DIR.exists(), "k8s/infra/ missing"
        infra_dirs = [d.name for d in K8S_INFRA_DIR.iterdir() if d.is_dir()]
        for svc in REQUIRED_SERVICES:
            svc_dir = K8S_INFRA_DIR / svc
            assert svc_dir.exists(), f"k8s/infra/{svc}/ missing"
            assert (svc_dir / "deployment.yaml").exists(), \
                f"k8s/infra/{svc}/deployment.yaml missing"
            assert (svc_dir / "service.yaml").exists(), \
                f"k8s/infra/{svc}/service.yaml missing"


# =============================================================================
# Docker Compose Connectivity Tests
# =============================================================================

class TestDockerComposeConnectivity:
    """Validate running Docker services respond on expected ports."""

    def test_all_services_running(self, docker_compose_running):
        for svc in REQUIRED_SERVICES:
            assert svc in docker_compose_running, \
                f"Service {svc} is not running (docker-compose ps)"

    def test_http_services_health(self):
        """Check HTTP health endpoints for all services with HTTP API."""
        failures = []
        for service, url in SERVICE_HEALTH_ENDPOINTS.items():
            try:
                resp = requests.get(url, timeout=5)
                assert resp.status_code in (200, 204, 401, 403), \
                    f"{service} returned {resp.status_code}"
            except (requests.ConnectionError, requests.Timeout) as e:
                failures.append(f"{service} unreachable at {url}: {e}")
        if failures:
            pytest.fail("\n".join(failures))

    def test_kafka_topic_create_list(self):
        """Validate Kafka can create and list topics."""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import NoBrokersAvailable

            admin = KafkaAdminClient(
                bootstrap_services=f"{DOCKER_HOST}:9092",
                client_id="omniwatch-phase0-test",
                request_timeout_ms=5000,
            )
            test_topic = f"omniwatch.test.{uuid.uuid4().hex[:8]}"
            admin.create_topics([NewTopic(test_topic, 1, 1)])
            topics = admin.list_topics()
            admin.delete_topics([test_topic])
            admin.close()
            assert test_topic in topics, f"Kafka topic {test_topic} not created"
        except ImportError:
            pytest.skip("kafka-python not installed")
        except NoBrokersAvailable:
            pytest.fail("Kafka broker not reachable at localhost:9092")

    def test_clickhouse_query(self):
        """Validate ClickHouse accepts queries."""
        try:
            from clickhouse_driver import Client as CHClient
            client = CHClient(host=DOCKER_HOST, port=9000)
            result = client.execute("SELECT 1")
            assert result == [(1,)], f"ClickHouse query failed: {result}"
            client.disconnect()
        except ImportError:
            pytest.skip("clickhouse-driver not installed")
        except Exception as e:
            pytest.fail(f"ClickHouse connection failed: {e}")

    

    def test_minio_bucket_list(self):
        """Validate MinIO lists buckets."""
        try:
            from minio import Minio
            client = Minio(
                f"{DOCKER_HOST}:9010",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False,
            )
            buckets = client.list_buckets()
            assert isinstance(buckets, list), "MinIO list_buckets failed"
        except ImportError:
            pytest.skip("minio not installed")
        except Exception as e:
            pytest.fail(f"MinIO connection failed: {e}")


# =============================================================================
# Terraform Validation Tests
# =============================================================================

class TestTerraformConfig:
    """Validate Terraform files are syntactically correct."""

    def test_terraform_dirs_exist(self):
        assert TERRAFORM_DIR.exists(), "config/gcp/terraform/ missing"

    def test_terraform_files_exist(self):
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for f in required:
            assert (TERRAFORM_DIR / f).exists(), f"Missing: config/gcp/terraform/{f}"

    def test_terraform_validate(self):
        """Run `terraform fmt -check` and `terraform validate`."""
        try:
            fmt_result = subprocess.run(
                ["terraform", "fmt", "-check", "-recursive"],
                capture_output=True, text=True, timeout=30,
                cwd=TERRAFORM_DIR,
            )
            init_result = subprocess.run(
                ["terraform", "init", "-backend=false"],
                capture_output=True, text=True, timeout=60,
                cwd=TERRAFORM_DIR,
            )
            assert init_result.returncode == 0, \
                f"terraform init failed:\n{init_result.stderr}"
            validate_result = subprocess.run(
                ["terraform", "validate"],
                capture_output=True, text=True, timeout=60,
                cwd=TERRAFORM_DIR,
            )
            assert validate_result.returncode == 0, \
                f"terraform validate failed:\n{validate_result.stderr}"
        except FileNotFoundError:
            pytest.skip("terraform CLI not found")


# =============================================================================
# K8s Manifest Validation Tests
# =============================================================================

class TestK8sManifests:
    """Validate K8s manifests have correct structure."""

    def test_namespace_yaml_valid(self):
        content = K8S_NAMESPACE_FILE.read_text()
        assert "kind: Namespace" in content
        assert "metadata:" in content
        assert "name:" in content

    def test_infra_deployments_have_required_fields(self):
        for svc in REQUIRED_SERVICES:
            deploy_file = K8S_INFRA_DIR / svc / "deployment.yaml"
            assert deploy_file.exists(), f"{svc}/deployment.yaml missing"
            content = deploy_file.read_text()
            assert "kind: Deployment" in content, \
                f"{svc}/deployment.yaml missing 'kind: Deployment'"
            assert "containers:" in content, \
                f"{svc}/deployment.yaml missing 'containers:'"
            assert "image:" in content, \
                f"{svc}/deployment.yaml missing 'image:'"
            if svc != "opa":
                assert "ports:" in content, \
                    f"{svc}/deployment.yaml missing 'ports:'"

    def test_infra_services_have_required_fields(self):
        for svc in REQUIRED_SERVICES:
            svc_file = K8S_INFRA_DIR / svc / "service.yaml"
            assert svc_file.exists(), f"{svc}/service.yaml missing"
            content = svc_file.read_text()
            assert "kind: Service" in content, \
                f"{svc}/service.yaml missing 'kind: Service'"
            assert "ports:" in content, \
                f"{svc}/service.yaml missing 'ports:'"


# =============================================================================
# Helper Check (for running as standalone script)
# =============================================================================

def check_docker_compose_config():
    """Check docker-compose config is valid."""
    try:
        result = subprocess.run(
            ["docker-compose", "config", "-q"],
            capture_output=True, text=True, timeout=15,
            cwd=PROJECT_ROOT,
        )
        return result.returncode == 0, result.stderr
    except FileNotFoundError:
        return False, "docker-compose CLI not found"


if __name__ == "__main__":
    """Standalone infra check (runs without pytest)."""
    print("=" * 60)
    print("OmniWatch — Phase 0 Infrastructure Check")
    print("=" * 60)

    # 1. Check files exist
    print("\n[1/4] Checking file structure...")
    checks = [
        ("docker-compose.yml", DOCKER_COMPOSE_FILE.exists()),
        (".env.example", (PROJECT_ROOT / ".env.example").exists()),
        ("config/gcp/terraform/", TERRAFORM_DIR.exists()),
        ("config/k8s/namespace.yaml", K8S_NAMESPACE_FILE.exists()),
        ("k8s/infra/", K8S_INFRA_DIR.exists()),
    ]
    for name, ok in checks:
        status = "✓" if ok else "✗"
        print(f"  {status} {name}")

    # 2. Check docker-compose config
    print("\n[2/4] Checking docker-compose config...")
    config_ok, err = check_docker_compose_config()
    print(f"  {'✓' if config_ok else '✗'} docker-compose config: {'valid' if config_ok else err}")

    # 3. Check services if running
    print("\n[3/4] Checking running services...")
    try:
        result = subprocess.run(
            ["docker-compose", "ps", "--services", "--filter", "status=running"],
            capture_output=True, text=True, timeout=10,
            cwd=PROJECT_ROOT,
        )
        running = result.stdout.strip().splitlines()
        for svc in REQUIRED_SERVICES:
            status = "✓" if svc in running else " "
            print(f"  {status} {svc}")
    except Exception as e:
        print(f"  ⚠ Could not check running services: {e}")

    # 4. Summary
    print("\n[4/4] Summary")
    print(f"  Files ok: {sum(1 for _, ok in checks if ok)}/{len(checks)}")
    print(f"  Config valid: {config_ok}")
    print(f"  Services running: {len(running) if 'running' in dir() else 'N/A'}/{len(REQUIRED_SERVICES)}")
    print()