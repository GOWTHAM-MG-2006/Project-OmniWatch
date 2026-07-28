# ---------------------------------------------------------------------------
# OmniWatch — GCP Infrastructure (Phase 0 — Environment Setup)
# Terraform ~> 1.5 | Provider hashicorp/google ~> 5.0
# Purpose: Provision GKE Autopilot cluster + supporting resources for
#          OmniWatch AIOps platform microservices (dev sandbox).
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# GCP Provider
# ---------------------------------------------------------------------------
provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# VPC Network & Subnet
# ---------------------------------------------------------------------------
resource "google_compute_network" "omniwatch" {
  name                    = "omniwatch-vpc"
  description             = "VPC for OmniWatch GKE cluster and supporting services"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "omniwatch" {
  name          = "omniwatch-subnet"
  description   = "Primary subnet for OmniWatch GKE nodes and internal load balancers"
  network       = google_compute_network.omniwatch.id
  region        = var.region
  ip_cidr_range = "10.0.0.0/16"

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.1.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.2.0.0/20"
  }
}

# ---------------------------------------------------------------------------
# Firewall — Internal Ingress
# ---------------------------------------------------------------------------
resource "google_compute_firewall" "allow_internal_ingress" {
  name        = "omniwatch-allow-internal-ingress"
  description = "Allow internal VPC traffic on all ports used by OmniWatch infra services"
  network     = google_compute_network.omniwatch.name
  priority    = 1000

  allow {
    protocol = "tcp"
    ports = [
      "8080",  # Dashboard backend
      "4317",  # OTLP gRPC
      "4318",  # OTLP HTTP
      "9092",  # Kafka
      "9000",  # ClickHouse native / MinIO API
      "8123",  # ClickHouse HTTP
      "7474",  # Neo4j HTTP
      "7687",  # Neo4j Bolt
      "9001",  # MinIO Console
      "9010",  # Reserved / auxiliary
      "3100",  # Loki
      "16686", # Jaeger UI
      "6379",  # Redis
      "8181",  # OPA
      "11434", # Ollama
      "9090",  # Prometheus
    ]
  }

  source_ranges = ["10.0.0.0/8"]
  target_tags   = ["omniwatch"]
}

# ---------------------------------------------------------------------------
# GKE Autopilot Cluster
# ---------------------------------------------------------------------------
resource "google_container_cluster" "omniwatch" {
  name        = var.cluster_name
  description = "GKE Autopilot cluster for OmniWatch AIOps microservices"
  location    = var.region

  # ── Autopilot mode (NOT standard) ──────────────────────────────────
  enable_autopilot = true

  # ── VPC integration ───────────────────────────────────────────────
  network    = google_compute_network.omniwatch.id
  subnetwork = google_compute_subnetwork.omniwatch.id

  # ── IP allocation (Autopilot requires explicit policy) ────────────
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # ── Release channel ───────────────────────────────────────────────
  release_channel {
    channel = "STABLE"
  }

  # ── Workload Identity ─────────────────────────────────────────────
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # ── Private cluster (public endpoint for dev sandbox) ─────────────
  private_cluster_config {
    enable_private_endpoint = false
    enable_private_nodes    = false
  }

  # ── Labels ────────────────────────────────────────────────────────
  resource_labels = {
    environment = var.environment
    managed_by  = "terraform"
    project     = "omniwatch"
  }

  # GKE Autopilot manages nodes — no node_pool blocks allowed.
  # Allow destroy for dev sandbox.
  deletion_protection = false
}

# ---------------------------------------------------------------------------
# Artifact Registry — Docker Repository
# ---------------------------------------------------------------------------
resource "google_artifact_registry_repository" "omniwatch_images" {
  location      = var.region
  repository_id = "omniwatch-images"
  description   = "Docker image repository for OmniWatch microservices"
  format        = "DOCKER"

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    project     = "omniwatch"
  }
}

# ---------------------------------------------------------------------------
# Service Account & IAM Bindings
# ---------------------------------------------------------------------------
resource "google_service_account" "omniwatch_sa" {
  account_id   = "omniwatch-sa"
  display_name = "OmniWatch Service Account"
  description  = "GCP service account for OmniWatch workloads running on GKE"
}

resource "google_project_iam_member" "container_developer" {
  project = var.project_id
  role    = "roles/container.developer"
  member  = "serviceAccount:${google_service_account.omniwatch_sa.email}"
}

resource "google_project_iam_member" "logging_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.omniwatch_sa.email}"
}

resource "google_project_iam_member" "monitoring_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.omniwatch_sa.email}"
}

# Bind the K8s service account (omniwatch/omniwatch-sa) to the GCP SA
# via Workload Identity.
resource "google_service_account_iam_member" "workload_identity_binding" {
  service_account_id = google_service_account.omniwatch_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[omniwatch/omniwatch-sa]"
}

# ---------------------------------------------------------------------------
# Kubernetes Provider — Bootstrapped from GKE cluster
# ---------------------------------------------------------------------------
data "google_client_config" "default" {}

provider "kubernetes" {
  host  = "https://${google_container_cluster.omniwatch.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(
    google_container_cluster.omniwatch.master_auth[0].cluster_ca_certificate
  )
}

resource "kubernetes_namespace" "omniwatch" {
  metadata {
    name = "omniwatch"
    labels = {
      environment = var.environment
      project     = "omniwatch"
    }
  }
  depends_on = [google_container_cluster.omniwatch]
}

resource "kubernetes_service_account" "omniwatch_sa" {
  metadata {
    name      = "omniwatch-sa"
    namespace = kubernetes_namespace.omniwatch.metadata[0].name
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.omniwatch_sa.email
    }
  }
  depends_on = [kubernetes_namespace.omniwatch]
}
