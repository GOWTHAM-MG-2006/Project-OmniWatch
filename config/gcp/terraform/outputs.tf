# ------------------------------------------------------------------
# OmniWatch — GCP Terraform Outputs
# Phase: 0 (Environment Setup)
# Purpose: Expose connection details for CI/CD and developer tooling
# ------------------------------------------------------------------

output "cluster_endpoint" {
  description = "GKE cluster API server endpoint (use with kubectl)"
  value       = google_container_cluster.omniwatch.endpoint
}

output "cluster_ca_certificate" {
  description = "Base64-encoded cluster CA certificate for secure kubeconfig"
  value       = google_container_cluster.omniwatch.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

output "artifact_registry_url" {
  description = "Artifact Registry Docker repository URL for pushing OmniWatch images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/omniwatch-images"
}

output "cluster_name" {
  description = "Name of the provisioned GKE cluster"
  value       = google_container_cluster.omniwatch.name
}
