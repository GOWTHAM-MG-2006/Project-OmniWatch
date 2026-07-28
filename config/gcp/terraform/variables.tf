# ------------------------------------------------------------------
# OmniWatch — GCP Terraform Variables
# Phase: 0 (Environment Setup)
# Purpose: Input variables for OmniWatch GCP infrastructure
# ------------------------------------------------------------------

variable "project_id" {
  type        = string
  description = "GCP project ID where OmniWatch resources will be provisioned"
}

variable "region" {
  type        = string
  description = "GCP region for resource deployment"
  default     = "us-central1"
}

variable "cluster_name" {
  type        = string
  description = "Name of the GKE Autopilot cluster"
  default     = "omniwatch-cluster"
}

variable "environment" {
  type        = string
  description = "Deployment environment label (e.g. development, staging)"
  default     = "development"
}
