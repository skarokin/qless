# Example Terraform configuration for a qless server running on Cloud Run.
#
# qless does its work AFTER the HTTP response is sent so the instance must keep CPU between requests.
# on Cloud Run that mean sinstance-based billing (cpu_idle = false).
#
# Other qless-relevant choices, called out inline below:
#   - min_instance_count = 1: scale-to-zero would discard the in-memory queue. Scale-to-zero is okay if
#     jobs are short and queue is small, since Cloud Run's autoscaler makes decisions from in-flight requests,
#     meaning qless' instant-202 from Cloud Run's perspective is an idle instance while jobs are being processed.
#   - Multiple instances each run an independent queue; the load balancer spreads jobs across them.
#     qless makes no cross-replica guarantees.
#   - Cloud Run allows ~10s between SIGTERM and SIGKILL. The example server budgets 3s HTTP drain + 6s queue drain, which fits.
#
# Usage:
#   terraform init
#   terraform apply -var project_id=my-project -var image=us-docker.pkg.dev/my-project/my-repo/qless-example:latest

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  description = "GCP project to deploy into."
  type        = string
}

variable "region" {
  description = "Cloud Run region."
  type        = string
  default     = "us-central1"
}

variable "image" {
  description = "Container image built from deploy/docker/Dockerfile, pushed to Artifact Registry."
  type        = string
}

variable "api_key" {
  description = "Value for the example server's API_KEY. Use Secret Manager instead of a plain variable for real deployments."
  type        = string
  sensitive   = true
  default     = ""
}

resource "google_cloud_run_v2_service" "qless" {
  name     = "qless-example"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  
  template {
    # Keep at least one instance warm: scaling to zero stops the container and discards any queued jobs
    # (an accepted qless risk, but min 1 makes it rare enough to not care about).
    scaling {
      min_instance_count = 1
      max_instance_count = 3
    }

    containers {
      image = var.image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        # Instance-based billing: CPU stays allocated after responses are sent,
        # which is what lets qless workers run in the background.
        cpu_idle          = false
        startup_cpu_boost = true
      }

      env {
        name  = "API_KEY"
        value = var.api_key
      }

      startup_probe {
        http_get {
          path = "/healthz"
        }
        initial_delay_seconds = 2
        period_seconds        = 5
        failure_threshold     = 6
      }

      liveness_probe {
        http_get {
          path = "/healthz"
        }
        period_seconds = 30
      }
    }

    # Each in-flight HTTP request is cheap for qless (it just enqueues), so a
    # high concurrency per instance is fine; the worker pool is bounded
    # separately by the qless Config.
    max_instance_request_concurrency = 80
  }
}

# The example server enforces its own X-API-Key check, so the endpoint is left
# publicly reachable. Remove this and use IAM/IAP if you want Google-managed auth.
resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = google_cloud_run_v2_service.qless.location
  name     = google_cloud_run_v2_service.qless.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

output "url" {
  value = google_cloud_run_v2_service.qless.uri
}
