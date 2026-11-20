# Two duet nodes in two zones sharing one regionally replicated disk, plus a
# client. This is the closest public-cloud shape to two control plane VMs on
# a two-host vSAN cluster: one multi-writer disk, synchronously replicated
# across two failure domains.
#
# Applied and destroyed by the lab-gcp workflow; see docs/LAB-RUNS.md.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.20"
    }
  }
}

variable "project" {
  type        = string
  description = "Google Cloud project to create the lab in"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zones" {
  type        = list(string)
  default     = ["us-central1-a", "us-central1-b"]
  description = "Exactly two zones; the shared disk is replicated across them"
}

variable "prefix" {
  type    = string
  default = "duet-lab"
}

variable "machine_type" {
  type        = string
  default     = "n4-standard-2"
  description = "Must be a series that supports Hyperdisk Balanced High Availability in multi-writer mode"
}

variable "shared_disk_gb" {
  type    = number
  default = 20
}

variable "max_run_seconds" {
  type        = number
  default     = 7200
  description = "Instances delete themselves after this long, in case teardown never runs"
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_compute_network" "lab" {
  name                    = "${var.prefix}-net"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "lab" {
  name          = "${var.prefix}-subnet"
  network       = google_compute_network.lab.id
  region        = var.region
  ip_cidr_range = "10.90.0.0/24"
}

# Nodes and the client talk to each other on the test HTTP port and the
# etcd client and peer ports.
resource "google_compute_firewall" "internal" {
  name          = "${var.prefix}-internal"
  network       = google_compute_network.lab.id
  source_ranges = [google_compute_subnetwork.lab.ip_cidr_range]
  allow {
    protocol = "tcp"
    ports    = ["7000", "2379", "2380"]
  }
}

# SSH only through Identity-Aware Proxy. No instance has a public address.
resource "google_compute_firewall" "iap_ssh" {
  name          = "${var.prefix}-iap-ssh"
  network       = google_compute_network.lab.id
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# The shared disk: multi-writer, replicated across both zones.
resource "google_compute_region_disk" "shared" {
  name          = "${var.prefix}-shared"
  region        = var.region
  type          = "hyperdisk-balanced-high-availability"
  replica_zones = var.zones
  size          = var.shared_disk_gb
  access_mode   = "READ_WRITE_MANY"
  labels        = { lab = var.prefix }
}

resource "google_compute_instance" "node" {
  count        = 2
  name         = "${var.prefix}-node-${count.index}"
  zone         = var.zones[count.index]
  machine_type = var.machine_type
  labels       = { lab = var.prefix }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      type  = "hyperdisk-balanced"
      size  = 10
    }
  }

  attached_disk {
    source      = google_compute_region_disk.shared.id
    device_name = "duet-shared"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.lab.id
  }

  metadata = { enable-oslogin = "TRUE" }

  scheduling {
    automatic_restart           = false
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "DELETE"
    max_run_duration {
      seconds = var.max_run_seconds
    }
  }
}

# Third member for the etcd control only. Three etcd members cannot be
# spread evenly over two zones: this one shares a zone with node 0, so that
# zone holds the majority.
resource "google_compute_instance" "third" {
  name         = "${var.prefix}-third"
  zone         = var.zones[0]
  machine_type = var.machine_type
  labels       = { lab = var.prefix }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      type  = "hyperdisk-balanced"
      size  = 10
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.lab.id
  }

  metadata = { enable-oslogin = "TRUE" }

  scheduling {
    automatic_restart           = false
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "DELETE"
    max_run_duration {
      seconds = var.max_run_seconds
    }
  }
}

# The load generator sits in the zone that does not hold the etcd majority,
# so it survives the loss of the other zone.
resource "google_compute_instance" "client" {
  name         = "${var.prefix}-client"
  zone         = var.zones[1]
  machine_type = var.machine_type
  labels       = { lab = var.prefix }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      type  = "hyperdisk-balanced"
      size  = 10
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.lab.id
  }

  metadata = { enable-oslogin = "TRUE" }

  scheduling {
    automatic_restart           = false
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "DELETE"
    max_run_duration {
      seconds = var.max_run_seconds
    }
  }
}

output "nodes" {
  value = [for n in google_compute_instance.node : {
    name = n.name
    zone = n.zone
    ip   = n.network_interface[0].network_ip
  }]
}

output "third" {
  value = {
    name = google_compute_instance.third.name
    zone = google_compute_instance.third.zone
    ip   = google_compute_instance.third.network_interface[0].network_ip
  }
}

output "client" {
  value = {
    name = google_compute_instance.client.name
    zone = google_compute_instance.client.zone
  }
}
