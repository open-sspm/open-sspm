provider "scaleway" {
  project_id = var.project_id != "" ? var.project_id : null
  zone       = var.zone
}

resource "scaleway_instance_security_group" "demo" {
  name                    = "${var.server_name}-sg"
  inbound_default_policy  = "drop"
  outbound_default_policy = "accept"

  inbound_rule {
    action   = "accept"
    protocol = "TCP"
    port     = 22
    ip_range = var.admin_cidr
  }

  inbound_rule {
    action   = "accept"
    protocol = "TCP"
    port     = 80
    ip_range = "0.0.0.0/0"
  }

  inbound_rule {
    action   = "accept"
    protocol = "TCP"
    port     = 443
    ip_range = "0.0.0.0/0"
  }
}

resource "scaleway_instance_ip" "demo" {}

resource "scaleway_instance_server" "demo" {
  name              = var.server_name
  type              = var.instance_type
  image             = var.image
  ip_id             = scaleway_instance_ip.demo.id
  security_group_id = scaleway_instance_security_group.demo.id

  user_data = {
    cloud-init = <<-EOF
      #cloud-config
      users:
        - name: deploy
          groups: [sudo]
          shell: /bin/bash
          sudo: ALL=(ALL) NOPASSWD:ALL
          ssh_authorized_keys:
            - ${var.ssh_public_key}
      ssh_pwauth: false
      package_update: true
      package_upgrade: true
    EOF
  }
}
