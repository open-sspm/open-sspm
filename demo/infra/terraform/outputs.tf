output "public_ip" {
  description = "Server public IPv4."
  value       = scaleway_instance_ip.demo.address
}

output "server_id" {
  description = "Scaleway instance server ID."
  value       = scaleway_instance_server.demo.id
}

