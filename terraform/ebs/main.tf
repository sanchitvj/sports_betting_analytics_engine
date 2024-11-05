resource "aws_ebs_volume" "kafka_data" {
  availability_zone = var.availability_zone
  size             = 64
  type             = "gp3"
  iops             = 3000
  throughput       = 125

  tags = {
    Name = "kafka-data"
  }
}