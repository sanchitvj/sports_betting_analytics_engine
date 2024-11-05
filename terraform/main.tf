module "security_groups" {
  source = "./security_groups"

  vpc_id          = var.vpc_id
  vpc_cidr        = var.vpc_cidr
  allowed_ip_range = var.allowed_ip_range
}

module "ec2" {
  source = "./ec2"

  subnet_id         = var.subnet_id
  security_group_id = module.security_groups.security_group_id
  key_pair_name     = var.key_pair_name
  iam_role_name     = var.iam_role_name
  availability_zone = ""
}