# Taken from: https://runs-on.com/guides/building-custom-ami-with-packer/
packer {
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = "~> 1"
    }
  }
}

# This is the subnet to deploy the Packer building EC2 instances to
# Use any subnet from our AWS account
variable "subnet_id" {
  type = string
}

variable "region" {
  type = string
  # Our default region is us-west-2, but we may choose to deploy across multiple
  # regions in the future.
  default = "us-west-2"
}

variable "image_name_prefix" {
  type = string
  # Prefix for the AMI names, will be suffixed with architecture and timestamp
  default = "python311-postgres13-runs-on"
}

variable "helper_script_folder" {
  type    = string
  default = "/imagegeneration/helpers"
}


data "amazon-ami" "runs-on-ami-x64" {
  filters = {
    name                = "runs-on-v2.2-ubuntu24-stepsecurity-x64-*"
    root-device-type    = "ebs"
    virtualization-type = "hvm"
  }

  most_recent = true
  # Runs-On owns the AMIs, so we use the owner ID to filter
  #https://runs-on.com/guides/building-custom-ami-with-packer/
  owners = ["135269210855"]  # RunsOn account
  region      = "${var.region}"
}


data "amazon-ami" "runs-on-ami-arm64" {
  filters = {
    name                = "runs-on-v2.2-ubuntu24-stepsecurity-arm64-*"
    root-device-type    = "ebs"
    virtualization-type = "hvm"
  }
  most_recent = true
  owners = ["135269210855"] # RunsOn account
  region      = "${var.region}"
}



source "amazon-ebs" "build-x64" {
  ami_name       = "x64-${var.image_name_prefix}-${formatdate("YYYY-MM-DD-hhmmss", timestamp())}"
  instance_type  = "m7i.xlarge"
  region         = "${var.region}"
  source_ami     = "${data.amazon-ami.runs-on-ami-x64.id}"
  ssh_username   = "ubuntu"
  subnet_id      = "${var.subnet_id}"
  user_data_file = "${path.root}/../scripts/build/user_data.sh"
}



source "amazon-ebs" "build-arm64" {
  ami_name       = "arm64-${var.image_name_prefix}-${formatdate("YYYY-MM-DD-hhmmss", timestamp())}"
  instance_type  = "m7g.xlarge"
  region         = "${var.region}"
  source_ami     = "${data.amazon-ami.runs-on-ami-arm64.id}"
  ssh_username   = "ubuntu"
  subnet_id      = "${var.subnet_id}"
  user_data_file = "${path.root}/../scripts/build/user_data.sh"
}

build {
  sources = ["source.amazon-ebs.build-x64", "source.amazon-ebs.build-arm64"]
  provisioner "file" {
    destination = "${var.helper_script_folder}"
    source      = "${path.root}/../scripts/helpers"
  }

  provisioner "shell" {
    environment_vars = ["HELPER_SCRIPTS=${var.helper_script_folder}","DEBIAN_FRONTEND=noninteractive"]
    script = "${path.root}/../scripts/build/provision.sh"
  }
}
