# Base AMI Provisioning for Self-Hosted GitHub Runners

This directory contains scripts and HashiCorp Packer templates used to provision custom base Amazon Machine Images (AMIs) to be used with our self-hosted GitHub runners.

These images generally only need to be updated when we want to update the base dependencies of our runners (for example migrate to Python 3.12), or upgrade to the latest version of the base Runs-On images.

If you need access to the AWS account, file an Access Change Request requesting access.

The available AMIs are preconfigured with commonly used software:

| AMI Templates              | Platforms | Description                                                                        |
|----------------------------|-----------|------------------------------------------------------------------------------------|
| `python311-postgres13` | arm64, x64 | Python 3.11 and PostgreSQL 13 installed, optimized for self-hosted GitHub runners. |

---

## Overview

This setup enables organizations to create scalable and reusable self-hosted GitHub runners by provisioning custom AMIs with the required tools and configurations. The AMIs can then be deployed on AWS EC2 instances.

## Setup
Install Hashicorp Packer:
```bash
brew tap hashicorp/tap
brew install hashicorp/tap/packer
```

Build an image:
```bash
# From runner_images/python311-postgres13 directory, run:
packer init templates/python311-postgres13.pkr.hcl
# To build the image, ensure the AWS CLI is configured with credentials from AdministratorAccess, run:
# SUBNET_ID may be any subnet from our AWS account
packer build -var "subnet_id=${SUBNET_ID}" templates/python311-postgres13.pkr.hcl
```
### Key Components

- **`scripts/build/provision.sh`**: A provisioning script that installs dependencies, such as Python (via `pyenv`) and PostgreSQL 13, to prepare the base image.
  
- **`templates/python311-postgres13.pkr.hcl`**: A Packer configuration file that defines the steps necessary to build the custom AMI, including loading user data and running provisioning scripts.

---

## File Details

### 1. `scripts/build/provision.sh`
This bash script performs the following tasks:
- Configures `pyenv` for seamless Python version management with the runner.
- Installs and sets Python 3.11 as the default runtime environment.
- Installs PostgreSQL 13.
- Copies files installed by PostgreSQL into a dedicated directory for easy inclusion in the AMI.

### 2. `scripts/build/user_data.sh`
This script is used during the boot process of the EC2 instance to perform minimal setup, such as starting the SSH service, allowing Packer to connect to the instance for provisioning.

### 3. `python311-postgres13.pkr.hcl`
This Packer configuration file automates the creation of the custom AMI:
- Uses the HashiCorp Amazon plugin and references an official AMI as the base image.
- Configures AWS-specific settings such as region, subnet ID, and source AMI.
- Executes the `provision.sh` script to install dependencies and configure the environment.
- Captures the finalized AMI that is ready for use with self-hosted GitHub runners.

---

## Prerequisites

### Tools Required:
- **Packer**: To build the AMI. [Install Packer](https://developer.hashicorp.com/packer/downloads).
- **AWS CLI**: To authenticate with AWS and manage resources. [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

---

## Usage Instructions

1. **Install Packer and AWS CLI**:
   - Ensure you have Packer installed and authenticated AWS CLI credentials with `AdministratorAccess`.

2. **Set up the Packer environment**:
   - Initialize Packer by running:
```shell script
packer init templates/python311-postgres13.pkr.hcl
```

3. **Build the AMI**:
   - Run the following command to build the AMI, replacing the variables with your custom values:
```shell script
packer build -var "subnet_id=your-subnet-id" -var "region=us-west-2" templates/python311-postgres13.pkr.hcl
```

---

## Notes

- Ensure the `subnet_id` and `region` variables are appropriately set for your environment when building the AMI.
- Use this custom AMI in your self-hosted GitHub runner setup to enhance performance and have a consistent runtime environment.
  
--- 

## Additional References

- [GitHub Self-Hosted Runners Documentation](https://docs.github.com/en/actions/hosting-your-own-runners)
- [Runs-On Custom AMI Documentation](https://runs-on.com/guides/building-custom-ami-with-packer/)
- [HashiCorp Packer Documentation](https://developer.hashicorp.com/packer)
