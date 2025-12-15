# Base AMI Provisioning for Self-Hosted GitHub Runners

This directory contains scripts and HashiCorp Packer templates used to provision custom base Amazon Machine Images (AMIs) to be used with our self-hosted GitHub runners.

## Automated Builds

AMIs are automatically rebuilt **every Monday at 9am UTC** via the [Rebuild Runner AMIs workflow](../.github/workflows/rebuild-runner-amis.yml). This ensures our runner images stay up-to-date with the latest security patches and base image updates.

The workflow:
- Builds both x64 and arm64 AMIs using Packer
- Updates `.github/runs-on.yml` with the new AMI names
- Creates a pull request for review before deployment

You can also manually trigger a rebuild via the GitHub Actions UI if needed.

## Manual Builds

These images can also be built manually when you need to:
- Update base dependencies (e.g., migrate to Python 3.12)
- Upgrade to a new version of the base Runs-On images
- Test changes to provisioning scripts before committing

If you need access to the AWS account for manual builds, file an Access Change Request requesting access.

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

### Available Packer Variables

The Packer template supports the following variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `subnet_id` | Yes | - | AWS Subnet ID where Packer will launch build instances |
| `region` | No | `us-west-2` | AWS region for the build |
| `image_name_prefix` | No | `python311-postgres13-runs-on` | Prefix for AMI names (suffixed with architecture and timestamp) |

**Example with custom prefix:**
```shell script
packer build \
  -var "subnet_id=subnet-0b52b6902a6c54804" \
  -var "region=us-west-2" \
  -var "image_name_prefix=my-custom-runner" \
  templates/python311-postgres13.pkr.hcl
```

This will create AMIs named like:
- `x64-my-custom-runner-2025-12-15-090000`
- `arm64-my-custom-runner-2025-12-15-090000`

---

## AWS OIDC Configuration for Automated Workflow

The automated rebuild workflow uses GitHub's OIDC provider to authenticate with AWS, which is more secure than storing long-lived access keys. Here's how to configure it:

### 1. Create an IAM OIDC Identity Provider (one-time setup)

If you haven't already configured GitHub as an OIDC provider in your AWS account:

1. Go to AWS Console → IAM → Identity providers
2. Click "Add provider"
3. Provider type: OpenID Connect
4. Provider URL: `https://token.actions.githubusercontent.com`
5. Audience: `sts.amazonaws.com`
6. Click "Add provider"

### 2. Create an IAM Role for the Workflow

Create an IAM role that the GitHub Actions workflow can assume:

1. Go to AWS Console → IAM → Roles → Create role
2. Select "Web identity" as the trusted entity type
3. Identity provider: `token.actions.githubusercontent.com`
4. Audience: `sts.amazonaws.com`
5. GitHub organization: `Recidiviz`
6. GitHub repository: `pulse-data`
7. Click "Next"

### 3. Attach IAM Permissions

The role needs the following permissions to run Packer:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:AttachVolume",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:CopyImage",
        "ec2:CreateImage",
        "ec2:CreateKeypair",
        "ec2:CreateSecurityGroup",
        "ec2:CreateSnapshot",
        "ec2:CreateTags",
        "ec2:CreateVolume",
        "ec2:DeleteKeyPair",
        "ec2:DeleteSecurityGroup",
        "ec2:DeleteSnapshot",
        "ec2:DeleteVolume",
        "ec2:DeregisterImage",
        "ec2:DescribeImageAttribute",
        "ec2:DescribeImages",
        "ec2:DescribeInstances",
        "ec2:DescribeInstanceStatus",
        "ec2:DescribeRegions",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSnapshots",
        "ec2:DescribeSubnets",
        "ec2:DescribeTags",
        "ec2:DescribeVolumes",
        "ec2:DetachVolume",
        "ec2:GetPasswordData",
        "ec2:ModifyImageAttribute",
        "ec2:ModifyInstanceAttribute",
        "ec2:ModifySnapshotAttribute",
        "ec2:RegisterImage",
        "ec2:RunInstances",
        "ec2:StopInstances",
        "ec2:TerminateInstances"
      ],
      "Resource": "*"
    }
  ]
}
```

**Tip:** You can create a custom policy with these permissions or use the AWS managed policy `PowerUserAccess` (though that's broader than needed).

### 4. Update the Trust Policy

After creating the role, you may need to refine the trust policy to restrict it to the repository:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::475312562243:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:Recidiviz/pulse-data:*"
        }
      }
    }
  ]
}
```


### 5. Add the Secrets to GitHub

Add two repository secrets for the workflow:

**AWS Role ARN:**
1. Go to GitHub → Repository Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `AWS_RUNS_ON_PACKER_ROLE_ARN`
4. Value: The ARN of the IAM role you created (e.g., `arn:aws:iam::475312562243:role/GitHubActionsPackerRole`)
5. Click "Add secret"

**Subnet ID:**
1. Click "New repository secret" again
2. Name: `AWS_RUNS_ON_SUBNET_ID`
3. Value: The subnet ID where Packer should launch build instances (e.g., `subnet-0b52b6902a6c54804`)
4. Click "Add secret"

### 6. Test the Configuration

Manually trigger the workflow to verify everything is configured correctly:

1. Go to Actions → Rebuild Runner AMIs
2. Click "Run workflow"
3. Use the default values or specify custom parameters
4. Monitor the workflow run to ensure it can authenticate with AWS and build the AMIs

---

## Notes

- Ensure the `subnet_id` and `region` variables are appropriately set for your environment when building the AMI.
- The automated weekly builds use the default `image_name_prefix` value.
- AMI names include a timestamp to ensure uniqueness and enable tracking of build history.
- Use this custom AMI in your self-hosted GitHub runner setup to enhance performance and have a consistent runtime environment.

---

## Additional References

- [GitHub Self-Hosted Runners Documentation](https://docs.github.com/en/actions/hosting-your-own-runners)
- [Runs-On Custom AMI Documentation](https://runs-on.com/guides/building-custom-ami-with-packer/)
- [HashiCorp Packer Documentation](https://developer.hashicorp.com/packer)
