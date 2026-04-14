#!/bin/bash
# This script is run when EC2 creates the instance and starts the SSH service
# so that we can remotely provision it
sudo systemctl start ssh
