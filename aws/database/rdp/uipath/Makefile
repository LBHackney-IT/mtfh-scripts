.ONESHELL:

# Requires AWS CLI Profile matching housing-${ENVIRONMENT} to be set up
# Requires AWS Session Manager Plugin to be installed:
# 	https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
# On Windows you will need to run these commands using Git Bash, NOT Powershell / CMD

# Force Make to use bash, even on Windows.
SHELL := bash

# -- Configuration --
# AWS region for the Session Manager commands
REGION = eu-west-2
# Local port for the RDP tunnel on your machine
LOCAL_PORT = 55678
# AWS environment used to build the profile name
ENVIRONMENT = preproduction
# Set to AWSCLI Profile name
PROFILE = uipath-${ENVIRONMENT}
# Windows jump box instance ID (dynamically fetched by Name tag)
INSTANCE_ID = $(shell aws ec2 describe-instances --filters "Name=tag:Name,Values=uipath-developer-prod" --region ${REGION} --profile ${PROFILE} --query "Reservations[0].Instances[0].InstanceId" --output text)
# RDP port on remote instance
REMOTE_PORT = 3389

# -- Parameters --
_ := $(shell aws sts get-caller-identity --profile ${PROFILE} > /dev/null 2>&1 || aws sso login --profile ${PROFILE})

# -- Commands --

# Use this command to create an RDP tunnel from localhost to the Windows jump box
rdp:
	@echo ""
	@echo "Starting RDP tunnel..."
	@echo "Instance: ${INSTANCE_ID}"
	@echo "Local Port: ${LOCAL_PORT}"
	@echo "Remote Port: ${REMOTE_PORT}"
	@echo ""
	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region ${REGION} \
		--profile ${PROFILE} \
		--document-name AWS-StartPortForwardingSession \
		--parameters "localPortNumber=${LOCAL_PORT},portNumber=${REMOTE_PORT}"