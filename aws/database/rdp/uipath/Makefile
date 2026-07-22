.ONESHELL:

# Requires AWS CLI Profile matching housing-${ENVIRONMENT} to be set up
# Requires AWS Session Manager Plugin to be installed:
# 	https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
# Requires a Windows-compatible RDP client installed on your local machine (e.g. Microsoft Remote Desktop)
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
# Set to AWSCLI Profile name that can access the RDP server
PROFILE = uipath-${ENVIRONMENT}

# Profile with access to secrets manager to fetch the RDP credentials for the server
# Ensure you have set up this cli profile and have made a commonfate request for it
PROFILE_SECRETSMANAGER = uipath-secretsmanager-${ENVIRONMENT}

# Windows instance ID running the RDP server (dynamically fetched by Name tag)
INSTANCE_ID = $(shell aws ec2 describe-instances --filters "Name=tag:Name,Values=uipath-developer-prod" --region ${REGION} --profile ${PROFILE} --query "Reservations[0].Instances[0].InstanceId" --output text)
# RDP port on remote instance
REMOTE_PORT = 3389

# -- Parameters --
_ := $(shell aws sts get-caller-identity --profile ${PROFILE} > /dev/null 2>&1 || aws sso login --profile ${PROFILE})

SECRETS_MANAGER_PROFILE_READY := $(shell \
	aws sts get-caller-identity --profile $(PROFILE_SECRETSMANAGER) >/dev/null 2>&1 || \
	aws sso login --profile $(PROFILE_SECRETSMANAGER) >/dev/null 2>&1; \
	if aws sts get-caller-identity --profile $(PROFILE_SECRETSMANAGER) >/dev/null 2>&1; then echo ready; else echo failed; fi \
)

CREDS_JSON := $(if $(filter ready,$(SECRETS_MANAGER_PROFILE_READY)),\
	$(shell aws secretsmanager get-secret-value --secret-id uipath/rdp-server-credentials --region $(REGION) --profile $(PROFILE_SECRETSMANAGER) --query SecretString --output text 2>/dev/null),\
	failed)

USERNAME := $(if $(filter-out failed,$(CREDS_JSON)),$(shell echo '$(CREDS_JSON)' | sed -E 's/.*"username"\s*:\s*"([^"]+)".*/\1/'),failed)
PASSWORD := $(if $(filter-out failed,$(CREDS_JSON)),$(shell echo '$(CREDS_JSON)' | sed -E 's/.*"password"\s*:\s*"([^"]+)".*/\1/'),failed)

# -- Commands --

# Use this command to create an RDP tunnel from localhost to the Windows instance running the RDP server.
rdp:
	@echo ""
	@echo "Starting RDP tunnel at localhost:${LOCAL_PORT} | 127.0.0.1:${LOCAL_PORT}..."
	@echo "Instance: ${INSTANCE_ID}"
	@echo "Local Port: ${LOCAL_PORT}"
	@echo "Remote Port: ${REMOTE_PORT}"
	@echo "=== Credentials for RDP ==="
	@echo "Secrets Manager AWS Profile: ${PROFILE_SECRETSMANAGER} | Working: ${SECRETS_MANAGER_PROFILE_READY}"
	@echo "Username: ${USERNAME}"
	@echo "Password: ${PASSWORD}"
	@echo ""
	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region ${REGION} \
		--profile ${PROFILE} \
		--document-name AWS-StartPortForwardingSession \
		--parameters "localPortNumber=${LOCAL_PORT},portNumber=${REMOTE_PORT}"