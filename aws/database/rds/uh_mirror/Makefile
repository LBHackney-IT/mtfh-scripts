.ONESHELL:
# Requires AWS CLI Profile matching housing-${ENVIRONMENT} to be set up
# Requires AWS Session Manager Plugin to be installed:
# 	https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
# On Windows you will need to run these commands using Git Bash, NOT Powershell / CMD

# Force Make to use bash, even on Windows. 
SHELL := bash

# -- Configuration --
# Set the local port to use for the port forwarding - connect to this port on your local machine to connect to the RDS
LOCAL_PORT = 5432
# For Parameter store URL Paths
STAGE := staging
# Set to AWSCLI Profile name
PROFILE := ${STAGE}-apis

# -- Parameter Store paths --
JUMP_BOX_INSTANCE_NAME_PATH:="platform-apis-jump-box-instance-name"
POSTGRES_HOST_PATH:=" /uh-api/${STAGE}/postgres-hostname"
POSTGRES_PORT_PATH:=" /uh-api/${STAGE}/postgres-port"
POSTGRES_USERNAME_PATH:=" /uh-api/${STAGE}/postgres-username"
POSTGRES_PASSWORD_PATH:=" /uh-api/${STAGE}/postgres-password"

# -- Parameters --
# Get parameters from parameter store for the profile used
_ := $(shell aws sts get-caller-identity --profile ${PROFILE} > /dev/null 2>&1 || aws sso login --profile ${PROFILE})
INSTANCE_ID := $(shell aws ssm get-parameter --name ${JUMP_BOX_INSTANCE_NAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_HOST := $(shell aws ssm get-parameter --name ${POSTGRES_HOST_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_PORT := $(shell aws ssm get-parameter --name ${POSTGRES_PORT_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_USERNAME := $(shell aws ssm get-parameter --name ${POSTGRES_USERNAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_PASSWORD := $(shell aws ssm get-parameter --with-decryption --name ${POSTGRES_PASSWORD_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

# -- Commands --

# Use this command to connect to create a port forwarding session from localhost to the RDS instance via the jump-box
# This will allow connecting to the database using a GUI tool like pgAdmin, or with local scripts
# NOTE: On Windows you may need to run this command in a Git Bash terminal - run the `make port_forwarding_to_uh_db` and copy it from there
port_forwarding_to_uh_db:
	echo USERNAME: ${REMOTE_USERNAME}
	echo PASSWORD: ${REMOTE_PASSWORD}

	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region eu-west-2 \
		--profile ${PROFILE} \
		--document-name AWS-StartPortForwardingSessionToRemoteHost \
		--parameters "host=${REMOTE_HOST},portNumber=${REMOTE_PORT},localPortNumber=${LOCAL_PORT}"
