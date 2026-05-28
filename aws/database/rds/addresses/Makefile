.ONESHELL:

# Requires AWS CLI Profile matching base-${ENVIRONMENT} to be set up
# Requires AWS Session Manager Plugin to be installed:
# 	https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
# On Windows you will need to run these commands using Git Bash, NOT Powershell / CMD
# Force Make to use bash, even on Windows. 
SHELL := bash

# -- Configuration --
# Local port to connect to on your machine
LOCAL_PORT = 1433
# For Parameter store URL Paths
ENVIRONMENT = staging
# Set to AWSCLI Profile name
PROFILE = ${ENVIRONMENT}-apis

# -- Parameter Store paths --
INSTANCE_ID_PATH := " platform-apis-jump-box-instance-name"
DB_PORT_PATH := " /addresses-api/${ENVIRONMENT}/postgres-port"
DB_ENDPOINT_PATH := " /addresses-api/${ENVIRONMENT}/postgres-hostname"
DB_USERNAME_PATH := " /addresses-api/${ENVIRONMENT}/postgres-username"
DB_PASSWORD_PATH := " /addresses-api/${ENVIRONMENT}/postgres-password"

# -- Parameters --
_ := $(shell aws sts get-caller-identity --profile ${PROFILE} > /dev/null 2>&1 || aws sso login --profile ${PROFILE})
INSTANCE_ID := $(shell aws ssm get-parameter --name ${INSTANCE_ID_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_PORT := $(shell aws ssm get-parameter --name ${DB_PORT_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
DB_ENDPOINT := $(shell aws ssm get-parameter --name ${DB_ENDPOINT_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
DB_USERNAME := $(shell aws ssm get-parameter --name ${DB_USERNAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
DB_PASSWORD := $(shell aws ssm get-parameter --name ${DB_PASSWORD_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

# -- Commands --

port_forwarding_to_address_db:
	echo USERNAME: ${DB_USERNAME}
	echo PASSWORD: ${DB_PASSWORD}
	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region=eu-west-2  \
		--profile=${PROFILE} \
		--document AWS-StartPortForwardingSessionToRemoteHost \
		--parameters "host=${DB_ENDPOINT},portNumber=${REMOTE_PORT},localPortNumber=${LOCAL_PORT}"

reset:
	fuser -n tcp -k ${LOCAL_PORT} && echo "Port ${LOCAL_PORT} cleared" || echo "Port ${LOCAL_PORT} not in use"
