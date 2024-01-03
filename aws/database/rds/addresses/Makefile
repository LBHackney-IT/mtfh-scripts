.ONESHELL:

# Requires AWS CLI Profile matching base-${ENVIRONMENT} to be set up
# Requires AWS Session Manager Plugin to be installed:
# 	https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
# On Windows you will need to run these commands using Git Bash, NOT Powershell / CMD

# -- Configuration --
# Local port to connect to on your machine
LOCAL_PORT = 1433
# For Parameter store URL Paths
ENVIRONMENT = staging
# Set to AWSCLI Profile name
PROFILE = base-${ENVIRONMENT}

# -- Parameter Store paths --
INSTANCE_ID_PATH := " platform-apis-jump-box-instance-name"
DB_PORT_PATH := " /addresses-api/${ENVIRONMENT}/postgres-port"
DB_ENDPOINT_PATH := " /addresses-api/${ENVIRONMENT}/postgres-hostname"
DB_USERNAME_PATH := " /addresses-api/${ENVIRONMENT}/postgres-username"
DB_PASSWORD_PATH := " /addresses-api/${ENVIRONMENT}/postgres-password"

# -- Parameters --
INSTANCE_ID := $(shell aws ssm get-parameter --name ${INSTANCE_ID_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_PORT := $(shell aws ssm get-parameter --name ${DB_PORT_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
DB_ENDPOINT := $(shell aws ssm get-parameter --name ${DB_ENDPOINT_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
DB_USERNAME := $(shell aws ssm get-parameter --name ${DB_USERNAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
DB_PASSWORD := $(shell aws ssm get-parameter --name ${DB_PASSWORD_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

DATABASE_PARAMS := '{"host":["${DB_ENDPOINT}"], "portNumber":["${REMOTE_PORT}"], "localPortNumber":["${LOCAL_PORT}"]}'

# -- Commands --

# Use this command to login to the AWS SSO service via your browser
# This is required to use the ssm commands
sso_login:
	if (aws sts get-caller-identity --profile ${PROFILE})
	then
		echo "Session still valid"
	else
		echo "Session expired, logging in"
		aws sso login --profile ${PROFILE}
	fi

port_forwarding_to_address_db:
	echo "Connecting to ${DB_ENDPOINT} on local port ${LOCAL_PORT} Username: ${DB_USERNAME} Password: ${DB_PASSWORD}"
	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region=eu-west-2  \
		--profile=${PROFILE} \
		--document AWS-StartPortForwardingSessionToRemoteHost \
		--parameters ${DATABASE_PARAMS}

reset:
	fuser -n tcp -k ${LOCAL_PORT} && echo "Port ${LOCAL_PORT} cleared" || echo "Port ${LOCAL_PORT} not in use"
