LOCAL_PORT = 5432
STAGE := staging
PROFILE := "${STAGE}-apis"


# -- Parameter Store paths --
JUMP_BOX_INSTANCE_NAME_PATH:="platform-apis-jump-box-instance-name"
POSTGRES_HOST_PATH:="/api-auth-token-generator/${STAGE}/postgres-hostname"
POSTGRES_PORT_PATH:="/api-auth-token-generator/${STAGE}/postgres-port"
POSTGRES_USERNAME_PATH:="/api-auth-token-generator/${STAGE}/postgres-username"
POSTGRES_PASSWORD_PATH:="/api-auth-token-generator/${STAGE}/postgres-password"

# -- Parameters --
# Get parameters from parameter store for the profile used
_ := $(shell aws sts get-caller-identity --profile ${PROFILE} || aws sso login --profile ${PROFILE})
INSTANCE_ID := $(shell aws ssm get-parameter --name ${JUMP_BOX_INSTANCE_NAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_HOST := $(shell aws ssm get-parameter --name ${POSTGRES_HOST_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_PORT := $(shell aws ssm get-parameter --name ${POSTGRES_PORT_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_USERNAME := $(shell aws ssm get-parameter --name ${POSTGRES_USERNAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)
REMOTE_PASSWORD := $(shell aws ssm get-parameter --with-decryption --name ${POSTGRES_PASSWORD_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

DATABASE_PARAMS = '{"host":["${REMOTE_HOST}"], "portNumber":["${REMOTE_PORT}"], "localPortNumber":["${LOCAL_PORT}"]}'

# -- Commands --

# Use this command to connect to create a port forwarding session from localhost to the RDS instance via the jump-box
# This will allow connecting to the database using a GUI tool like pgAdmin, or with local scripts
# NOTE: On Windows you may need to run this command in a Git Bash terminal
port_forwarding:
	echo "Connecting to ${DB_ENDPOINT} on local port ${LOCAL_PORT}\n===\nUsername: ${REMOTE_USERNAME}\nPassword: ${REMOTE_PASSWORD}\n==="
	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region=eu-west-2  \
		--profile=${PROFILE} \
		--document AWS-StartPortForwardingSessionToRemoteHost \
		--parameters ${DATABASE_PARAMS}
