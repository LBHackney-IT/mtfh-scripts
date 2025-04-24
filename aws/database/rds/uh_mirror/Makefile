.ONESHELL:
# Requires AWS CLI Profile matching housing-${ENVIRONMENT} to be set up
# Requires AWS Session Manager Plugin to be installed:
# 	https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
# On Windows you will need to run these commands using Git Bash, NOT Powershell / CMD


# -- Configuration --
# Set the local port to use for the port forwarding - connect to this port on your local machine to connect to the RDS
LOCAL_PORT = 5432
# For Parameter store URL Paths
STAGE := production
# Set to AWSCLI Profile name
PROFILE := "${STAGE}-apis"

# -- Parameter Store paths --
JUMP_BOX_INSTANCE_NAME_PATH:="platform-apis-jump-box-instance-name"
POSTGRES_HOST_PATH:=" /uh-api/${STAGE}/postgres-hostname"
POSTGRES_PORT_PATH:=" /uh-api/${STAGE}/postgres-port"
POSTGRES_USERNAME_PATH:=" /uh-api/${STAGE}/postgres-username"
POSTGRES_PASSWORD_PATH:=" /uh-api/${STAGE}/postgres-password"

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
# NOTE: On Windows you may need to run this command in a Git Bash terminal - run the `make port_forwarding_to_uh_db` and copy it from there
port_forwarding_to_uh_db:
	echo USERNAME: ${REMOTE_USERNAME}
	echo PASSWORD: ${REMOTE_PASSWORD}

	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region eu-west-2 \
		--profile ${PROFILE} \
		--document-name AWS-StartPortForwardingSessionToRemoteHost \
		--parameters ${DATABASE_PARAMS};

# Use these commands to connect to the EC2 jump-box instance and then the RDS using SSM
# This will allow running sql commands on the database in your local terminal
connect_to_db_in_terminal:
	printf "'--- v COMMAND v ---\
			\n psql --host=${REMOTE_HOST} --port=${REMOTE_PORT} --username=${REMOTE_USERNAME} --password --dbname=uh_mirror\
			\n PASSWORD: ${REMOTE_PASSWORD} \
			\n--- ^ COMMAND ^ --- \n"

	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region eu-west-2 \
		--profile ${PROFILE};

	# Once connected to the EC2 jump-box instance, this command will connect to the Postgres Database on the RDS instance
	# You will be prompted for the password, see above

reset:
	fuser -n tcp -k ${LOCAL_PORT} && echo "Port ${LOCAL_PORT} cleared" || echo "Port ${LOCAL_PORT} not in use"
