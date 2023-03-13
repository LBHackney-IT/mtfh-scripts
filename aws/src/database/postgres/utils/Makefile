.ONESHELL:

# NOTE:
# 	Check the README file in this directory for usage/details

# -- Configuration --

# Set the AWS profile to use for these connections - should match a profile in your .aws/config file
PROFILE=base-staging

# Set the local port to use for the port forwarding - connect to this port on your local machine to connect to the RDS
LOCAL_PORT = 5432

# Set the parameter paths for the profile used - as in parameter store, e.g. /service/stage/parameter-name
JUMP_BOX_INSTANCE_NAME_PATH:="platform-apis-jump-box-instance-name"
POSTGRES_HOST_PATH:="/uh-api/staging/postgres-hostname"
POSTGRES_PORT_PATH:="/uh-api/staging/postgres-port"
POSTGRES_USERNAME_PATH:="/uh-api/staging/postgres-username"
POSTGRES_PASSWORD_PATH:="/uh-api/staging/postgres-password"


# -- Parameters --

# Get the EC2 jump-box instance ID for the RDS from parameter store for the profile used
INSTANCE_ID := $(shell aws ssm get-parameter --name ${JUMP_BOX_INSTANCE_NAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

# Get the host URL for the RDS from parameter store for the profile used
REMOTE_HOST := $(shell aws ssm get-parameter --name ${POSTGRES_HOST_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

# Get the port for the RDS from parameter store for the profile used
REMOTE_PORT := $(shell aws ssm get-parameter --name ${POSTGRES_PORT_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

# Get the username for the RDS from parameter store for the profile used
REMOTE_USERNAME := $(shell aws ssm get-parameter --name ${POSTGRES_USERNAME_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)

# Get the password for the RDS from parameter store for the profile used
REMOTE_PASSWORD := $(shell aws ssm get-parameter --with-decryption --name ${POSTGRES_PASSWORD_PATH} --region "eu-west-2" --profile ${PROFILE} --query Parameter.Value --output text)


# -- Commands --

port_forwarding_to_db:
	# Use this command to connect to the RDS using SSM port forwarding
	# This will allow connecting to the database using a GUI tool like pgAdmin, or with local scripts

	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region eu-west-2 \
		--profile ${PROFILE} \
		--document-name AWS-StartPortForwardingSession \
		--parameters '{"host":["${REMOTE_HOST}"],"portNumber":["${REMOTE_PORT}"], "localPortNumber":["${LOCAL_PORT}"]}'


connect_to_db_in_terminal:
	# Use these commands to connect to the EC2 jump-box instance and then the RDS using SSM
	# This will allow running sql commands on the database in your local terminal

	printf "'--- v COMMAND v ---\
			\n psql --host=${REMOTE_HOST} --port=${REMOTE_PORT} --username=${REMOTE_USERNAME} --password --dbname=uh_mirror\
			\n PASSWORD: ${REMOTE_PASSWORD} \
			\n--- ^ COMMAND ^ --- \n"

	aws ssm start-session \
		--target ${INSTANCE_ID} \
		--region eu-west-2 \
		--profile ${PROFILE};

	# Once connected to the EC2 jump-box instance, this command will connect to the Postgres Database on the RDS instance
	# You will be prompted for the password