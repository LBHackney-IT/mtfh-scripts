# Working With Databases

## Base Requirements
- AWS CLI profile with access to the database & database parameters
- [Session Manager Plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)

## Port forwarding to the EC2 jumpbox instance
- Run the Makefile in the database's directory to start a port forwarding session to the database in the correct stage (dev/staging/prod)
- There are two commands in the Makefiles:
  - Sso login with the selected profile: `make sso_login`
  - Port forwarding to the EC2 jumpbox / bastion instance from which database connection is possible. View the file for the specific command
- After port forwarding, the EC2 instance is available on localhost at the local port specified in the Makefile, and a connection command can be sent to it
- You will need to keep running this port forwarding session while you are using the database
- Note that the connection times out after some time without activity
## Using a graphical client (DBeaver, PgAdmin, SSMS, etc)
- Note the username, password, and port written to the terminal after running the Makefile
- Connect directly to localhost as the hostname and with the local port specified in the Makefile
- Use the username and password written to the terminal to authenticate

## Using a  Python script
View the `connect_to_{db_name}` files in the database directories for examples of how to connect to and use the database in Python

The scripts use SQLAlchemy as an ORM and psycopg2 as a driver for Postgres and pyodbc as a driver for Sql Server

### SQL Server Driver Setup
- You will need the ODBC Driver for Sql Server Version 17 installed. Version tested: **17.10.4.1**
  - [Windows](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#version-17)
  - [MacOS/Linux](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- **On MacOS/Linux** you will need to install `unixodbc` with your package manager or otherwise
