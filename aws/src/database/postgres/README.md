## Setup:

Install
the [AWS session manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)
to be able to run AWS cli commands to set up port forwarding.

See [Setup / AWS](../../../../README.md) in the base README for how to set up and use profiles.

## Usage:

Look at the [Makefile](Makefile) for scripts to run as needed.
These get parameters from Parameter Store for the profile configured so you need to be using the correct profile and
have these set on Parameter Store.

- `make port_forwarding_to_jumpbox_for_ca_rds` will set up local port forwarding via the jump box EC2 to the RDS
  database. This will allow
  scripted connections
  to the database.
  You could also use [pgAdmin](https://www.pgadmin.org/) to connect to the database with a GUI.
- `make connect_to_db_in_terminal` will connect to the EC2 jump-box instance in your local terminal.
  You'll need to enter the command written to your terminal to connect to the database, and then the password when
  prompted.

## Useful links:

- [How to Connect to an RDS Instance Using a Session Manager Proxy](https://docs.google.com/document/d/1FOBudIE2TTA5LfGdrURux39qRD10-XLv4L_zOYaoAL0)