# mtfh-scripts

Scripts for use within Modern Tools For Housing

Provides utils to facilitate connecting to and scripting against:
- DynamoDB
- RDS (with ORM bindings)
- Elasticsearch

# Requirements & Setup

## Without Devcontainers

1. Ensure you have a recent version of [Python 3](https://www.python.org/downloads/) (e.g. Python 3.11)
2. Ensure you have Pip package manager for Python 3. Try `python3 -m pip --version` to see if you have it installed. If not,
   try `python3 -m ensurepip` to install it.
3. Make a venv (local package directory) with `python3 -m venv venv`
4. Activate the venv
   - Linux / MacOS: Run `source venv/bin/activate`
   - Windows: Run `./venv/Scripts/activate.bat` or `./venv/Scripts/activate.ps1` depending on what's available
5. Optionally verify the venv is active:
   - Linux / MacOS: Run `echo $VIRTUAL_ENV` and check it points to the venv
   - Windows: Run `echo %VIRTUAL_ENV%` and check it points to the venv
6. Run `python3 -m pip install -r requirements.txt` in the root directory of the repository to install all requirements
   into the venv.

## Using Devcontainers

This will simplify setup and install various useful tools.

### Note for Windows
You must clone and use this repository in WSL for acceptable performance. You must mount your AWS directory from your WSL filesystem, not your Windows filesystem. If you experience issues, consider setting up without devcontainers. This is just a limitation of Docker on Windows with no clear fix.

### Steps
1. Ensure you have Docker Desktop or Docker Engine installed with the Docker daemon active
2. Ensure you have the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension in VSCode or equivalent in your favourite IDE
3. Select the prompt to "Reopen in Container" or equivalent when it appears or access it from the command menu (Ctrl+Shift+P)

Note: First setup may take several minutes to build the Docker container, but subsequent builds will only be a few seconds.
The common rules of Docker apply where it is built in layers from .devcontainer/Dockerfile and caches layers where possible.

## Setup / AWS

Note: Your ~/.aws directory will be mounted into the Docker container if you're using a devcontainer, so you can set up your AWS credentials on your host machine and access them in the container.

Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) -
the scripts use AWS CLI profile based authentication to make calls to AWS.

You'll need a recent version of the AWS CLI to make connections to databases via port forwarding as this is a new feature.

You can follow these steps multiple times to set up different profiles for different AWS accounts:

1. Go to the Google SSO AWS start page and select "Command line or programmatic access" for the account needed.
2. Note the steps under **AWS IAM Identity Center credentials (Recommended)** and use those details below.
3. Run `aws configure sso` and follow the prompts to set up your AWS credentials.
   During this process, pay attention to setting the correct values for `sso_region` and `cli default client region` as these can be different.
   For the `cli profile name` value, refer to the Stage enum in `enums/enums.py` as it must exactly match one of these.
   Add to the enum as needed for different profiles.
4. To refresh your credentials when they expire, run `aws sso login --profile {profile_name}`

You will need to refresh your credentials as in step 4 when commands fail, usually with a message
like `Error when retrieving token from sso: Token has expired and refresh failed`

## Running scripts

You should not need further dependencies to run any scripts.

Open a script and hit the play button in VSCode or your IDE to debug it - on VSCode this will use the debug configuration in the .vscode/launch.json file.

Python convention is to create a `main` function in a file and call it inside an `if __name__ == "__main__":` block at the end of the file.
