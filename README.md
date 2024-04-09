# mtfh-scripts

Scripts for use within Modern Tools For Housing

## Requirements

- Docker Desktop or Docker Engine installed with the Docker daemon active
- [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension in VSCode or equivalent in your favourite IDE

## Setup / Installation

1. Clone this repository
2. Look at the .devcontainer/devcontainer.json file and select the option in the "mounts" section based on if you are on Windows or Mac/Linux/WSL
3. Select the prompt to "Reopen in Container" or equivalent when it appears

Note: First setup may take several minutes to build the Docker container, but subsequent builds will only be a few seconds.
The common rules of Docker apply where it is built in layers from .devcontainer/Dockerfile and caches layers where possible.

## Setup / AWS

Note: Your ~/.aws directory will be mounted into the Docker container, so you can set up your AWS credentials on your host machine and access them in the container.

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

Open a script and hit the play button in VSCode or your IDE to run or debug it.
