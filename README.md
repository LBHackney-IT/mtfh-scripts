# mtfh-scripts
Scripts for use within Modern Tools For Housing

### NOTE: 
These Python commands may be different depending on your OS and how you installed Python. 

Try `python` instead of `python3` if you get "command not found".

## Requirements
1. A recent version of [Python 3](https://www.python.org/downloads/) (e.g. Python 3.11)
2. The Pip package manager for Python 3. Try `python3 -m pip --version` to see if you have it installed. If not, try `python3 -m ensurepip` to install it.

## Setup / Installation
1. Clone this repository
2. Make a venv (local package directory) with `python3 -m venv venv`
3. Activate the venv
   - Linux / MacOS: Run `source venv/bin/activate`
   - Windows: Run `./venv/bin/activate.bat` or `./venv/bin/activate.ps1` depending on what's available
3. Optionally verify the venv is active:
   - Linux / MacOS: Run `which python3` and check it points to the venv
   - Windows: Run `where.exe python3` and check it points to the venv
4. Run `python3 -m pip install -r requirements.txt` in the root directory of the repository to install all requirements
into the venv.

## Setup / AWS
Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) - 
you need to set your AWS credentials to be read by the AWS client in your `.aws/credentials` folder.

You can follow these steps multiple times to set up different profiles for different AWS accounts.

1. Go to the Google SSO AWS start page and select "Command line or programmatic access" for the account needed. 
2. Note the steps under **AWS IAM Identity Center credentials (Recommended)** and use those details below. 
3. Run `aws configure sso` and follow the prompts to set up your AWS credentials.
Refer to the Stage enum in `aws/src/enums/enums.py` as it must exactly match one of these.
Add to the enum as needed for different profiles.
4. To refresh your credentials when they expire, run `aws sso login --profile {profile_name}`

You will need to refresh your credentials as above when commands fail, usually with a message like `Error when retrieving token from sso: Token has expired and refresh failed`

## Running scripts
You should not need further dependencies to run any scripts.
1. Open the script file in your code editor
2. Edit the Config object at the top of the file to match your needs.
3. Open the directory of the script you want to run in your terminal and run `python {script_name}.py`

## Running tests
Tests are in Pytest which is installed as a dependency
1. Open the directory of the test files to run
2. Run `pytest {test_file_name}.py` to run tests in a file or just `pytest` to run all tests
