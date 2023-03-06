# mtfh-scripts
Scripts for use within Modern Tools For Housing

## Requirements
1. A recent version of [Python 3](https://www.python.org/downloads/) (e.g. Python 3.11)
2. The Pip package manager for Python 3. Try `python3 -m pip --version` to see if you have it installed. If not, try `python3 -m ensurepip` to install it.

### NOTE: 
These Python commands may be different depending on your OS and how you installed Python. 

Try `python` instead of `python3` if you get an error.

## Setup
1. Clone this repository
2. Make a venv (local package directory) with `python3 -m venv venv`
3. Run `source venv/bin/activate` on Linux/MacOS or `./env/bin/activate.bat` on Windows (or might be `./env/bin/activate.ps1`)
4. Run `python3 -m pip install -r requirements.txt` in the root directory of the repository

## Setup / AWS
Installing the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) is recommended - 
you need to set your AWS credentials to be read by the AWS client in your `.aws/credentials` folder.
1. Run `aws configure --profile {stage}` where `{stage}` is the stage you want to use 
(e.g. `development` or `staging` or `production`) - get these credentials from the AWS console for the account that you want to use. 
The name must exactly match one of the stages in the Stage enum in `aws/src/enums/enums.py`
2. Open your .aws/credentials file and add your `aws_session_token` to the profile you just created (e.g. `aws_session_token = <token>`)

3. Your credentials file should look something like this:
```
[development]
aws_access_key_id = <access_key>
aws_secret_access_key = <secret_key>
aws_session_token = <token>

[staging]
aws_access_key_id = <access_key>
aws_secret_access_key = <secret_key>
aws_session_token = <token>

[production]
aws_access_key_id = <access_key>
aws_secret_access_key = <secret_key>
aws_session_token = <token>
```
You will need to refresh these manually when they expire.

## Running scripts
You should not need further dependencies to run any scripts.
1. Open the script file in your code editor
2. Edit the Config object at the top of the file to match your needs.
3. Open the directory of the script you want to run in your terminal and run `python {script_name}.py`

## Running tests
Tests are in Pytest which is installed as a dependency
1. Open the directory of the test files to run
2. Run `pytest {test_file_name}.py` or just `pytest` to run all tests
