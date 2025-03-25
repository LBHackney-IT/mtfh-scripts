import os
import time
import requests
import chime

CIRCLE_TOKEN = os.environ["CIRCLE_TOKEN"]

# Define constants
VCS_TYPE = "github"
ORG_NAME = "LBHackney-IT"
REPO_NAME = "repairs-api-dotnet"
BRANCH_NAME = "develop"

# Base URL for CircleCI API v2
BASE_URL = "https://circleci.com/api/v2"

# Headers with authorization
HEADERS = {
    "Circle-Token": CIRCLE_TOKEN,
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def get_latest_workflow() -> dict:
    """Fetch the latest workflow for a given branch."""
    url = f"{BASE_URL}/project/{VCS_TYPE}/{ORG_NAME}/{REPO_NAME}/pipeline"

    params = {"branch": BRANCH_NAME}

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()

    pipelines = response.json().get("items", [])
    assert len(pipelines) > 0, f"No pipelines found for branch {BRANCH_NAME}"

    # Get the latest pipeline ID (the first item in the list is the latest)
    latest_pipeline_id = pipelines[0]["id"]

    # Fetch workflows for the latest pipeline
    workflows_url = f"{BASE_URL}/pipeline/{latest_pipeline_id}/workflow"
    workflows_response = requests.get(workflows_url, headers=HEADERS)
    response.raise_for_status()

    workflows = workflows_response.json().get("items", [])
    return workflows[0]


def cancel_workflow(workflow_id):
    """Cancel a specific workflow by its ID."""
    cancel_url = f"{BASE_URL}/workflow/{workflow_id}/cancel"
    response = requests.post(cancel_url, headers=HEADERS)
    print(f"Canceling workflow {workflow_id} - status code: {response.status_code}")


def rerun_workflow(workflow_id):
    """Re-run a specific workflow by its ID."""
    rerun_url = f"{BASE_URL}/workflow/{workflow_id}/rerun"
    cancel_workflow(workflow_id)
    response = requests.post(rerun_url, headers=HEADERS)
    response.raise_for_status()
    print(f"Re-running workflow {workflow_id}.")


def main():
    while True:
        # Step 1: Get the latest failing workflow
        latest_workflow = get_latest_workflow()

        is_latest_workflow_failing = latest_workflow["status"] == "failed"
        is_latest_workflow_running = latest_workflow["status"] == "running"

        if is_latest_workflow_failing:
            print(
                "Latest failing workflow found: "
                f"{latest_workflow['id']} ({latest_workflow['name']})"
            )
            rerun_workflow(latest_workflow["id"])
            chime.warning()
        elif is_latest_workflow_running:
            print(
                "Latest running workflow found: "
                f"{latest_workflow['id']} ({latest_workflow['name']})"
            )
        else:
            print("No workflows to re-run")
            chime.success()
            return

        time.sleep(60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        chime.error()
        raise e
