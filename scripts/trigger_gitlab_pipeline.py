#!/usr/bin/env python3
import os, sys, requests, urllib3

GITLAB_URL = os.environ["GITLAB_URL"].rstrip("/")
GITLAB_PROJECT_ID = os.environ["GITLAB_PROJECT_ID"]
GITLAB_ACCESS_TOKEN = os.environ["GITLAB_ACCESS_TOKEN"]
GITLAB_REF = os.environ.get("GITLAB_REF", "main")
GITLAB_JOB_NAME = os.environ.get("GITLAB_JOB_NAME", "ansible-prod")
SSL_VERIFY = os.environ.get("GITLAB_SSL_VERIFY", "false").lower() != "false"

if not SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"PRIVATE-TOKEN": GITLAB_ACCESS_TOKEN}


def get_latest_pipeline():
    resp = requests.get(
        f"{GITLAB_URL}/api/v4/projects/{GITLAB_PROJECT_ID}/pipelines",
        headers=HEADERS,
        params={"ref": GITLAB_REF, "order_by": "id", "sort": "desc", "per_page": 1},
        timeout=30,
        verify=SSL_VERIFY,
    )
    resp.raise_for_status()
    pipelines = resp.json()
    if not pipelines:
        print(f"ERROR: no pipelines found for ref '{GITLAB_REF}'", file=sys.stderr)
        sys.exit(1)
    p = pipelines[0]
    print(f"Found pipeline #{p['id']} (status: {p['status']}): {p.get('web_url', '')}")
    return p["id"]


def find_and_play_job(pipeline_id):
    resp = requests.get(
        f"{GITLAB_URL}/api/v4/projects/{GITLAB_PROJECT_ID}/pipelines/{pipeline_id}/jobs",
        headers=HEADERS,
        timeout=30,
        verify=SSL_VERIFY,
    )
    resp.raise_for_status()

    for job in resp.json():
        if job["name"] != GITLAB_JOB_NAME:
            continue
        status = job["status"]
        if status != "manual":
            print(f"Job '{GITLAB_JOB_NAME}' is '{status}' — remediation may already be running or completed.")
            return
        play = requests.post(
            f"{GITLAB_URL}/api/v4/projects/{GITLAB_PROJECT_ID}/jobs/{job['id']}/play",
            headers=HEADERS,
            timeout=30,
            verify=SSL_VERIFY,
        )
        play.raise_for_status()
        print(f"Job '{GITLAB_JOB_NAME}' started (job id: {job['id']})")
        return

    print(f"ERROR: job '{GITLAB_JOB_NAME}' not found in pipeline #{pipeline_id}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    pipeline_id = get_latest_pipeline()
    find_and_play_job(pipeline_id)
