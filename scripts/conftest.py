# scripts/conftest.py
# Pytest plugin hooks for STAC functional tests.
# Accumulates per-test results and pushes eodc_e2e_functional_* metrics
# to the OTEL collector at session end.

import os
import sys
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from otel_push import record, flush

SERVICE    = "stac"
ENV        = os.environ.get("E2E_ENV", "dev")
COLLECTION = os.environ.get("STAC_FUNCTIONAL_COLLECTION") or "auto"

_results: list = []


def pytest_runtest_logreport(report):
    if report.when != "call":
        return
    _results.append({
        "test":     report.nodeid.split("::")[-1],
        "success":  report.passed,
        "duration": getattr(report, "duration", 0.0),
    })


def pytest_sessionfinish(session, exitstatus):
    now = time.time()
    for r in _results:
        record(
            {"eodc_e2e_functional_success":             1 if r["success"] else 0,
             "eodc_e2e_functional_duration_seconds":    float(r["duration"]),
             "eodc_e2e_functional_last_run_timestamp":  now},
            {"env": ENV, "service": SERVICE, "test": r["test"], "collection": COLLECTION},
        )
    flush()
