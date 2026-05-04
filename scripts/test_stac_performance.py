import gevent.monkey
gevent.monkey.patch_all()

import os
import time
import random
import logging
import gevent
from datetime import datetime, timedelta

from locust import HttpUser, task, between
from locust.env import Environment
from locust.log import setup_logging
from shapely.geometry import Polygon, mapping

from otel_push import record, flush


STAC_URL = os.environ.get("STAC_URL", "https://stac.eodc.eu/api/v1")
ENV = os.environ.get("E2E_ENV", "dev")

VU_STAGES = [10, 25, 50, 100]
STAGE_SECS = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


class StacUser(HttpUser):
    host = STAC_URL
    wait_time = between(1, 3)
    _collections: list[str] = []

    def on_start(self):
        with self.client.get("/collections", name="GET /collections", catch_response=True) as resp:
            if resp.status_code != 200:
                self._collections = []
                resp.failure(f"Could not load collections: HTTP {resp.status_code}")
                return

            try:
                data = resp.json()
                self._collections = [
                    c["id"]
                    for c in data.get("collections", [])
                    if "id" in c
                ]
                resp.success()
                log.info("Loaded %d collections", len(self._collections))
            except Exception as e:
                self._collections = []
                resp.failure(f"Could not parse collections response: {e}")

    @task
    def search_post(self):
        payload = {
            "datetime": self._random_datetime(),
            "intersects": self._random_polygon(),
            "limit": 100,
        }

        collections = self._random_collections()
        if collections:
            payload["collections"] = collections

        self.client.post(
            "/search",
            name="POST_search",
            json=payload,
        )

    @task
    def get_items(self):
        if not self._collections:
            return

        col = random.choice(self._collections)

        self.client.get(
            f"/collections/{col}/items",
            params={"limit": 10},
            name="GET_collections_id_items",
        )

    @staticmethod
    def _random_datetime():
        start = datetime(2015, 1, 1)
        end = datetime(2025, 1, 1)
        delta = int((end - start).total_seconds())

        dates = sorted(
            start + timedelta(seconds=random.randrange(delta))
            for _ in range(2)
        )

        return f"{dates[0].isoformat()}Z/{dates[1].isoformat()}Z"

    @staticmethod
    def _random_polygon():
        cx = random.uniform(-180, 180)
        cy = random.uniform(-90, 90)

        pts = [
            (
                max(min(cx + random.uniform(-10, 10), 180), -180),
                max(min(cy + random.uniform(-10, 10), 90), -90),
            )
            for _ in range(random.randint(3, 10))
        ]

        poly = Polygon(pts)
        if not poly.is_valid:
            poly = poly.buffer(0)

        return mapping(poly)

    def _random_collections(self):
        if not self._collections:
            return []

        return random.sample(
            self._collections,
            random.randint(1, min(len(self._collections), 5)),
        )


def push_metrics(all_stages):
    baseline = all_stages.get(VU_STAGES[0], {})
    now = time.time()

    for vu_count, stats in all_stages.items():
        for endpoint, s in stats.items():
            baseline_p95 = baseline.get(endpoint, {}).get("p95")

            ratio = (
                s["p95"] / baseline_p95
                if baseline_p95
                else 1.0
            )

            record(
                {
                    "eodc_e2e_perf_p95_seconds": s["p95"],
                    "eodc_e2e_perf_p50_seconds": s["p50"],
                    "eodc_e2e_perf_rps": s["rps"],
                    "eodc_e2e_perf_error_rate": s["err"],
                    "eodc_e2e_perf_vus": float(vu_count),
                    "eodc_e2e_perf_slowdown_ratio": ratio,
                    "eodc_e2e_perf_last_run_timestamp": now,
                },
                {
                    "env": ENV,
                    "service": "stac",
                    "endpoint": endpoint,
                    "vus": str(vu_count),
                },
            )

            log.info(
                "staged  vu=%3d  endpoint=%-30s  p50=%.3fs  p95=%.3fs  rps=%.1f  err=%.1f%%  slowdown=%.2fx",
                vu_count,
                endpoint,
                s["p50"],
                s["p95"],
                s["rps"],
                s["err"] * 100,
                ratio,
            )

    flush()


def main():
    setup_logging("INFO")

    log.info("STAC_URL=%s", STAC_URL)
    log.info("ENV=%s", ENV)

    env = Environment(user_classes=[StacUser])
    env.create_local_runner()

    all_stages = {}

    for vu_count in VU_STAGES:
        log.info("Stage %d VUs — %ds", vu_count, STAGE_SECS)

        env.stats.reset_all()
        env.runner.start(vu_count, spawn_rate=10)

        gevent.sleep(STAGE_SECS)

        env.runner.stop()
        gevent.sleep(1)

        all_stages[vu_count] = {
            name: {
                "p95": (entry.get_response_time_percentile(0.95) or 0) / 1000,
                "p50": (entry.get_response_time_percentile(0.50) or 0) / 1000,
                "rps": entry.total_rps,
                "err": entry.fail_ratio,
            }
            for (_, name), entry in env.stats.entries.items()
            if name not in ("", "Aggregated")
        }

        for name, s in all_stages[vu_count].items():
            log.info(
                "  %-30s  p50=%.3fs  p95=%.3fs  rps=%.1f  err=%.1f%%",
                name,
                s["p50"],
                s["p95"],
                s["rps"],
                s["err"] * 100,
            )

    push_metrics(all_stages)

    env.runner.quit()
    log.info("Done.")


if __name__ == "__main__":
    main()