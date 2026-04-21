import gevent.monkey
gevent.monkey.patch_all()

import os, time, random, logging, gevent
from datetime import datetime, timedelta
from locust import HttpUser, task, between
from locust.env import Environment
from locust.log import setup_logging
from shapely.geometry import Polygon, mapping
from otel_push import record, flush

STAC_URL   = os.environ.get("STAC_URL", "https://stac.eodc.eu/api/v1")
ENV        = os.environ.get("E2E_ENV", "dev")

VU_STAGES  = [10, 25, 50, 100]
STAGE_SECS = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


class StacUser(HttpUser):
    host = STAC_URL
    wait_time = between(1, 3)
    _collections: list = []

    def on_start(self):
        resp = self.client.get("/collections", name="GET /collections")
        if resp.status_code == 200:
            self._collections = [c["id"] for c in resp.json().get("collections", [])]

    @task
    def search_post(self):
        self.client.post("/search", name="POST /search", json={
            "datetime":    self._random_datetime(),
            "intersects":  self._random_polygon(),
            "collections": self._random_collections(),
            "limit": 100,
        })

    @task
    def get_items(self):
        col = random.choice(self._collections) if self._collections else "unknown"
        self.client.get(f"/collections/{col}/items", params={"limit": 10},
                        name="GET /collections/{id}/items")

    @staticmethod
    def _random_datetime():
        start = datetime(2015, 1, 1)
        end   = datetime(2025, 1, 1)
        delta = int((end - start).total_seconds())
        dates = sorted(start + timedelta(seconds=random.randrange(delta)) for _ in range(2))
        return f"{dates[0].isoformat()}Z/{dates[1].isoformat()}Z"

    @staticmethod
    def _random_polygon():
        cx, cy = random.uniform(-180, 180), random.uniform(-90, 90)
        pts = [(max(min(cx + random.uniform(-10, 10), 180), -180),
                max(min(cy + random.uniform(-10, 10),  90), -90))
               for _ in range(random.randint(3, 10))]
        poly = Polygon(pts)
        return mapping(poly if poly.is_valid else poly.buffer(0))

    def _random_collections(self):
        if not self._collections:
            return []
        return random.sample(self._collections, random.randint(1, min(len(self._collections), 5)))


def push_metrics(all_stages):
    baseline = all_stages.get(VU_STAGES[0], {})
    now      = time.time()

    for vu_count, stats in all_stages.items():
        for endpoint, s in stats.items():
            safe  = endpoint.replace(" ", "").replace("/", "_").replace("{", "").replace("}", "").strip("_")
            ratio = s["p95"] / baseline[endpoint]["p95"] if baseline.get(endpoint, {}).get("p95") else 1.0
            record(
                {"eodc_e2e_perf_p95_seconds":        s["p95"],
                 "eodc_e2e_perf_rps":                s["rps"],
                 "eodc_e2e_perf_error_rate":         s["err"],
                 "eodc_e2e_perf_vus":                float(vu_count),
                 "eodc_e2e_perf_slowdown_ratio":     ratio,
                 "eodc_e2e_perf_last_run_timestamp": now},
                {"env": ENV, "service": "stac", "endpoint": safe, "vus": str(vu_count)},
            )
            log.info("staged  vu=%3d  endpoint=%-30s  p95=%.3fs  rps=%.1f  slowdown=%.2fx",
                     vu_count, endpoint, s["p95"], s["rps"], ratio)

    flush()


def main():
    setup_logging("INFO")
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
            name: {"p95": (entry.get_response_time_percentile(0.95) or 0) / 1000,
                   "rps": entry.total_rps,
                   "err": entry.fail_ratio}
            for (_, name), entry in env.stats.entries.items()
        }
        for name, s in all_stages[vu_count].items():
            log.info("  %s  p95=%.3fs  rps=%.1f  err=%.1f%%", name, s["p95"], s["rps"], s["err"] * 100)

    push_metrics(all_stages)
    env.runner.quit()
    log.info("Done.")


if __name__ == "__main__":
    main()
