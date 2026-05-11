import os, time, logging, requests
from datetime import datetime, timezone, timedelta
from otel_push import record, flush

STAC_URL    = os.environ.get("STAC_URL",    "https://stac.eodc.eu/api/v1")
BROWSER_URL = os.environ.get("BROWSER_URL", "https://services.eodc.eu/browser")
ENV         = os.environ.get("E2E_ENV",     "dev")
TIMEOUT  = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def request(method, url, **kwargs):
    t0 = time.perf_counter()
    try:
        r = getattr(requests, method)(url, timeout=TIMEOUT, allow_redirects=True, **kwargs)
        return r.status_code, time.perf_counter() - t0, r
    except Exception:
        return 0, time.perf_counter() - t0, None


def push(probe_name, collection, success, duration, status):
    record(
        {"eodc_e2e_probe_success":           int(success),
         "eodc_e2e_probe_duration_seconds":  duration,
         "eodc_e2e_probe_http_status":       status,
         "eodc_e2e_probe_last_run_timestamp": time.time()},
        {"env": ENV, "service": "stac", "probe": probe_name, "collection": collection},
    )


def push_browser(url, success, duration, status):
    record(
        {"eodc_e2e_browser_reachable":          int(success),
         "eodc_e2e_browser_duration_seconds":   duration,
         "eodc_e2e_browser_last_run_timestamp": time.time()},
        {"env": ENV, "url": url},
    )


def ok(status):
    return 200 <= status < 300


def run():
    all_ok = True

    # Browser frontend check — push deferred until collections result is known
    browser_status, browser_dur, _ = request("get", BROWSER_URL)

    # Root
    status, dur, _ = request("get", f"{STAC_URL}/")
    result = ok(status)
    log.info("root              %s  http=%d  %.0fms", "OK" if result else "FAIL", status, dur * 1000)
    push("root", "_", result, dur, status)
    all_ok = all_ok and result

    # Collections list
    status, dur, resp = request("get", f"{STAC_URL}/collections")
    collections_ok = ok(status)
    log.info("collections_list  %s  http=%d  %.0fms", "OK" if collections_ok else "FAIL", status, dur * 1000)
    push("collections_list", "_", collections_ok, dur, status)
    all_ok = all_ok and collections_ok

    # Browser is only up if frontend loads AND collections are reachable
    browser_result = ok(browser_status) and collections_ok
    log.info("browser_home  %s  http=%d  collections=%d  %.0fms  url=%s",
             "OK" if browser_result else "FAIL", browser_status, int(collections_ok), browser_dur * 1000, BROWSER_URL)
    push_browser(BROWSER_URL, browser_result, browser_dur, browser_status)
    all_ok = all_ok and browser_result

    if not collections_ok:
        log.error("Cannot list collections — skipping per-collection probes")
        flush()
        return all_ok

    col_ids = [c["id"] for c in resp.json().get("collections", []) if "id" in c]
    log.info("Found %d collections", len(col_ids))

    for col_id in col_ids:
        # Collection detail
        status, dur, _ = request("get", f"{STAC_URL}/collections/{col_id}")
        result = ok(status)
        log.info("  %s  collection_detail  %s  http=%d  %.0fms", col_id, "OK" if result else "FAIL", status, dur * 1000)
        push("collection_detail", col_id, result, dur, status)
        all_ok = all_ok and result

        # Items list
        status, dur, resp = request("get", f"{STAC_URL}/collections/{col_id}/items", params={"limit": 5})
        result = ok(status)
        log.info("  %s  items_list         %s  http=%d  %.0fms", col_id, "OK" if result else "FAIL", status, dur * 1000)
        push("items_list", col_id, result, dur, status)
        all_ok = all_ok and result

        # Search (last 90 days)
        now   = datetime.now(timezone.utc)
        start = (now - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
        status, dur, _ = request("post", f"{STAC_URL}/search",
                                 json={"limit": 5, "collections": [col_id],
                                       "datetime": f"{start}/{now.strftime('%Y-%m-%dT%H:%M:%SZ')}"})
        result = ok(status)
        log.info("  %s  search_post        %s  http=%d  %.0fms", col_id, "OK" if result else "FAIL", status, dur * 1000)
        push("search_post", col_id, result, dur, status)
        all_ok = all_ok and result

        # Asset fetch — find first http asset URL from items
        asset_url = None
        if resp and resp.status_code == 200:
            for feat in resp.json().get("features", []):
                for asset in feat.get("assets", {}).values():
                    if isinstance(asset, dict) and asset.get("href", "").startswith("http"):
                        asset_url = asset["href"]
                        break
                if asset_url:
                    break

        if asset_url:
            status, dur, _ = request("head", asset_url)
            result = ok(status) or status in (401, 403, 405)
            log.info("  %s  asset_fetch        %s  http=%d  %.0fms", col_id, "OK" if result else "FAIL", status, dur * 1000)
            push("asset_fetch", col_id, result, dur, status)
        else:
            log.info("  %s  asset_fetch        skipped — no http asset URL found", col_id)
            push("asset_fetch", col_id, True, 0, 0)

    flush()
    return all_ok


if __name__ == "__main__":
    success = run()
    if not success:
        raise SystemExit(1)
