"""
AgriShield - Incremental Ingestion | NASA CMR API -> Bronze (Raw)
Window: 2026-10-21 to 2026-11-19 (last 30 days, demo)
- First run: backfills from --start
- Daily run: only ingests new granules where granule_date > watermark
- Late data: re-checks last 3 days
- Deduplication: skips if manifest exists
- Watermark: data/control/watermark.json, only updated after success
"""
import json, argparse
from pathlib import Path
from datetime import datetime, timedelta, date
import earthaccess
DATASETS = {"gpm": {"short_name": "GPM_3IMERGDF", "version": "07"},"modis_ndvi": {"short_name": "MOD13A2", "version": "061"},"smap": {"short_name": "SPL3SMP", "version": "009"},"viirs_fire": {"short_name": "VNP14IMGT", "version": "002"}}
BRONZE_PATH = Path("./data/bronze")
CONTROL_PATH = Path("./data/control/watermark.json")
LATE_DAYS = 3
def get_watermark(dataset):
    if not CONTROL_PATH.exists(): return None
    return json.loads(CONTROL_PATH.read_text()).get(dataset, {}).get("max_ingested_date")
def save_watermark(dataset, max_date, granules):
    CONTROL_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(CONTROL_PATH.read_text()) if CONTROL_PATH.exists() else {}
    data[dataset] = {"max_ingested_date": max_date, "last_run": datetime.utcnow().isoformat(), "granules_ingested": granules}
    CONTROL_PATH.write_text(json.dumps(data, indent=2))
    print(f"[Watermark] {dataset} -> {max_date}")
def already_ingested(dataset, granule_id, granule_date):
    y,m,d = granule_date.split("-")
    dest = BRONZE_PATH / dataset / f"year={y}" / f"month={m}" / f"day={d}"
    for manifest in dest.glob("_manifest_*.json"):
        if json.loads(manifest.read_text()).get("granule_id") == granule_id: return True
    return False
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", choices=list(DATASETS.keys()), required=True)
    p.add_argument("--start", default="2026-10-21")
    p.add_argument("--end", default=None)
    p.add_argument("--bbox", default="2.5,6.0,15.0,14.0")
    p.add_argument("--full-refresh", action="store_true")
    args = p.parse_args()
    cfg = DATASETS[args.dataset]; bbox = tuple(map(float, args.bbox.split(",")))
    watermark = None if args.full_refresh else get_watermark(args.dataset)
    end_date = args.end or (date.today() - timedelta(days=1)).isoformat()
    if watermark:
        start_date = (datetime.fromisoformat(watermark) - timedelta(days=LATE_DAYS)).date().isoformat()
        print(f"[Incremental] Last success: {watermark} | Window: {start_date} to {end_date} | Re-checking last {LATE_DAYS} days for late data")
    else:
        start_date = args.start; print(f"[Backfill] First run. Window: {start_date} to {end_date}")
    results = earthaccess.search_data(short_name=cfg["short_name"], version=cfg["version"], temporal=(start_date, end_date), bounding_box=bbox)
    print(f"Found {len(results)} granules in CMR")
    ingested, skipped, max_date_seen = 0, 0, start_date
    for r in results:
        granule_id = getattr(r, "id", "unknown")
        try: granule_date = str(r.temporal[0])[:10]
        except Exception: granule_date = start_date
        max_date_seen = max(max_date_seen, granule_date)
        if already_ingested(args.dataset, granule_id, granule_date):
            print(f"  SKIP (already exists): {granule_id}"); skipped+=1; continue
        y,m,d = granule_date.split("-")
        dest = BRONZE_PATH / args.dataset / f"year={y}" / f"month={m}" / f"day={d}"
        dest.mkdir(parents=True, exist_ok=True)
        files = earthaccess.download([r], local_path=str(dest))
        manifest = {"dataset": args.dataset, "short_name": cfg["short_name"], "granule_id": granule_id, "granule_date": granule_date, "downloaded_at": datetime.utcnow().isoformat(), "is_late": granule_date < watermark if watermark else False, "local_files": files, "bbox": bbox}
        (dest / f"_manifest_{granule_id}.json").write_text(json.dumps(manifest, indent=2, default=str))
        print(f"  NEW: {granule_id} -> {files}"); ingested+=1
    if ingested>0 or skipped>0: save_watermark(args.dataset, max_date_seen, ingested)
    print(f"Done. Ingested: {ingested}, Skipped (duplicates): {skipped} | Watermark: {max_date_seen}")
if __name__ == "__main__": main()
