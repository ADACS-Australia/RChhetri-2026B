import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

# Load the local .env before importing Prefect so its settings pick up the
# API URL and auth string. Existing shell environment variables take priority.
load_dotenv()

_public_host = os.environ.get("PUBLIC_HOST", "").removeprefix("https://").removeprefix("http://").rstrip("/")
if _public_host and not os.environ.get("PREFECT_API_URL"):
    os.environ["PREFECT_API_URL"] = f"https://{_public_host}/api"

_prefect_password = os.environ.get("PREFECT_PASSWORD")
if _prefect_password and not os.environ.get("PREFECT_API_AUTH_STRING") and not os.environ.get("PREFECT_API_KEY"):
    os.environ["PREFECT_API_AUTH_STRING"] = f"admin:{_prefect_password}"

import requests
from astropy.io import fits
from astroquery.utils.tap.core import TapPlus
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment
from prefect.runtime import flow_run

CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"
DAP_WS_URL = "https://data.csiro.au/dap/ws/v2/domains/casdaObservation"
PROJECT_CODE = "AS305"
CALIBRATOR_HEADER_KEY = "SB-CAL"

# One source of truth for both the serving schedule and the query window.
SCHEDULE_INTERVAL = timedelta(minutes=30)

ALL_TIME_START = datetime(2000, 1, 1, tzinfo=timezone.utc)

# Deployment to trigger for each new observation, in the form
# "flow-name/deployment-name". Override it with CASDA_PROCESSING_DEPLOYMENT.
PROCESSING_DEPLOYMENT = os.environ.get(
    "CASDA_PROCESSING_DEPLOYMENT",
    "download-observation/download-observation-ozstar",
)


def _adql_timestamp(value: datetime) -> str:
    """Format a datetime as an ADQL-friendly ISO 8601 UTC string."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cron_for_interval(interval: timedelta) -> str:
    """Return a cron expression for common fixed schedule intervals."""
    total_minutes = int(interval.total_seconds() // 60)
    if total_minutes <= 0:
        raise ValueError("Schedule interval must be positive")

    if total_minutes < 60:
        if 60 % total_minutes == 0:
            return f"*/{total_minutes} * * * *"
        raise ValueError("Intervals under an hour must divide evenly into an hour.")

    if total_minutes % 60 != 0:
        raise ValueError("Schedule interval must be a whole number of hours.")

    hours = total_minutes // 60
    if hours == 1:
        return "0 * * * *"
    if hours == 24:
        return "0 0 * * *"
    if 1 < hours < 24 and 24 % hours == 0:
        return f"0 */{hours} * * *"

    raise ValueError(
        "Unsupported schedule interval. Use a whole-hour interval that divides "
        "evenly into 24 hours (1, 2, 3, 4, 6, 8, 12 or 24 hours)."
    )


def observation_query(start: datetime, end: datetime) -> str:
    """Build the ADQL query for observations of PROJECT_CODE deposited in [start, end]."""
    return f"""
        SELECT DISTINCT
            o.sbid,
            e.obs_id,
            o.obs_start,
            o.obs_end,
            o.obs_program,
            o.deposit_state,
            e.project_code,
            e.event_date AS deposited_at
        FROM casda.observation AS o
        JOIN casda.observation_event AS e
            ON o.sbid = CAST(e.sbid AS INTEGER)
        WHERE e.project_code = '{PROJECT_CODE}'
            AND e.event_type = 'DEPOSITED'
            AND e.event_date >= '{_adql_timestamp(start)}'
            AND e.event_date < '{_adql_timestamp(end)}'
        ORDER BY deposited_at
    """


def _tap_query(query: str) -> list[dict]:
    """Run an ADQL query and return the rows as plain dictionaries."""
    tap = TapPlus(url=CASDA_TAP_URL)
    results = tap.launch_job(query).get_results()
    return [
        {name: value.item() if hasattr(value, "item") else value for name, value in zip(results.colnames, row)}
        for row in results
    ]


def cube_query(obs_id: str) -> str:
    """Build the query for the image cubes belonging to an observation."""
    return f"""
        SELECT
            obs_publisher_did,
            dataproduct_subtype,
            filename,
            obs_release_date
        FROM ivoa.obscore
        WHERE obs_id = '{obs_id}'
            AND dataproduct_type = 'cube'
        ORDER BY filename
    """


def _is_released(release_date) -> bool:
    """Return whether a product's release date has passed."""
    if release_date is None or str(release_date).strip() in ("", "--"):
        return False

    if isinstance(release_date, datetime):
        released_at = release_date
    else:
        try:
            released_at = datetime.fromisoformat(str(release_date).replace("Z", "+00:00"))
        except ValueError:
            return False

    if released_at.tzinfo is None:
        released_at = released_at.replace(tzinfo=timezone.utc)

    return released_at <= datetime.now(timezone.utc)


def _cube_sort_key(cube: dict) -> tuple:
    """Prefer the restored Stokes I continuum cube when choosing a header source."""
    subtype = str(cube.get("dataproduct_subtype") or "")
    filename = str(cube.get("filename") or "")
    return (
        not (subtype == "cont.restored.t0" and filename.startswith("image.i.")),
        not subtype.startswith("cont.restored"),
        not subtype.startswith("cont."),
        filename,
    )


def fetch_product_metadata(product_type: str, product_id: int, *, released: bool) -> dict:
    """Fetch a data product's metadata, including its FITS header, from the DAP web service."""
    response = requests.get(
        f"{DAP_WS_URL}/{product_type}/{product_id}/metadata",
        params={"released": "true" if released else "false"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def _cube_header(cube: dict) -> fits.Header | None:
    """Return the FITS header for a cube, or None if it cannot be read."""
    publisher_did = str(cube["obs_publisher_did"])
    product_type, _, product_id = publisher_did.partition("-")
    if product_type != "cube":
        return None

    try:
        metadata = fetch_product_metadata(
            "IMAGE_CUBE",
            int(product_id),
            released=_is_released(cube.get("obs_release_date")),
        )
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in (400, 404):
            return None
        raise

    cards = metadata.get("imageHeader")
    if not cards:
        return None

    try:
        return fits.Header([fits.Card.fromstring(card) for card in cards])
    except (ValueError, TypeError, UnicodeError):
        return None


def match_calibrator(observation: dict) -> int | None:
    """Return the calibrator SBID referenced by an observation's image cube."""
    cubes = _tap_query(cube_query(observation["obs_id"]))

    for cube in sorted(cubes, key=_cube_sort_key):
        header = _cube_header(cube)
        if header is None or CALIBRATOR_HEADER_KEY not in header:
            continue

        try:
            if "SBID" in header and int(header["SBID"]) != int(observation["sbid"]):
                continue
            return int(header[CALIBRATOR_HEADER_KEY])
        except (TypeError, ValueError):
            continue

    return None


def get_new_observations(start: datetime, end: datetime) -> list[dict]:
    """Return the observations deposited in [start, end) for PROJECT_CODE."""
    observations = _tap_query(observation_query(start, end))

    # An observation can have more than one DEPOSITED event; keep the first.
    unique = {}
    for observation in observations:
        unique.setdefault(observation.get("obs_id") or observation["sbid"], observation)
    return list(unique.values())


@task(retries=3, retry_delay_seconds=60)
def fetch_new_observations(start: datetime, end: datetime) -> list[dict]:
    """Return the observations deposited in [start, end) for PROJECT_CODE."""
    return get_new_observations(start, end)


@task(retries=2, retry_delay_seconds=30)
def find_calibrator(observation: dict) -> int | None:
    """Match an observation to the calibrator referenced in its image cube header."""
    logger = get_run_logger()
    calibrator_sbid = match_calibrator(observation)

    if calibrator_sbid is None:
        logger.warning(
            "Could not determine the calibrator for %s (SBID %s).",
            observation["obs_id"],
            observation["sbid"],
        )
    else:
        logger.info(
            "Matched %s (SBID %s) to calibrator SBID %s.",
            observation["obs_id"],
            observation["sbid"],
            calibrator_sbid,
        )

    return calibrator_sbid


@task(retries=2, retry_delay_seconds=30)
def trigger_processing(target_sbid: int, calibrator_sbid: int) -> None:
    """Trigger the download deployment for a target/calibrator SBID pair."""
    logger = get_run_logger()

    flow_run = run_deployment(
        PROCESSING_DEPLOYMENT,
        parameters={
            "target_sbid": target_sbid,
            "calibrator_sbid": calibrator_sbid,
        },
        flow_run_name=f"download-{target_sbid}",
        idempotency_key=f"casda-{target_sbid}",
        timeout=0,
        as_subflow=False,
    )

    logger.info(
        "Triggered %s for target SBID %s with calibrator SBID %s (flow run %s).",
        PROCESSING_DEPLOYMENT,
        target_sbid,
        calibrator_sbid,
        flow_run.id,
    )


@flow(name="casda-query", log_prints=True)
def casda_query(
    start: datetime | None = None,
    end: datetime | None = None,
    dry_run: bool = False,
) -> None:
    """Check CASDA for newly deposited observations under PROJECT_CODE."""
    logger = get_run_logger()

    # Align the default window to the scheduled run time. This keeps
    # consecutive runs contiguous even when a run starts late.
    if end is None:
        end = flow_run.scheduled_start_time
    if start is None:
        start = end - SCHEDULE_INTERVAL
    if start >= end:
        raise ValueError("start must be before end")

    observations = fetch_new_observations(start, end)

    if not observations:
        logger.info(
            "No new %s observations deposited between %s and %s.",
            PROJECT_CODE,
            start.isoformat(),
            end.isoformat(),
        )
        return

    logger.info(
        "Found %d new %s observation(s) deposited between %s and %s:",
        len(observations),
        PROJECT_CODE,
        start.isoformat(),
        end.isoformat(),
    )
    for observation in observations:
        logger.info(
            "Found %s (SBID %s): %s to %s [%s] %s (deposited %s)",
            observation["obs_id"],
            observation["sbid"],
            observation["obs_start"],
            observation["obs_end"],
            observation["deposit_state"],
            observation["obs_program"],
            observation["deposited_at"],
        )

        calibrator_sbid = find_calibrator(observation)
        if calibrator_sbid is None:
            logger.warning(
                "Skipping %s (SBID %s): no image cube, FITS header or %s value.",
                observation["obs_id"],
                observation["sbid"],
                CALIBRATOR_HEADER_KEY,
            )
            continue

        if dry_run:
            logger.info(
                "Dry run: would trigger %s for target SBID %s with calibrator SBID %s.",
                PROCESSING_DEPLOYMENT,
                observation["sbid"],
                calibrator_sbid,
            )
            continue

        trigger_processing(observation["sbid"], calibrator_sbid)


def check_observations(start: datetime, end: datetime) -> None:
    """Query a window and print calibrator matches without triggering deployments."""
    observations = get_new_observations(start, end)

    print(f"Found {len(observations)} observation(s) deposited between " f"{start.isoformat()} and {end.isoformat()}.")
    for observation in observations:
        calibrator_sbid = match_calibrator(observation)
        line = f"  {observation['deposited_at']}  " f"{observation['obs_id']} (SBID {observation['sbid']})"
        if calibrator_sbid is None:
            print(f"{line}: no calibrator -> would be skipped")
        else:
            print(f"{line}: calibrator SBID {calibrator_sbid}")


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO 8601 datetime, assuming UTC when no timezone is given."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Query CASDA for newly deposited observations.",
        epilog=(
            "Without --run-flow, --all-time/--start/--end perform a local check "
            "only: no Prefect flow run and no deployments are created."
        ),
    )
    parser.add_argument(
        "--run-flow",
        action="store_true",
        help="Run the Prefect flow once for the selected window.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --run-flow, run the flow but skip run_deployment.",
    )
    parser.add_argument(
        "--all-time",
        action="store_true",
        help="Query from 2000-01-01 up to now.",
    )
    parser.add_argument(
        "--start",
        type=_parse_datetime,
        help="Start of the deposit window (ISO 8601, UTC assumed).",
    )
    parser.add_argument(
        "--end",
        type=_parse_datetime,
        help="End of the deposit window (ISO 8601, UTC assumed).",
    )
    args = parser.parse_args()

    if args.dry_run and not args.run_flow:
        parser.error("--dry-run requires --run-flow")

    if args.run_flow:
        end = args.end or datetime.now(timezone.utc)
        start = args.start or (ALL_TIME_START if args.all_time else end - SCHEDULE_INTERVAL)
        casda_query(start=start, end=end, dry_run=args.dry_run)
    elif args.all_time or args.start or args.end:
        end = args.end or datetime.now(timezone.utc)
        start = args.start or (ALL_TIME_START if args.all_time else end - SCHEDULE_INTERVAL)
        check_observations(start, end)
    else:
        casda_query.serve(
            name="casda-query",
            cron=cron_for_interval(SCHEDULE_INTERVAL),
        )
