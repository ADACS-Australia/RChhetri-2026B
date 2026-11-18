import os
from datetime import datetime, timedelta, timezone

from astroquery.utils.tap.core import TapPlus
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment

CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"
PROJECT_CODE = "AS305"
LOOKBACK = timedelta(hours=1)

# Deployment to trigger for each new observation, in the form
# "flow-name/deployment-name". Override it with CASDA_PROCESSING_DEPLOYMENT.
PROCESSING_DEPLOYMENT = os.environ.get(
    "CASDA_PROCESSING_DEPLOYMENT",
    "download-observation/download-observation",
)


def _adql_timestamp(value: datetime) -> str:
    """Format a datetime as an ADQL-friendly ISO 8601 UTC string."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def observation_query(start: datetime, end: datetime) -> str:
    """Build the ADQL query for observations of PROJECT_CODE in [start, end]."""
    return f"""
        SELECT DISTINCT
            o.sbid,
            e.obs_id,
            o.obs_start,
            o.obs_end,
            o.obs_program,
            o.deposit_state,
            e.project_code
        FROM casda.observation AS o
        JOIN casda.observation_event AS e
            ON o.sbid = CAST(e.sbid AS INTEGER)
        WHERE e.project_code = '{PROJECT_CODE}'
            AND o.obs_start >= '{_adql_timestamp(start)}'
            AND o.obs_start < '{_adql_timestamp(end)}'
        ORDER BY o.obs_start
    """


@task(retries=3, retry_delay_seconds=60)
def fetch_new_observations(start: datetime, end: datetime) -> list[dict]:
    """Return the observations made in [start, end) for PROJECT_CODE."""
    tap = TapPlus(url=CASDA_TAP_URL)
    results = tap.launch_job(observation_query(start, end)).get_results()
    return [
        {name: value.item() if hasattr(value, "item") else value for name, value in zip(results.colnames, row)}
        for row in results
    ]


@task(retries=2, retry_delay_seconds=30)
def trigger_processing(observation: dict) -> None:
    """Trigger the processing deployment for a single observation."""
    logger = get_run_logger()

    flow_run = run_deployment(
        PROCESSING_DEPLOYMENT,
        parameters={"observation": observation},
        flow_run_name=f"process-{observation['obs_id']}",
        idempotency_key=f"casda-{observation['obs_id']}",
        timeout=0,
        as_subflow=False,
    )

    logger.info(
        "Triggered %s for %s (flow run %s).",
        PROCESSING_DEPLOYMENT,
        observation["obs_id"],
        flow_run.id,
    )


@flow(name="casda-query", log_prints=True)
def casda_query() -> None:
    """Check CASDA hourly for new observations under PROJECT_CODE."""
    logger = get_run_logger()

    # Cover the hour that has just finished. Flooring to the hour keeps
    # consecutive runs contiguous even when the scheduler starts them late.
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - LOOKBACK

    observations = fetch_new_observations(start, end)

    if not observations:
        logger.info(
            "No new %s observations between %s and %s.",
            PROJECT_CODE,
            start.isoformat(),
            end.isoformat(),
        )
        return

    logger.info(
        "Found %d new %s observation(s) between %s and %s:",
        len(observations),
        PROJECT_CODE,
        start.isoformat(),
        end.isoformat(),
    )
    for observation in observations:
        logger.info(
            "Found %s (SBID %s): %s to %s [%s] %s",
            observation["obs_id"],
            observation["sbid"],
            observation["obs_start"],
            observation["obs_end"],
            observation["deposit_state"],
            observation["obs_program"],
        )
        trigger_processing(observation)


if __name__ == "__main__":
    casda_query.serve(name="casda-query", cron="0 * * * *")
