import os
import subprocess
import tempfile

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret


@task
def fetch_wheel(release_tag: str) -> str:
    print(release_tag)
    return
    dest = f"/tmp/artifacts/{release_tag}"
    os.makedirs(dest, exist_ok=True)
    subprocess.run(
        ["gh", "release", "download", release_tag, "-D", dest, "-p", "*.whl"],
        check=True,
        env={**os.environ, "GH_TOKEN": Secret.load("gh-token").get()},
    )
    return dest


@task
def fetch_sif(image_tag: str) -> str:
    print(image_tag)
    return
    path = f"/tmp/artifacts/myapp-{image_tag}.sif"
    subprocess.run(
        ["apptainer", "pull", path, f"oras://ghcr.io/yourorg/yourrepo:{image_tag}"],
        check=True,
    )
    return path


@task(retries=2, retry_delay_seconds=30)
def deploy_to_target(target: dict, wheel_dir: str, sif_path: str, release_tag: str):
    logger = get_run_logger()
    key = Secret.load(target["ssh_key_block"]).get()
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as kf:
        kf.write(key)
        key_path = kf.name
    os.chmod(key_path, 0o600)

    remote_dir = f"{target['base_path']}/releases/{release_tag}"
    ssh_opts = ["-i", key_path, "-o", "StrictHostKeyChecking=yes"]

    subprocess.run(
        ["ssh", *ssh_opts, f"{target['user']}@{target['host']}", f"mkdir -p {remote_dir}"],
        check=True,
    )
    subprocess.run(
        [
            "rsync",
            "-avz",
            "-e",
            f"ssh {' '.join(ssh_opts)}",
            *[f for f in os_listdir_whl(wheel_dir)],
            sif_path,
            f"{target['user']}@{target['host']}:{remote_dir}/",
        ],
        check=True,
    )

    # Smoke test before flipping the symlink
    result = subprocess.run(
        [
            "ssh",
            *ssh_opts,
            f"{target['user']}@{target['host']}",
            f"apptainer exec {remote_dir}/myapp-{release_tag}.sif python -c 'import mypkg'",
        ],
        capture_output=True,
    )
    if result.returncode != 0:
        logger.error(f"Smoke test failed on {target['host']}: {result.stderr}")
        raise RuntimeError(f"Smoke test failed on {target['name']}")

    subprocess.run(
        ["ssh", *ssh_opts, f"{target['user']}@{target['host']}", f"ln -sfn {remote_dir} {target['base_path']}/current"],
        check=True,
    )
    logger.info(f"Deployed {release_tag} to {target['name']}")


@flow(name="deploy-to-hpc")
def deploy(release_tag: str, image_tag: str):
    print("RUNNING")
    print(release_tag)
    print(image_tag)
    wheel_dir = fetch_wheel(release_tag)
    sif_path = fetch_sif(image_tag)
    # targets = JSON.load("hpc-targets").value  # list of dicts
    # deploy_to_target.map(targets, wheel_dir=wheel_dir, sif_path=sif_path, release_tag=release_tag)
