"""Set up or remove the isolated disposable benchmark stack."""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from time import monotonic, sleep
from urllib.request import ProxyHandler, Request, build_opener

if not __debug__:
    raise SystemExit("Do not use python -O: this benchmark requires its assertions.")

DIR = Path(__file__).resolve().parent
PROJECT = "ovation-http-ndjson-size"
VOLUME = f"{PROJECT}_pgdata"
PGDATA = "/var/lib/postgresql/data"
MARKER = {"org.ovation.benchmark.disposable": "http-ndjson-size"}
COMPOSE = (
    "compose",
    "--project-name",
    PROJECT,
    "--env-file",
    os.devnull,
    "-f",
    str(DIR.parents[1] / "compose.yaml"),
    "-f",
    str(DIR / "compose.yaml"),
)


def docker(*args, timeout=600, script=None):
    # Inherit the Docker runtime/context, but keep Compose settings and writer defaults local.
    env = {
        key: value
        for key, value in os.environ.items()
        if key == "WRITER_API_TOKEN" or not key.startswith(("COMPOSE_", "WRITER_"))
    }
    result = subprocess.run(
        ["docker", *args], input=script, capture_output=True, text=True, timeout=timeout, env=env
    )
    if result.returncode:
        raise RuntimeError(
            f"docker {args[0]} failed: {result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.strip()


def compose(*args, timeout=600, script=None):
    return docker(*COMPOSE, *args, timeout=timeout, script=script)


def psql(query, timeout=600):
    return compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-XAt",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        "/var/run/postgresql",
        "-U",
        "writer",
        "-d",
        "stream_writer",
        timeout=timeout,
        script=query,
    )


def require(condition, detail):
    if not condition:
        raise RuntimeError(f"Refusing to act on unrecognized benchmark resources: {detail}")


def matches(labels, expected):
    return all((labels or {}).get(key) == value for key, value in expected.items())


def require_local_docker():
    host = os.environ.get("DOCKER_HOST")
    if host:
        require(host.startswith("unix:///"), "DOCKER_HOST must be a local Unix socket")
    context = os.environ.get("DOCKER_CONTEXT")
    if context or not host:
        endpoint = docker(
            "context",
            "inspect",
            "--format",
            "{{.Endpoints.docker.Host}}",
            *([context] if context else []),
        )
        require(endpoint.startswith("unix:///"), "Docker context must use a local Unix socket")


def inspect_volume():
    (volume,) = json.loads(docker("volume", "inspect", VOLUME))
    require(
        volume["Name"] == VOLUME
        and volume["Driver"] == "local"
        and not volume["Options"]
        and matches(
            volume["Labels"],
            MARKER
            | {
                "com.docker.compose.project": PROJECT,
                "com.docker.compose.volume": "postgres-data",
            },
        ),
        "PostgreSQL volume must be the dedicated local Compose volume",
    )


def inspect_container(container):
    labels = container["Config"]["Labels"]
    service = (labels or {}).get("com.docker.compose.service")
    require(
        service in ("postgres", "init-db", "writer")
        and matches(labels, MARKER | {"com.docker.compose.project": PROJECT}),
        "service and disposable Compose labels must match",
    )
    if service == "postgres":
        mounts = container["Mounts"]
        require(
            len(mounts) == 1
            and (mount := mounts[0])["Type"] == "volume"
            and mount["Name"] == VOLUME
            and mount["Destination"] == PGDATA
            and mount["RW"]
            and [value for value in container["Config"]["Env"] if value.startswith("PGDATA=")]
            == [f"PGDATA={PGDATA}"],
            "PostgreSQL must use only the dedicated volume as PGDATA",
        )
        inspect_volume()
    return service


def inspect_project(*required_services):
    require_local_docker()
    ids = compose("ps", "--all", "--quiet").split()
    services = []
    if ids:
        services = [
            inspect_container(container)
            for container in json.loads(docker("container", "inspect", *ids))
        ]
    require(
        len(services) == len(set(services)) and all(name in services for name in required_services),
        "the expected Compose services must exist without duplicate containers",
    )
    # Also refuse an unmarked volume left behind when no PostgreSQL container exists.
    if "postgres" not in services and VOLUME in docker("volume", "ls", "-q").splitlines():
        inspect_volume()


def api_url():
    address = compose("port", "writer", "8080")
    require(re.fullmatch(r"127\.0\.0\.1:[0-9]+", address), "HTTP must be bound to local loopback")
    return f"http://{address}"


def wait_ready():
    token = os.environ["WRITER_API_TOKEN"]
    request = Request(f"{api_url()}/health/ready", headers={"Authorization": f"Bearer {token}"})
    opener = build_opener(ProxyHandler({}))
    deadline = monotonic() + 60
    last_error = None
    while (remaining := deadline - monotonic()) > 0:
        try:
            with opener.open(request, timeout=min(remaining, 2)) as response:
                return response.read().decode().strip()
        except OSError as exc:
            last_error = exc
            sleep(0.5)
    raise RuntimeError("Benchmark writer did not become ready within 60 seconds") from last_error


def setup():
    password = os.environ.get("BENCHMARK_WRITER_PASSWORD", "")
    for key in ("POSTGRES_PASSWORD", "BENCHMARK_WRITER_PASSWORD", "WRITER_API_TOKEN"):
        if not re.fullmatch(r"[A-Za-z0-9._~-]+", os.environ.get(key, "")):
            raise SystemExit(f"Set {key} to a URL-safe credential for this disposable stack.")
    inspect_project()
    compose("up", "-d", "--wait", "postgres")
    inspect_project("postgres")
    compose("run", "--rm", "--no-deps", "--build", "init-db")
    psql(rf"""
        SELECT 'CREATE ROLE benchmark_writer'
        WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='benchmark_writer')
        \gexec
        ALTER ROLE benchmark_writer LOGIN NOSUPERUSER CONNECTION LIMIT 4 PASSWORD '{password}';
        GRANT USAGE ON SCHEMA stream_writer TO benchmark_writer;
        GRANT SELECT, INSERT, UPDATE ON stream_writer.uploads TO benchmark_writer;
        GRANT SELECT, INSERT ON stream_writer.rows TO benchmark_writer;
    """)
    compose("up", "-d", "--wait", "--no-deps", "writer")
    inspect_project("postgres", "writer")
    print(wait_ready())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "down"))
    if parser.parse_args().command == "setup":
        setup()
    else:
        inspect_project()
        compose("down", "--volumes")


if __name__ == "__main__":
    main()
