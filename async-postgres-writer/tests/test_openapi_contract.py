from pathlib import Path

import httpx
import pytest
import yaml
from fastapi.routing import APIRoute
from openapi_schema_validator import OAS31Validator
from openapi_spec_validator import validate

from async_postgres_writer.app import create_app
from async_postgres_writer.config import Settings
from async_postgres_writer.writer import Store, WriterError

SETTINGS = Settings(database_url="postgresql://localhost/test", api_token="a" * 32)
ROOT = Path(__file__).resolve().parents[1]
UPLOAD = "/v1/uploads/{upload_id}"
ROWS = f"{UPLOAD}/rows"
STREAM = f"{ROWS}/stream"
OPERATION_IDS = {
    ("GET", "/health/live"): "checkLiveness",
    ("GET", "/health/ready"): "checkReadiness",
    ("PUT", UPLOAD): "uploadNdjson",
    ("GET", UPLOAD): "getUploadReceipt",
    ("GET", ROWS): "listUploadRows",
    ("GET", STREAM): "streamUploadRows",
}


@pytest.fixture
def document():
    return yaml.safe_load((ROOT / "openapi/stream-writer.v1.yaml").read_text(encoding="utf-8"))


async def test_openapi_is_a_valid_published_contract(document):
    validate(document)
    app = create_app(SETTINGS)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        published = await http.get("/openapi.json")
        docs = await http.get("/docs")

    assert published.status_code == 200 and published.json() == document
    assert docs.status_code == 200 and "url: '/openapi.json'" in docs.text
    assert {
        (method.upper(), path): operation["operationId"]
        for path, item in document["paths"].items()
        for method, operation in item.items()
        if method.upper() in {"GET", "PUT"}
    } == OPERATION_IDS
    assert {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    } == OPERATION_IDS.keys()


def test_write_only_variant_does_not_publish_disabled_routes():
    class WriteOnlyStore(Store):
        supports_result_reads = False

    original = create_app(SETTINGS).openapi()
    limited = create_app(SETTINGS, store_type=WriteOnlyStore).openapi()
    assert set(original["paths"]) - limited["paths"].keys() == {ROWS, STREAM}


@pytest.mark.parametrize(
    "method,template,params,token,status",
    [
        ("GET", "/health/live", {}, None, 200),
        ("GET", "/health/ready", {}, None, 401),
        ("GET", UPLOAD, {}, "bad", 401),
        ("PUT", UPLOAD, {}, SETTINGS.api_token.get_secret_value(), 415),
        ("GET", UPLOAD, {}, SETTINGS.api_token.get_secret_value(), 404),
        ("GET", ROWS, {"limit": -1}, SETTINGS.api_token.get_secret_value(), 422),
        (
            "GET",
            ROWS,
            {"offset": 0, "after_row_number": 0},
            SETTINGS.api_token.get_secret_value(),
            422,
        ),
        ("GET", STREAM, {}, SETTINGS.api_token.get_secret_value(), 404),
    ],
)
async def test_observed_json_responses_validate_against_contract(
    document, method, template, params, token, status
):
    class EmptyStore:
        async def lookup(self, _):
            return None

        def stream_rows(self, _):
            raise WriterError(404, "upload_not_found", "Committed upload not found")

    app = create_app(SETTINGS)
    app.state.store = EmptyStore()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        response = await http.request(
            method,
            template.format(upload_id="00000000-0000-0000-0000-000000000001"),
            params=params,
            headers={"Authorization": f"Bearer {token}"} if token else None,
        )

    assert response.status_code == status
    mime = response.headers["content-type"].split(";")[0]
    contract_response = document["paths"][template][method.lower()]["responses"][str(status)]
    if "$ref" in contract_response:
        contract_response = document["components"]["responses"][
            contract_response["$ref"].rsplit("/", 1)[1]
        ]
    validator = OAS31Validator(document, format_checker=OAS31Validator.FORMAT_CHECKER)
    validator.evolve(schema=contract_response["content"][mime]["schema"]).validate(response.json())
    if status == 401:
        assert response.headers["WWW-Authenticate"] == "Bearer"


def test_page_examples_match_exactly_one_declared_shape(document):
    content = document["paths"][ROWS]["get"]["responses"]["200"]["content"]["application/json"]
    validator = OAS31Validator(document).evolve(schema=content["schema"])
    for example in content["examples"].values():
        validator.validate(example["value"])
    both_positions = content["examples"]["cursor"]["value"] | {"offset": 0}
    assert not validator.is_valid(both_positions)
