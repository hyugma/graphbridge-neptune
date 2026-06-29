from datetime import date, datetime
from decimal import Decimal
import json

import pytest
import requests

from graphbridge_neptune.client import NeptuneClient


class DummyCredentials:
    graph_scheme = "https"
    graph_host = "neptune.local"
    graph_port = 8182
    graph_database = ""
    graph_user = ""
    graph_password = ""
    connection_timeout = 30

    @property
    def graph_uri(self):
        return f"{self.graph_scheme}://{self.graph_host}:{self.graph_port}"


class EmptyGraphUriCredentials(DummyCredentials):
    @property
    def graph_uri(self):
        return ""


class MissingHostCredentials(DummyCredentials):
    graph_host = ""


class HostWithSchemeCredentials(DummyCredentials):
    graph_host = "https://neptune.local:8182"


class FakeResponse:
    def __init__(self, body, status_code=200, text=""):
        self.body = body
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"{self.status_code} Error", response=self
            )
        pass

    def json(self):
        return self.body


class FakeSession:
    def __init__(self):
        self.posts = []
        self.closed = False
        self.auth = None

    def post(self, endpoint, data, timeout):
        self.posts.append((endpoint, data, timeout))
        return FakeResponse({"results": [{"ok": 1}]})

    def close(self):
        self.closed = True


def test_client_posts_open_cypher_query(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    response, records = client.execute_cypher(
        "MATCH (n {name: $name}) RETURN n",
        {"name": "Ada"},
    )

    assert records == [{"ok": 1}]
    assert response.rows_affected == 1
    assert fake_session.posts == [
        (
            "https://neptune.local:8182/openCypher",
            {
                "query": "MATCH (n {name: $name}) RETURN n",
                "parameters": '{"name":"Ada"}',
            },
            30,
        )
    ]


def test_translates_neo4j_label_metadata_query(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    client.execute_cypher("CALL db.labels() YIELD label RETURN label")

    assert fake_session.posts[0][1]["query"] == (
        "MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label"
    )


def test_translates_neo4j_relationship_metadata_query(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    client.execute_cypher(
        "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
    )

    assert fake_session.posts[0][1]["query"] == (
        "MATCH ()-[r]->() RETURN DISTINCT type(r) AS relationshipType"
    )


def test_client_posts_batch_data(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    response = client.execute_cypher_batch(
        "UNWIND $batch AS row RETURN row",
        [{"amount": Decimal("1.25")}, {"amount": Decimal("2.50")}],
        batch_size=1,
    )

    assert response.rows_affected == 2
    assert len(fake_session.posts) == 2
    assert json.loads(fake_session.posts[0][1]["parameters"]) == {
        "batch": [{"amount": 1.25}]
    }
    assert json.loads(fake_session.posts[1][1]["parameters"]) == {
        "batch": [{"amount": 2.5}]
    }


def test_client_renders_batch_rows_as_literals_for_neptune(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    response = client.execute_cypher_batch(
        """
        UNWIND $batch AS row
        MERGE (n:Company { company_id: row.company_id })
        SET n.company_name = row.company_name,
            n.rank = row.rank,
            n._dbt_loaded_at = datetime()
        """,
        [{"company_id": 'a"b', "company_name": "Ada", "rank": 1}],
    )

    assert response.rows_affected == 1
    assert fake_session.posts[0][1] == {
        "query": """MERGE (n:Company { `company_id`: "a\\"b" })
        SET n.`company_name` = "Ada",
            n.`rank` = 1,
            n.`_dbt_loaded_at` = datetime()
        """
    }


def test_client_renders_unicode_literals_without_regex_escape_errors(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    client.execute_cypher_batch(
        """
        UNWIND $batch AS row
        MERGE (n:CEO { ceo_name: row.ceo_name })
        SET n._dbt_loaded_at = datetime()
        """,
        [{"ceo_name": "佐藤"}],
    )

    assert fake_session.posts[0][1] == {
        "query": """MERGE (n:CEO { `ceo_name`: "佐藤" })
        SET n.`_dbt_loaded_at` = datetime()
        """
    }


def test_metadata_identifiers_are_rendered_as_string_property(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    client.execute_cypher(
        """
        MERGE (m:_dbt_model {name: 'company_node'})
        SET m.identifiers = ["Company"],
            m.materialization = 'node'
        """
    )

    assert fake_session.posts[0][1] == {
        "query": """
        MERGE (m:_dbt_model {name: 'company_node'})
        SET m.identifiers = "Company",
            m.materialization = 'node'
        """
    }


def test_verify_connectivity_runs_simple_query(monkeypatch):
    fake_session = FakeSession()
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    client = NeptuneClient(DummyCredentials())
    client.verify_connectivity()
    client.close()

    assert fake_session.posts[0][1] == {"query": "RETURN 1 AS ok"}
    assert fake_session.closed is True


def test_endpoint_uses_host_components_even_when_graph_uri_is_empty():
    endpoint = NeptuneClient._open_cypher_endpoint(EmptyGraphUriCredentials())

    assert endpoint == "https://neptune.local:8182/openCypher"


def test_endpoint_accepts_host_with_scheme():
    endpoint = NeptuneClient._open_cypher_endpoint(HostWithSchemeCredentials())

    assert endpoint == "https://neptune.local:8182/openCypher"


def test_endpoint_requires_host():
    with pytest.raises(ValueError, match="graph_host is required"):
        NeptuneClient._open_cypher_endpoint(MissingHostCredentials())


def test_sanitize_record_converts_json_unsafe_values():
    record = {
        "amount": Decimal("10.5"),
        "created_at": datetime(2026, 6, 29, 10, 30),
        "created_on": date(2026, 6, 29),
        "nested": {"value": Decimal("2.5")},
        "items": [Decimal("1.5")],
    }

    assert NeptuneClient._sanitize_record(record) == {
        "amount": 10.5,
        "created_at": "2026-06-29T10:30:00",
        "created_on": "2026-06-29",
        "nested": {"value": 2.5},
        "items": [1.5],
    }


def test_extract_records_accepts_common_response_shapes():
    assert NeptuneClient._extract_records({"results": [{"a": 1}]}) == [{"a": 1}]
    assert NeptuneClient._extract_records({"records": [{"b": 2}]}) == [{"b": 2}]
    assert NeptuneClient._extract_records({"result": {"c": 3}}) == [{"c": 3}]
