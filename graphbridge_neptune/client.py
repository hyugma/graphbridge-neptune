"""Amazon Neptune openCypher client for dbt-graph-bridge."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

import requests
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.graphbridge.graph_engines import GraphEngineClient


class NeptuneClient(GraphEngineClient):
    """Amazon Neptune backend using the HTTPS openCypher endpoint."""

    def __init__(self, credentials: Any):
        self._endpoint = self._open_cypher_endpoint(credentials)
        self._timeout = getattr(credentials, "connection_timeout", 30)
        self._session = requests.Session()

        graph_user = getattr(credentials, "graph_user", "") or ""
        graph_password = getattr(credentials, "graph_password", "") or ""
        if graph_user or graph_password:
            self._session.auth = (graph_user, graph_password)

    def verify_connectivity(self) -> None:
        self.execute_cypher("RETURN 1 AS ok")

    def execute_cypher(
        self,
        cypher: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> Tuple[AdapterResponse, list]:
        records = self._post_open_cypher(cypher, parameters or {})
        response = AdapterResponse(
            _message=f"OK ({len(records)})",
            code="OK",
            rows_affected=len(records),
        )
        return response, records

    def execute_cypher_batch(
        self,
        cypher: str,
        batch_data: list,
        batch_size: int = 10000,
        database: Optional[str] = None,
    ) -> AdapterResponse:
        if not batch_data:
            return AdapterResponse(_message="OK (0)", code="OK", rows_affected=0)

        total_rows = 0
        safe_batch_size = max(int(batch_size or 10000), 1)
        for start in range(0, len(batch_data), safe_batch_size):
            chunk = batch_data[start : start + safe_batch_size]
            sanitized_batch = [self._sanitize_record(row) for row in chunk]
            self._post_open_cypher(cypher, {"batch": sanitized_batch})
            total_rows += len(chunk)

        return AdapterResponse(
            _message=f"OK (total: {total_rows})",
            code="OK",
            rows_affected=total_rows,
        )

    def close(self) -> None:
        self._session.close()

    def _post_open_cypher(
        self,
        cypher: str,
        parameters: Dict[str, Any],
    ) -> list:
        payload = {"query": cypher}
        if parameters:
            payload["parameters"] = json.dumps(
                self._sanitize_value(parameters),
                separators=(",", ":"),
            )

        response = self._session.post(
            self._endpoint,
            data=payload,
            timeout=self._timeout,
        )
        response.raise_for_status()
        body = response.json()
        return self._extract_records(body)

    @staticmethod
    def _open_cypher_endpoint(credentials: Any) -> str:
        graph_uri = getattr(credentials, "graph_uri", "")
        if not graph_uri:
            scheme = getattr(credentials, "graph_scheme", "https")
            host = getattr(credentials, "graph_host")
            port = getattr(credentials, "graph_port", 8182)
            graph_uri = f"{scheme}://{host}:{port}"
        return urljoin(graph_uri.rstrip("/") + "/", "openCypher")

    @staticmethod
    def _extract_records(body: Any) -> list:
        if not isinstance(body, dict):
            return []

        for key in ("results", "records"):
            value = body.get(key)
            if isinstance(value, list):
                return value

        if "result" in body:
            value = body["result"]
            return value if isinstance(value, list) else [value]

        return []

    @staticmethod
    def _sanitize_record(record: Dict[str, Any]) -> Dict[str, Any]:
        return {key: NeptuneClient._sanitize_value(value) for key, value in record.items()}

    @staticmethod
    def _sanitize_value(value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, dict):
            return {
                key: NeptuneClient._sanitize_value(nested_value)
                for key, nested_value in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [NeptuneClient._sanitize_value(item) for item in value]
        return value
