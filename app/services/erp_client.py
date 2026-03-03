from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from xml.etree import ElementTree

import httpx
from fastapi import HTTPException


class ErpClient:
    def __init__(
        self,
        *,
        base_url: str,
        token_url: str,
        stored_procedure: str,
        username: str,
        password: str,
        grant_type: str,
        integrated: str,
        database: str,
        refresh_token: str,
        client_id: str,
        client_secret: str,
        scope: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token_url = token_url
        self._stored_procedure = stored_procedure.strip()
        self._username = username
        self._password = password
        self._grant_type = grant_type
        self._integrated = integrated
        self._database = database
        self._refresh_token = refresh_token
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._timeout = timeout_seconds

        self._access_token: str | None = None
        self._expires_at: datetime | None = None

    async def fetch_qc_payload(self, qc_udic_id: str) -> dict:
        token = await self._get_access_token()
        if not self._stored_procedure:
            raise HTTPException(status_code=500, detail="ERP_STORED_PROCEDURE is not configured")

        url = f"{self._base_url}/Utilities/InvokeCustom/{self._stored_procedure}"
        backoff = 0.5

        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.post(
                        url,
                        headers={
                            "Authorization": f"Bearer {token}",
                            "Accept": "application/json",
                            "Content-Type": "application/json",
                        },
                        json={"qcUdicId": qc_udic_id},
                    )

                if 500 <= response.status_code < 600:
                    raise HTTPException(status_code=503, detail=f"ERP fetch failed with {response.status_code}")

                if response.status_code >= 400:
                    raise HTTPException(status_code=502, detail=f"ERP fetch returned {response.status_code}")

                return self._normalize_fetch_payload(response, qc_udic_id)
            except (httpx.RequestError, httpx.TimeoutException, HTTPException) as exc:
                retryable = isinstance(exc, (httpx.RequestError, httpx.TimeoutException))
                if isinstance(exc, HTTPException) and exc.status_code == 503:
                    retryable = True

                if attempt == 2 or not retryable:
                    if isinstance(exc, HTTPException):
                        raise exc
                    raise HTTPException(status_code=503, detail="ERP fetch failed after retries") from exc

                await asyncio.sleep(backoff)
                backoff *= 2

        raise HTTPException(status_code=503, detail="ERP fetch failed after retries")

    async def _get_access_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._access_token and self._expires_at and now < self._expires_at:
            return self._access_token

        if not all([self._token_url, self._username, self._password, self._client_id, self._client_secret]):
            raise HTTPException(status_code=500, detail="ERP OAuth settings are incomplete")

        payload = {
            "Username": self._username,
            "Password": self._password,
            "grant_type": self._grant_type,
            "Integrated": self._integrated,
            "database": self._database,
            "Client_Id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._refresh_token:
            payload["refresh_token"] = self._refresh_token
        if self._scope:
            payload["scope"] = self._scope

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                self._token_url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

        if response.status_code >= 400:
            raise HTTPException(status_code=500, detail="Failed to obtain ERP access token")

        token_payload = response.json()
        token = token_payload.get("access_token")
        expires_in = int(token_payload.get("expires_in", 3600))

        if not token:
            raise HTTPException(status_code=500, detail="ERP token response missing access_token")

        self._access_token = token
        self._expires_at = now + timedelta(seconds=max(60, expires_in - 30))
        return self._access_token

    def _normalize_fetch_payload(self, response: httpx.Response, qc_udic_id: str) -> dict:
        body: Any
        try:
            body = response.json()
        except Exception:  # noqa: BLE001
            body = response.text

        xml_text = self._extract_xml_text(body)
        if xml_text is None:
            if isinstance(body, dict):
                return body
            raise HTTPException(status_code=502, detail="ERP fetch payload format is not supported")

        return self._parse_stored_procedure_xml(xml_text, qc_udic_id)

    @staticmethod
    def _extract_xml_text(payload: Any) -> str | None:
        if isinstance(payload, str):
            text = payload.strip()
            if "<NewDataSet" in text:
                return text
            return None

        if isinstance(payload, dict):
            for key in ("value", "Value", "result", "Result", "data", "Data"):
                value = payload.get(key)
                if isinstance(value, str) and "<NewDataSet" in value:
                    return value.strip()
            return None

        if isinstance(payload, list):
            for value in payload:
                if isinstance(value, str) and "<NewDataSet" in value:
                    return value.strip()
            return None

        return None

    @staticmethod
    def _parse_stored_procedure_xml(xml_text: str, expected_qc_udic_id: str) -> dict:
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError as exc:
            raise HTTPException(status_code=502, detail="ERP XML payload is invalid") from exc

        table = root.find(".//Table")
        if table is None:
            raise HTTPException(status_code=502, detail="ERP XML payload missing Table element")

        pep_udic_id = ErpClient._child_text(table, "pepUdicID")
        now_datetime = ErpClient._child_text(table, "NowDateTime")
        qc_records_raw = ErpClient._child_text(table, "qcRecords")
        if not qc_records_raw:
            raise HTTPException(status_code=502, detail="ERP XML payload missing qcRecords")

        try:
            qc_records = json.loads(qc_records_raw)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=502, detail="ERP qcRecords value is not valid JSON") from exc

        if isinstance(qc_records, dict):
            records = [qc_records]
        elif isinstance(qc_records, list):
            records = [r for r in qc_records if isinstance(r, dict)]
        else:
            records = []

        if not records:
            raise HTTPException(status_code=502, detail="ERP qcRecords JSON has no records")

        expected = expected_qc_udic_id.lower()
        selected = next(
            (r for r in records if str(r.get("qcUdicID", "")).lower() == expected),
            records[0],
        )

        reviewer_data = selected.get("reviewerData")
        disciplines: list[dict[str, str]] = []
        if isinstance(reviewer_data, list):
            for reviewer in reviewer_data:
                if not isinstance(reviewer, dict):
                    continue
                code = reviewer.get("disciplineID")
                name = reviewer.get("reviewerDiscipline")
                if code:
                    disciplines.append(
                        {
                            "erp_discipline_code": str(code).strip().upper(),
                            "discipline_name": str(name).strip() if name else str(code).strip().upper(),
                        }
                    )

        return {
            "pep_udic_id": pep_udic_id,
            "record_created_date": selected.get("recordCreatedDate") or now_datetime,
            "project_wbs": selected.get("projectWbs"),
            "submittal_name": selected.get("submittalName"),
            "submittal_date": selected.get("submittalDate"),
            "project_name": selected.get("projectName"),
            "client_name": selected.get("clientName"),
            "market": selected.get("market"),
            "location": selected.get("location"),
            "pm_name": selected.get("pmContact"),
            "pm_email": selected.get("projectManager"),
            "pp_name": selected.get("ppContact"),
            "pp_email": selected.get("projectProf"),
            "qc_udic_id": selected.get("qcUdicID") or expected_qc_udic_id,
            "disciplines": disciplines,
            "reviewer_data": reviewer_data if isinstance(reviewer_data, list) else [],
            "source_payload": {
                "xml_text": xml_text,
                "pepUdicID": pep_udic_id,
                "NowDateTime": now_datetime,
                "qcRecords": records,
            },
        }

    @staticmethod
    def _child_text(node: ElementTree.Element, child_name: str) -> str | None:
        child = node.find(child_name)
        if child is None or child.text is None:
            return None
        return child.text.strip()
