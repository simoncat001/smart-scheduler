from __future__ import annotations

from dataclasses import dataclass
import http.client
import json
from typing import Any
from urllib.parse import quote, urlencode, urljoin, urlsplit

from app.config import Settings, get_settings


class ManageDbClientError(RuntimeError):
    pass


@dataclass(slots=True)
class ManageDbRecord:
    record_id: str
    payload: dict[str, Any]
    source_url: str
    base_url: str | None = None


@dataclass(slots=True)
class ManageDbSearchResult:
    total: int
    items: list[ManageDbRecord]


@dataclass(slots=True)
class ManageDbCredentials:
    base_url: str
    username: str
    password: str


class ManageDbClient:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._access_token: str | None = None

    def fetch_development_data(
        self,
        *,
        record_id: str | None,
        standard_ref: dict[str, Any] | None = None,
    ) -> ManageDbRecord | None:
        reference = standard_ref if isinstance(standard_ref, dict) else {}
        resolved_url = self._resolve_lookup_url(record_id=record_id, standard_ref=reference)
        if not resolved_url:
            return None

        payload = self._request_json("GET", resolved_url)
        return self._coerce_record(
            payload,
            source_url=resolved_url,
            default_record_id=record_id or reference.get("record_id"),
            base_url=self._resolve_base_url(reference, resolved_url),
        )

    def download_bytes(
        self,
        resource: str,
        *,
        base_url: str | None = None,
        retry_on_unauthorized: bool = True,
    ) -> bytes:
        url = self._resolve_resource_url(resource, base_url=base_url)
        if not url:
            raise ManageDbClientError(f"invalid manage_db resource reference: {resource!r}")

        headers: dict[str, str] = {}
        token = self._ensure_access_token(url_hint=url)
        if token:
            headers["Authorization"] = f"Bearer {token}"

        status_code, _, body = self._perform_request(
            "GET",
            url,
            headers=headers,
        )
        if status_code == 401 and retry_on_unauthorized and headers.get("Authorization"):
            self._access_token = None
            return self.download_bytes(
                resource,
                base_url=base_url,
                retry_on_unauthorized=False,
            )
        if status_code >= 400:
            detail = body.decode("utf-8", errors="replace")
            raise ManageDbClientError(
                f"manage_db download failed: {status_code} {detail}"
            )
        return body

    def search_development_data(
        self,
        *,
        conditions: list[dict[str, Any]] | None = None,
        logic: str = "and",
        template_id: str | None = None,
        data_category: str | None = None,
        template_type: str | None = None,
        created_by: str | None = None,
        review_statuses: list[str] | None = None,
        include_deleted: bool = False,
        deleted_only: bool = False,
        start: int = 0,
        size: int = 50,
    ) -> ManageDbSearchResult:
        base_url = self._resolve_base_url({})
        if not base_url:
            raise ManageDbClientError(
                "manage_db base URL is not configured; set SMART_CHEDULER_MANAGE_DB_BASE_URL"
            )

        payload = self._request_payload(
            "POST",
            urljoin(base_url.rstrip("/") + "/", "api/development-data/json-search"),
            json_data={
                "conditions": conditions or [],
                "logic": logic,
                "template_id": template_id,
                "data_category": data_category,
                "template_type": template_type,
                "created_by": created_by,
                "review_statuses": review_statuses,
                "include_deleted": include_deleted,
                "deleted_only": deleted_only,
                "start": start,
                "size": size,
            },
        )
        if not isinstance(payload, dict):
            raise ManageDbClientError("unexpected manage_db search response shape")

        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            raise ManageDbClientError("manage_db search response has no items list")

        items: list[ManageDbRecord] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            try:
                items.append(
                    self._coerce_record(
                        item,
                        source_url=urljoin(
                            base_url.rstrip("/") + "/",
                            f"api/development-data/{item.get('id')}",
                        ),
                        default_record_id=item.get("id"),
                        base_url=base_url,
                    )
                )
            except ManageDbClientError:
                continue

        total = payload.get("total", len(items))
        try:
            total_count = int(total)
        except (TypeError, ValueError):
            total_count = len(items)

        return ManageDbSearchResult(total=total_count, items=items)

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        form_data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        retry_on_unauthorized: bool = True,
    ) -> dict[str, Any]:
        parsed = self._request_payload(
            method,
            url,
            form_data=form_data,
            json_data=json_data,
            retry_on_unauthorized=retry_on_unauthorized,
        )
        if not isinstance(parsed, dict):
            raise ManageDbClientError(f"manage_db returned unexpected JSON from {url}")
        return parsed

    def _request_payload(
        self,
        method: str,
        url: str,
        *,
        form_data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        retry_on_unauthorized: bool = True,
    ) -> Any:
        if form_data is not None and json_data is not None:
            raise ManageDbClientError("manage_db request cannot use form_data and json_data together")

        headers: dict[str, str] = {"Accept": "application/json"}
        body: bytes | None = None
        if form_data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            body = urlencode(form_data).encode("utf-8")
        if json_data is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(json_data, ensure_ascii=False).encode("utf-8")

        if form_data is None:
            token = self._ensure_access_token(url_hint=url)
            if token:
                headers["Authorization"] = f"Bearer {token}"

        status_code, _, raw = self._perform_request(
            method,
            url,
            headers=headers,
            body=body,
        )
        if status_code == 401 and form_data is None and retry_on_unauthorized and headers.get("Authorization"):
            self._access_token = None
            return self._request_payload(
                method,
                url,
                form_data=form_data,
                json_data=json_data,
                retry_on_unauthorized=False,
            )
        if status_code >= 400:
            detail = raw.decode("utf-8", errors="replace")
            raise ManageDbClientError(
                f"manage_db request failed: {status_code} {detail}"
            )

        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise ManageDbClientError(f"manage_db returned invalid JSON from {url}") from exc

    def _ensure_access_token(self, *, url_hint: str | None = None) -> str | None:
        if self._access_token:
            return self._access_token
        credentials = self._resolve_credentials(url_hint=url_hint)
        if credentials is None:
            return None
        return self._login(credentials)

    def _login(self, credentials: ManageDbCredentials) -> str:
        token_url = urljoin(credentials.base_url.rstrip("/") + "/", "api/token")
        payload = self._request_json(
            "POST",
            token_url,
            form_data={
                "grant_type": "password",
                "username": credentials.username,
                "password": credentials.password,
            },
            retry_on_unauthorized=False,
        )
        access_token = payload.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise ManageDbClientError(f"manage_db login did not return access_token: {token_url}")
        self._access_token = access_token
        return access_token

    def _perform_request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ManageDbClientError(f"unsupported manage_db URL: {url}")
        path = quote(parsed.path or "/", safe="/:@%+-._~")
        if parsed.query:
            path = f"{path}?{quote(parsed.query, safe='=&%/:,+-._~')}"
        port = parsed.port
        timeout = max(1, self.settings.manage_db_timeout_seconds)
        connection_class = (
            http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        )
        try:
            connection = connection_class(parsed.hostname, port=port, timeout=timeout)
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            response_body = response.read()
            response_headers = {key: value for key, value in response.getheaders()}
            status = int(response.status)
            connection.close()
            return status, response_headers, response_body
        except OSError as exc:
            raise ManageDbClientError(f"manage_db request failed: {exc}") from exc

    def _resolve_credentials(
        self,
        *,
        url_hint: str | None = None,
    ) -> ManageDbCredentials | None:
        hint_base_url = self._normalize_base_url(url_hint)
        configured_base_url = self._normalize_base_url(self.settings.manage_db_base_url)
        if (
            configured_base_url
            and self.settings.manage_db_username
            and self.settings.manage_db_password
        ):
            return ManageDbCredentials(
                base_url=configured_base_url,
                username=self.settings.manage_db_username,
                password=self.settings.manage_db_password,
            )

        if hint_base_url and self.settings.manage_db_username and self.settings.manage_db_password:
            return ManageDbCredentials(
                base_url=hint_base_url,
                username=self.settings.manage_db_username,
                password=self.settings.manage_db_password,
            )
        return None

    @staticmethod
    def _normalize_base_url(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        parsed = urlsplit(value.strip())
        if not parsed.scheme or not parsed.netloc:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"

    def _resolve_lookup_url(
        self,
        *,
        record_id: str | None,
        standard_ref: dict[str, Any],
    ) -> str | None:
        lookup_url = standard_ref.get("lookup_url")
        if isinstance(lookup_url, str) and lookup_url:
            return lookup_url

        resource_path = standard_ref.get("resource_path")
        base_url = self._resolve_base_url(standard_ref)
        if isinstance(resource_path, str) and resource_path and base_url:
            return urljoin(base_url.rstrip("/") + "/", resource_path.lstrip("/"))

        effective_record_id = record_id or standard_ref.get("record_id")
        if effective_record_id in (None, "") or not base_url:
            return None
        return urljoin(
            base_url.rstrip("/") + "/",
            f"api/development-data/{effective_record_id}",
        )

    def _resolve_base_url(
        self,
        standard_ref: dict[str, Any],
        lookup_url: str | None = None,
    ) -> str | None:
        explicit = standard_ref.get("base_url")
        if isinstance(explicit, str) and explicit:
            return explicit
        configured = self.settings.manage_db_base_url
        if configured:
            return configured
        if lookup_url:
            parsed = urlsplit(lookup_url)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"
        return None

    @staticmethod
    def _resolve_resource_url(resource: str, *, base_url: str | None) -> str | None:
        value = str(resource or "").strip()
        if not value:
            return None
        parsed = urlsplit(value)
        if parsed.scheme and parsed.netloc:
            return value
        if value.startswith("/"):
            if not base_url:
                return None
            return urljoin(base_url.rstrip("/") + "/", value.lstrip("/"))
        if base_url:
            return urljoin(base_url.rstrip("/") + "/", value)
        return value

    @staticmethod
    def _coerce_record(
        payload: dict[str, Any],
        *,
        source_url: str,
        default_record_id: Any,
        base_url: str | None,
    ) -> ManageDbRecord:
        if not isinstance(payload, dict):
            raise ManageDbClientError(f"unexpected manage_db response shape from {source_url}")

        raw_data = payload.get("data")
        record_payload = raw_data if isinstance(raw_data, dict) else {}
        effective_record_id = payload.get("id") or default_record_id
        if effective_record_id in (None, ""):
            raise ManageDbClientError(f"manage_db response from {source_url} has no record id")

        return ManageDbRecord(
            record_id=str(effective_record_id),
            payload=record_payload,
            source_url=source_url,
            base_url=base_url,
        )
