"""Official Polymarket Bridge API client and admission-controlled commands."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from quant.simulator.admission import (
    AdmissionOperation,
    AdmissionRequest,
    ExposureEffect,
    UnifiedAdmissionService,
)


class PolymarketBridgeClient:
    """Network wrapper for the official bridge.polymarket.com contract."""

    def __init__(
        self,
        *,
        base_url: str = "https://bridge.polymarket.com",
        session: requests.Session | None = None,
        proxy_url: str | None = None,
        timeout_seconds: float = 15.0,
        builder_code: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        if session is None:
            self.session.trust_env = False
            retry = Retry(
                total=3,
                connect=3,
                read=3,
                status=3,
                backoff_factor=0.5,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET"}),
                respect_retry_after_header=True,
            )
            adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.builder_code = builder_code

    @property
    def _proxies(self) -> dict[str, str] | None:
        if not self.proxy_url:
            return None
        return {"http": self.proxy_url, "https": self.proxy_url}

    @property
    def _builder_headers(self) -> dict[str, str]:
        return {"X-Builder-Code": self.builder_code} if self.builder_code else {}

    def supported_assets(self) -> Mapping[str, Any]:
        return self._request("GET", "/supported-assets")

    def quote(
        self,
        *,
        from_amount_base_unit: int | str,
        from_chain_id: str,
        from_token_address: str,
        recipient_address: str,
        to_chain_id: str,
        to_token_address: str,
    ) -> Mapping[str, Any]:
        if int(from_amount_base_unit) <= 0:
            raise ValueError("bridge quote amount must be positive")
        return self._request(
            "POST",
            "/quote",
            body={
                "fromAmountBaseUnit": str(from_amount_base_unit),
                "fromChainId": str(from_chain_id),
                "fromTokenAddress": from_token_address,
                "recipientAddress": recipient_address,
                "toChainId": str(to_chain_id),
                "toTokenAddress": to_token_address,
            },
        )

    def create_deposit_addresses(self, *, address: str) -> Mapping[str, Any]:
        return self._request(
            "POST", "/deposit", body={"address": address}, headers=self._builder_headers
        )

    def create_withdrawal_addresses(
        self,
        *,
        address: str,
        to_chain_id: str,
        to_token_address: str,
        recipient_address: str,
    ) -> Mapping[str, Any]:
        return self._request(
            "POST",
            "/withdraw",
            body={
                "address": address,
                "toChainId": str(to_chain_id),
                "toTokenAddress": to_token_address,
                "recipientAddr": recipient_address,
            },
            headers=self._builder_headers,
        )

    def transaction_status_page(
        self,
        *,
        address: str,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Mapping[str, Any]:
        if not 1 <= limit <= 100:
            raise ValueError("bridge status limit must be between 1 and 100")
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        return self._request("GET", f"/status/{address}", params=params)

    def transaction_history(
        self, *, address: str, max_pages: int = 10_000
    ) -> tuple[Mapping[str, Any], ...]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        rows: dict[str, Mapping[str, Any]] = {}
        for _ in range(max_pages):
            page = self.transaction_status_page(address=address, cursor=cursor)
            transactions = page.get("transactions")
            if not isinstance(transactions, list):
                raise TypeError("bridge status endpoint returned invalid transactions")
            for transaction in transactions:
                if not isinstance(transaction, Mapping):
                    raise TypeError("bridge status transaction must be an object")
                identity = _bridge_identity(transaction)
                rows[identity] = dict(transaction)
            next_cursor = page.get("nextCursor")
            if next_cursor is None:
                return tuple(rows[key] for key in sorted(rows))
            cursor = str(next_cursor)
            if cursor in seen_cursors:
                raise RuntimeError("bridge status cursor loop detected")
            seen_cursors.add(cursor)
        raise RuntimeError("bridge status exceeded max_pages")

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            json=dict(body) if body is not None else None,
            params=dict(params) if params is not None else None,
            headers=dict(headers or {}),
            proxies=self._proxies,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise TypeError("bridge endpoint returned a non-object payload")
        return dict(payload)


class BridgeCommandService:
    """User command boundary; address creation is dry-run unless explicitly executed."""

    def __init__(
        self,
        *,
        client: PolymarketBridgeClient,
        admission: UnifiedAdmissionService,
    ) -> None:
        self.client = client
        self.admission = admission

    def supported_assets(self, *, account_id: str, strategy_id: str | None = None) -> Mapping[str, Any]:
        self._admit(
            request_id=f"bridge-assets:{account_id}",
            operation=AdmissionOperation.READ,
            account_id=account_id,
            strategy_id=strategy_id,
            observed_at=datetime.now(timezone.utc),
            metadata={"command": "SUPPORTED_ASSETS"},
        )
        return self.client.supported_assets()

    def quote(
        self,
        *,
        account_id: str,
        strategy_id: str | None,
        direction: str,
        request: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        selected = direction.strip().upper()
        operation = (
            AdmissionOperation.BRIDGE_DEPOSIT
            if selected == "DEPOSIT"
            else AdmissionOperation.BRIDGE_WITHDRAWAL
        )
        self._admit(
            request_id=f"bridge-quote:{account_id}:{_request_identity(request)}",
            operation=operation,
            account_id=account_id,
            strategy_id=strategy_id,
            observed_at=datetime.now(timezone.utc),
            metadata={"command": "QUOTE", "direction": selected},
        )
        return self.client.quote(
            from_amount_base_unit=request["fromAmountBaseUnit"],
            from_chain_id=str(request["fromChainId"]),
            from_token_address=str(request["fromTokenAddress"]),
            recipient_address=str(request["recipientAddress"]),
            to_chain_id=str(request["toChainId"]),
            to_token_address=str(request["toTokenAddress"]),
        )

    def deposit_address(
        self,
        *,
        account_id: str,
        strategy_id: str | None,
        wallet_address: str,
        execute: bool = False,
    ) -> Mapping[str, Any]:
        self._admit(
            request_id=f"bridge-deposit-address:{account_id}:{wallet_address.lower()}",
            operation=AdmissionOperation.BRIDGE_DEPOSIT,
            account_id=account_id,
            strategy_id=strategy_id,
            observed_at=datetime.now(timezone.utc),
            metadata={"command": "CREATE_DEPOSIT_ADDRESS", "execute": execute},
        )
        if not execute:
            return {
                "status": "DRY_RUN",
                "operation": "CREATE_DEPOSIT_ADDRESS",
                "wallet_address": wallet_address,
            }
        return self.client.create_deposit_addresses(address=wallet_address)

    def withdrawal_address(
        self,
        *,
        account_id: str,
        strategy_id: str | None,
        wallet_address: str,
        to_chain_id: str,
        to_token_address: str,
        recipient_address: str,
        execute: bool = False,
    ) -> Mapping[str, Any]:
        self._admit(
            request_id=(
                f"bridge-withdraw-address:{account_id}:{wallet_address.lower()}:"
                f"{to_chain_id}:{to_token_address.lower()}:{recipient_address.lower()}"
            ),
            operation=AdmissionOperation.BRIDGE_WITHDRAWAL,
            account_id=account_id,
            strategy_id=strategy_id,
            observed_at=datetime.now(timezone.utc),
            metadata={"command": "CREATE_WITHDRAWAL_ADDRESS", "execute": execute},
        )
        if not execute:
            return {
                "status": "DRY_RUN",
                "operation": "CREATE_WITHDRAWAL_ADDRESS",
                "wallet_address": wallet_address,
                "recipient_address": recipient_address,
                "to_chain_id": str(to_chain_id),
                "to_token_address": to_token_address,
            }
        return self.client.create_withdrawal_addresses(
            address=wallet_address,
            to_chain_id=to_chain_id,
            to_token_address=to_token_address,
            recipient_address=recipient_address,
        )

    def status(
        self,
        *,
        account_id: str,
        strategy_id: str | None,
        bridge_address: str,
    ) -> tuple[Mapping[str, Any], ...]:
        self._admit(
            request_id=f"bridge-status:{account_id}:{bridge_address}",
            operation=AdmissionOperation.RECONCILIATION,
            account_id=account_id,
            strategy_id=strategy_id,
            observed_at=datetime.now(timezone.utc),
            metadata={"command": "STATUS", "bridge_address": bridge_address},
        )
        return self.client.transaction_history(address=bridge_address)

    def recovery(
        self,
        *,
        account_id: str,
        strategy_id: str | None,
        bridge_address: str,
        transaction_hash: str,
    ) -> Mapping[str, Any]:
        rows = self.status(
            account_id=account_id,
            strategy_id=strategy_id,
            bridge_address=bridge_address,
        )
        selected = transaction_hash.lower()
        matches = [
            row
            for row in rows
            if selected
            in {
                str(row.get("txHash") or "").lower(),
                str(row.get("fromTxHash") or "").lower(),
                str(row.get("toTxHash") or "").lower(),
            }
        ]
        return {
            "status": "FOUND" if matches else "NO_OFFICIAL_EVIDENCE",
            "transaction_hash": transaction_hash,
            "matches": matches,
            "action": (
                "RECONCILE_OFFICIAL_STATUS"
                if matches
                else "DO_NOT_CREDIT_CASH_CONTACT_BRIDGE_SUPPORT"
            ),
        }

    def _admit(
        self,
        *,
        request_id: str,
        operation: AdmissionOperation,
        account_id: str,
        strategy_id: str | None,
        observed_at: datetime,
        metadata: Mapping[str, Any],
    ) -> None:
        decision = self.admission.decide(
            AdmissionRequest(
                request_id=f"{request_id}:{observed_at.isoformat()}",
                operation=operation,
                account_id=account_id,
                strategy_id=strategy_id,
                exposure_effect=ExposureEffect.NEUTRAL,
                observed_at=observed_at,
                metadata=metadata,
            )
        )
        if not decision.allowed:
            raise ValueError(
                "Bridge command rejected by Unified Admission: "
                + ",".join(decision.reason_codes)
            )


def _bridge_identity(row: Mapping[str, Any]) -> str:
    for key in ("id", "txHash", "fromTxHash", "toTxHash", "quoteId"):
        if row.get(key):
            return f"{key}:{row[key]}"
    return _request_identity(row)


def _request_identity(payload: Mapping[str, Any]) -> str:
    import hashlib
    import json

    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
