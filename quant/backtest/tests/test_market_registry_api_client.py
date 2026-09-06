from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from quant.market.api_client import (
    MarketRegistryApiConfig,
    PolymarketApiClient,
    book_probe_from_payload,
    is_active_open_gamma_market,
    is_open_book_enabled_clob_market,
    markets_from_gamma_events,
    tokens_from_clob_market,
    tokens_from_gamma_market,
    tokens_from_ws_new_market,
)
from quant.market.service import _is_terminal_clob_cursor


def test_tokens_from_gamma_market_accepts_json_string_token_mapping() -> None:
    tokens = tokens_from_gamma_market(
        {
            "id": "gamma-1",
            "slug": "demo",
            "question": "Demo?",
            "conditionId": "0xabc",
            "active": True,
            "closed": False,
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["Up", "Down"]',
        }
    )

    assert [token.asset_id for token in tokens] == ["111", "222"]
    assert [token.outcome_name for token in tokens] == ["UP", "DOWN"]
    assert all(token.condition_id == "0xabc" for token in tokens)
    assert all(token.token_count == 2 for token in tokens)
    assert all(token.source == "gamma_api" for token in tokens)


def test_native_new_market_event_immediately_yields_subscription_tokens() -> None:
    tokens = tokens_from_ws_new_market(
        {
            "event_type": "new_market",
            "id": "123456",
            "question": "Will the event happen?",
            "market": "0xcondition",
            "slug": "will-the-event-happen",
            "assets_ids": ["yes-token", "no-token"],
            "outcomes": ["Yes", "No"],
            "timestamp": "1782753357257",
        }
    )

    assert [token.asset_id for token in tokens] == ["yes-token", "no-token"]
    assert [token.outcome_name for token in tokens] == ["YES", "NO"]
    assert all(token.condition_id == "0xcondition" for token in tokens)
    assert all(token.token_count == 2 for token in tokens)
    assert all(token.source == "ws_new_market" for token in tokens)


def test_native_new_market_event_accepts_stream_envelope() -> None:
    tokens = tokens_from_ws_new_market(
        {
            "topic": "market",
            "type": "new_market",
            "payload": {
                "id": "123456",
                "market": "0xcondition",
                "token_ids": ["yes-token", "no-token"],
                "outcomes": ["Yes", "No"],
            },
        }
    )

    assert [token.asset_id for token in tokens] == ["yes-token", "no-token"]


def test_gamma_explicit_accepting_book_is_trusted_open_source() -> None:
    tokens = tokens_from_gamma_market(
        {
            "id": "gamma-open",
            "conditionId": "0xopen",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "enableOrderBook": True,
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["YES", "NO"]',
        }
    )

    assert all(token.source == "gamma_api_open_book" for token in tokens)


def test_active_gamma_filter_rejects_closed_nested_event_markets() -> None:
    assert is_active_open_gamma_market({"active": True, "closed": False}) is True
    assert is_active_open_gamma_market({"active": True, "closed": True}) is False
    assert is_active_open_gamma_market({"active": False, "closed": False}) is False
    assert is_active_open_gamma_market({"archived": True}) is False


def test_gamma_keyset_pagination_retries_the_same_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    client = PolymarketApiClient(
        MarketRegistryApiConfig(proxy_mode="direct", max_retries=0, backoff_seconds=0),
        session=_FakeSession([]),
    )
    calls: list[str | None] = []

    def fetch_page(*, limit: int, cursor: str | None = None) -> dict[str, object]:
        calls.append(cursor)
        if len(calls) == 1:
            raise TimeoutError("temporary timeout")
        return {"markets": [{"id": "market-1"}], "next_cursor": None}

    monkeypatch.setattr(client, "fetch_gamma_keyset_markets", fetch_page)

    rows = client.fetch_gamma_keyset_markets_all(page_size=100)

    assert rows == [{"id": "market-1"}]
    assert calls == [None, None]
    assert client.discovery_stats["gamma_markets_page_retries"] == 1


def test_recent_gamma_pagination_stops_after_durable_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    client = PolymarketApiClient(
        MarketRegistryApiConfig(proxy_mode="direct", backoff_seconds=0),
        session=_FakeSession([]),
    )
    cursors: list[str | None] = []

    def fetch_page(
        *,
        limit: int,
        cursor: str | None = None,
        closed: bool | None = None,
    ) -> dict[str, object]:
        cursors.append(cursor)
        if cursor is None:
            return {
                "markets": [{"id": "new", "updatedAt": "2026-07-23T12:10:00Z"}],
                "next_cursor": "page-2",
            }
        return {
            "markets": [{"id": "old", "updatedAt": "2026-07-23T11:59:00Z"}],
            "next_cursor": "page-3",
        }

    monkeypatch.setattr(client, "fetch_gamma_recent_keyset_markets", fetch_page)

    rows, reached = client.fetch_gamma_recent_keyset_markets_since(
        updated_since=datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc),
        max_markets=100,
    )

    assert [row["id"] for row in rows] == ["new", "old"]
    assert reached is True
    assert cursors == [None, "page-2"]


def test_api_market_tokens_ignores_closed_markets_nested_in_active_events() -> None:
    session = _FakeSession(
        [
            _FakeResponse({"markets": [], "next_cursor": None}),
            _FakeResponse([]),
            _FakeResponse(
                {
                    "events": [
                        {
                            "active": True,
                            "closed": False,
                            "markets": [
                                {
                                    "id": "closed-child",
                                    "active": True,
                                    "closed": True,
                                    "conditionId": "0xclosed",
                                    "clobTokenIds": '["old-yes", "old-no"]',
                                },
                                {
                                    "id": "open-child",
                                    "active": True,
                                    "closed": False,
                                    "conditionId": "0xopen",
                                    "clobTokenIds": '["new-yes", "new-no"]',
                                },
                            ],
                        }
                    ],
                    "next_cursor": None,
                }
            ),
            _FakeResponse({"data": [], "next_cursor": None}),
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    tokens = client.fetch_api_market_tokens(limit=10, page_size=10)

    assert {token.asset_id for token in tokens} == {"new-yes", "new-no"}
    assert client.discovery_stats["gamma_non_active_rows_ignored"] == 1


def test_book_probe_marks_two_sided_book_ready() -> None:
    observed_at = datetime(2026, 7, 6, 8, 0, tzinfo=timezone.utc)
    result = book_probe_from_payload(
        "111",
        {
            "bids": [{"price": "0.41", "size": "10"}, {"price": "0.40", "size": "5"}],
            "asks": [{"price": "0.43", "size": "7"}, {"price": "0.44", "size": "3"}],
        },
        observed_at=observed_at,
    )

    assert result.ok is True
    assert result.book_status == "ok"
    assert result.book_quality == "READY_MEDIUM"
    assert result.best_bid == Decimal("0.41")
    assert result.best_ask == Decimal("0.43")
    assert result.bid_depth == Decimal("15")
    assert result.ask_depth == Decimal("10")


def test_book_probe_keeps_empty_book_out_of_execution_quality() -> None:
    result = book_probe_from_payload("111", {"bids": [], "asks": []}, observed_at=datetime.now(timezone.utc))

    assert result.ok is False
    assert result.book_status == "empty"
    assert result.book_quality == "EMPTY"


def test_gamma_keyset_fetch_all_follows_next_cursor() -> None:
    session = _FakeSession(
        [
            _FakeResponse({"markets": [{"id": "m1", "clobTokenIds": '["111"]'}], "next_cursor": "cursor-2"}),
            _FakeResponse({"markets": [{"id": "m2", "clobTokenIds": '["222"]'}], "next_cursor": None}),
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    rows = client.fetch_gamma_keyset_markets_all(page_size=1)

    assert [row["id"] for row in rows] == ["m1", "m2"]
    assert session.calls[1]["params"]["after_cursor"] == "cursor-2"
    assert session.calls[0]["params"]["active"] == "true"
    assert session.calls[0]["params"]["closed"] == "false"


def test_gamma_keyset_fetch_all_rejects_repeated_cursor() -> None:
    session = _FakeSession(
        [
            _FakeResponse({"events": [{"id": "e1"}], "next_cursor": "cursor-2"}),
            _FakeResponse({"events": [{"id": "e1"}], "next_cursor": "cursor-2"}),
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    with pytest.raises(RuntimeError, match="repeated its cursor"):
        client.fetch_gamma_keyset_events_all(page_size=1)

    assert session.calls[1]["params"]["after_cursor"] == "cursor-2"


def test_api_client_fails_over_to_process_local_profile8_proxy() -> None:
    session = _FailoverSession()
    client = PolymarketApiClient(
        MarketRegistryApiConfig(
            proxy_mode="explicit",
            proxy_url="http://127.0.0.1:17890",
            fallback_proxy_urls="http://127.0.0.1:18080",
            max_retries=0,
            backoff_seconds=0,
        ),
        session=session,
    )

    rows = client.fetch_gamma_active_markets(limit=1)

    assert rows == [{"id": "market-via-fallback"}]
    assert session.proxy_calls == [
        "http://127.0.0.1:17890",
        "http://127.0.0.1:18080",
    ]
    assert client.proxy_summary["active_proxy_is_fallback"] is True


def test_gamma_recent_markets_includes_closed_rows_and_orders_by_update() -> None:
    session = _FakeSession(
        [_FakeResponse([{"id": "closed-market", "active": True, "closed": True}])]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    rows = client.fetch_gamma_recent_markets(limit=800)

    assert rows == [{"id": "closed-market", "active": True, "closed": True}]
    assert session.calls[0]["params"] == {
        "limit": 500,
        "order": "updatedAt",
        "ascending": "false",
    }


def test_fetch_gamma_market_uses_direct_market_wrapper() -> None:
    session = _FakeSession([_FakeResponse({"id": "2617594", "closed": True})])
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    row = client.fetch_gamma_market("2617594")

    assert row == {"id": "2617594", "closed": True}
    assert session.calls[0]["url"].endswith("/markets/2617594")


def test_gamma_recent_keyset_fetch_all_tracks_closed_updates_without_active_filter() -> None:
    session = _FakeSession(
        [
            _FakeResponse({"markets": [{"id": "m1"}], "next_cursor": "cursor-2"}),
            _FakeResponse({"markets": [{"id": "m2"}], "next_cursor": None}),
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    rows = client.fetch_gamma_recent_keyset_markets_all(
        max_markets=2,
        page_size=1,
        closed=True,
    )

    assert [row["id"] for row in rows] == ["m1", "m2"]
    assert session.calls[0]["params"] == {
        "limit": 1,
        "order": "updatedAt",
        "ascending": "false",
        "closed": "true",
    }
    assert session.calls[1]["params"]["after_cursor"] == "cursor-2"
    assert "active" not in session.calls[0]["params"]


def test_api_market_tokens_uses_keyset_paths_without_duplicate_offset_fetches() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                {
                    "markets": [
                        {
                            "id": "m1",
                            "conditionId": "0xabc",
                            "slug": "direct-market",
                            "clobTokenIds": '["111", "222"]',
                            "outcomes": '["YES", "NO"]',
                        }
                    ],
                    "next_cursor": None,
                }
            ),
            _FakeResponse(
                {
                    "events": [
                        {
                            "active": True,
                            "closed": False,
                            "markets": [
                                {
                                    "id": "m2",
                                    "conditionId": "0xdef",
                                    "slug": "event-market",
                                    "clobTokenIds": '["333", "444"]',
                                    "outcomes": '["UP", "DOWN"]',
                                }
                            ],
                        }
                    ],
                    "next_cursor": None,
                }
            ),
            _FakeResponse({"data": [], "next_cursor": None}),
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    tokens = client.fetch_api_market_tokens(limit=10, page_size=10)

    assert [call["url"] for call in session.calls] == [
        "https://gamma-api.polymarket.com/markets/keyset",
        "https://gamma-api.polymarket.com/events/keyset",
        "https://clob.polymarket.com/markets",
    ]
    assert {token.asset_id for token in tokens} == {"111", "222", "333", "444"}
    assert client.discovery_errors == []


def test_api_market_tokens_falls_back_to_offset_when_keyset_fails() -> None:
    session = _FakeSession(
        [
            _FakeResponse({"error": "temporary"}, status_code=500),
            _FakeResponse({"error": "temporary"}, status_code=500),
            _FakeResponse(
                [
                    {
                        "id": "m1",
                        "conditionId": "0xabc",
                        "slug": "fallback-market",
                        "clobTokenIds": '["111", "222"]',
                        "outcomes": '["YES", "NO"]',
                    }
                ]
            ),
            _FakeResponse({"events": [], "next_cursor": None}),
            _FakeResponse([]),
            _FakeResponse({"data": [], "next_cursor": None}),
        ]
    )
    client = PolymarketApiClient(
        MarketRegistryApiConfig(proxy_mode="direct", max_retries=0, backoff_seconds=0),
        session=session,
    )

    tokens = client.fetch_api_market_tokens(limit=10, page_size=10)

    assert [call["url"] for call in session.calls] == [
        "https://gamma-api.polymarket.com/markets/keyset",
        "https://gamma-api.polymarket.com/markets/keyset",
        "https://gamma-api.polymarket.com/markets",
        "https://gamma-api.polymarket.com/events/keyset",
        "https://gamma-api.polymarket.com/events",
        "https://clob.polymarket.com/markets",
    ]
    assert {token.asset_id for token in tokens} == {"111", "222"}
    assert len(client.discovery_errors) == 1
    assert client.discovery_errors[0].startswith("markets_keyset:")


def test_clob_markets_are_filtered_locally_and_mapped_to_tokens() -> None:
    closed = {
        "condition_id": "0xclosed",
        "market_slug": "closed-market",
        "question": "Closed?",
        "active": True,
        "closed": True,
        "archived": False,
        "enable_order_book": True,
        "accepting_orders": True,
        "tokens": [{"token_id": "bad-1", "outcome": "YES"}],
    }
    no_book = {
        "condition_id": "0xnobook",
        "market_slug": "no-book-market",
        "question": "No book?",
        "active": True,
        "closed": False,
        "archived": False,
        "enable_order_book": False,
        "accepting_orders": True,
        "tokens": [{"token_id": "bad-2", "outcome": "YES"}],
    }
    open_book = {
        "condition_id": "0xopen",
        "market_slug": "open-book-market",
        "question": "Open book?",
        "active": True,
        "closed": False,
        "archived": False,
        "enable_order_book": True,
        "accepting_orders": True,
        "end_date_iso": "2026-07-10T00:00:00Z",
        "tokens": [
            {"token_id": "555", "outcome": "UP"},
            {"token_id": "666", "outcome": "DOWN"},
        ],
    }
    session = _FakeSession(
        [
            _FakeResponse({"data": [closed, no_book], "next_cursor": "cursor-2"}),
            _FakeResponse({"data": [open_book], "next_cursor": None}),
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    rows = client.fetch_clob_open_markets_all(max_markets=10)
    tokens = [token for row in rows for token in tokens_from_clob_market(row)]

    assert rows == [open_book]
    assert is_open_book_enabled_clob_market(open_book) is True
    assert [call["params"] for call in session.calls] == [{}, {"next_cursor": "cursor-2"}]
    assert {token.asset_id for token in tokens} == {"555", "666"}
    assert all(token.condition_id == "0xopen" for token in tokens)
    assert all(token.source == "clob_markets_api" for token in tokens)
    assert all(token.active is True and token.closed is False for token in tokens)


def test_clob_markets_stops_on_terminal_minus_one_cursor() -> None:
    session = _FakeSession(
        [_FakeResponse({"data": [{"condition_id": "old"}], "next_cursor": "LTE="})]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    rows = client.fetch_clob_open_markets_all(max_markets=10)

    assert rows == []
    assert len(session.calls) == 1
    assert _is_terminal_clob_cursor("LTE=") is True


def test_api_market_tokens_keeps_gamma_identity_when_clob_duplicates_asset() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                {
                    "markets": [
                        {
                            "id": "gamma-1",
                            "conditionId": "0xabc",
                            "slug": "gamma-market",
                            "question": "Gamma?",
                            "clobTokenIds": '["111", "222"]',
                            "outcomes": '["YES", "NO"]',
                        }
                    ],
                    "next_cursor": None,
                }
            ),
            _FakeResponse({"events": [], "next_cursor": None}),
            _FakeResponse(
                {
                    "data": [
                        {
                            "condition_id": "0xabc",
                            "market_slug": "clob-market",
                            "question": "CLOB?",
                            "active": True,
                            "closed": False,
                            "archived": False,
                            "enable_order_book": True,
                            "accepting_orders": True,
                            "tokens": [
                                {"token_id": "111", "outcome": "YES"},
                                {"token_id": "222", "outcome": "NO"},
                            ],
                        }
                    ],
                    "next_cursor": None,
                }
            ),
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    tokens = client.fetch_api_market_tokens(limit=10, page_size=10)

    assert {token.asset_id for token in tokens} == {"111", "222"}
    assert {token.source for token in tokens} == {"gamma_api"}
    assert session.calls[-1]["url"] == "https://clob.polymarket.com/markets"


def test_bulk_book_probe_posts_books_payload() -> None:
    observed_payload = [
        {
            "asset_id": "111",
            "bids": [{"price": "0.41", "size": "10"}],
            "asks": [{"price": "0.43", "size": "7"}],
        },
        {
            "asset_id": "222",
            "bids": [],
            "asks": [],
        },
    ]
    session = _FakeSession([_FakeResponse(observed_payload)])
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    results = client.probe_books(["111", "222"], batch_size=50)

    assert session.calls[0]["json"] == [{"token_id": "111"}, {"token_id": "222"}]
    assert results["111"].ok is True
    assert results["222"].ok is False
    assert results["222"].book_status == "empty"


def test_single_book_probe_uses_get_book_path() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                {
                    "asset_id": "111",
                    "bids": [{"price": "0.41", "size": "10"}],
                    "asks": [{"price": "0.43", "size": "7"}],
                }
            )
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    results = client.probe_books(["111"], batch_size=50)

    assert session.calls[0]["url"].endswith("/book")
    assert session.calls[0]["params"] == {"token_id": "111"}
    assert results["111"].ok is True


def test_bulk_book_probe_marks_missing_response_as_no_clob_book() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                [
                    {
                        "asset_id": "111",
                        "bids": [{"price": "0.41", "size": "10"}],
                        "asks": [{"price": "0.43", "size": "7"}],
                    }
                ]
            )
        ]
    )
    client = PolymarketApiClient(MarketRegistryApiConfig(proxy_mode="direct"), session=session)

    results = client.probe_books(["111", "missing"], batch_size=50)

    assert results["111"].ok is True
    assert results["missing"].book_status == "no_clob_book"
    assert results["missing"].book_quality == "NO_CLOB_BOOK"


def test_markets_from_gamma_events_flattens_nested_markets() -> None:
    rows = markets_from_gamma_events(
        [
            {
                "active": True,
                "closed": False,
                "markets": [
                    {
                        "id": "m1",
                        "conditionId": "0xabc",
                        "slug": "demo",
                        "question": "Demo?",
                        "clobTokenIds": '["111", "222"]',
                        "outcomes": '["YES", "NO"]',
                    }
                ],
            }
        ]
    )

    assert len(rows) == 1
    tokens = tokens_from_gamma_market(rows[0])
    assert [token.asset_id for token in tokens] == ["111", "222"]
    assert all(token.active is True for token in tokens)


def test_gamma_resolution_truth_is_mapped_without_treating_closed_as_winner() -> None:
    closed_tokens = tokens_from_gamma_market(
        {
            "id": "m-closed",
            "conditionId": "0xclosed",
            "active": False,
            "closed": True,
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["YES", "NO"]',
        }
    )
    resolved_tokens = tokens_from_gamma_market(
        {
            "id": "m-resolved",
            "conditionId": "0xresolved",
            "active": False,
            "closed": True,
            "resolved": True,
            "winningOutcome": "NO",
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["YES", "NO"]',
        }
    )

    assert all(token.resolved is False for token in closed_tokens)
    assert all(token.resolved is True for token in resolved_tokens)
    assert all(token.winning_asset_id == "222" for token in resolved_tokens)
    assert all(token.winning_outcome == "NO" for token in resolved_tokens)
    assert all(token.resolution_status == "RESOLVED" for token in resolved_tokens)


def test_gamma_uma_resolution_uses_settled_outcome_prices_as_winner_truth() -> None:
    tokens = tokens_from_gamma_market(
        {
            "id": "m-uma-resolved",
            "conditionId": "0xresolved",
            "active": True,
            "closed": True,
            "umaResolutionStatus": "resolved",
            "outcomePrices": '["0", "1"]',
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["YES", "NO"]',
            "closedTime": "2026-07-18 14:00:43+00",
        }
    )

    assert all(token.resolved is True for token in tokens)
    assert all(token.winning_asset_id == "222" for token in tokens)
    assert all(token.winning_outcome == "NO" for token in tokens)
    assert all(token.resolution_status == "RESOLVED" for token in tokens)
    assert all(token.resolution_source == "gamma_api" for token in tokens)
    assert all(token.resolved_time == datetime(2026, 7, 18, 14, 0, 43, tzinfo=timezone.utc) for token in tokens)


def test_gamma_uma_proposal_stops_execution_without_claiming_resolution() -> None:
    tokens = tokens_from_gamma_market(
        {
            "id": "m-uma-proposed",
            "conditionId": "0xproposed",
            "active": True,
            "closed": False,
            "umaResolutionStatus": "proposed",
            "outcomePrices": '["0.0005", "0.9995"]',
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["OVER", "UNDER"]',
            "umaEndDate": "2026-07-18T14:25:00Z",
        }
    )

    assert all(token.closed is True for token in tokens)
    assert all(token.resolved is False for token in tokens)
    assert all(token.winning_asset_id is None for token in tokens)
    assert all(token.resolution_status == "PROPOSED" for token in tokens)
    assert all(token.resolved_time is None for token in tokens)


def test_gamma_uma_proposal_does_not_close_explicitly_reopened_order_book() -> None:
    tokens = tokens_from_gamma_market(
        {
            "id": "m-uma-reopened",
            "conditionId": "0xreopened",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "enableOrderBook": True,
            "umaResolutionStatus": "proposed",
            "clobTokenIds": '["111", "222"]',
            "outcomes": '["OVER", "UNDER"]',
        }
    )

    assert all(token.closed is False for token in tokens)
    assert all(token.resolved is False for token in tokens)
    assert all(token.resolution_status == "PROPOSED" for token in tokens)


class _FakeResponse:
    def __init__(self, payload: object, *, status_code: int = 200, text: str = "") -> None:
        self.payload = payload
        self.status_code = status_code
        self.text = text

    def json(self) -> object:
        return self.payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []
        self.headers: dict[str, str] = {}
        self.trust_env = True
        self.proxies: dict[str, str] = {}

    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"method": "GET", "url": url, **kwargs})
        return self.responses.pop(0)

    def post(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"method": "POST", "url": url, **kwargs})
        return self.responses.pop(0)


class _FailoverSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.trust_env = True
        self.proxies: dict[str, str] = {}
        self.proxy_calls: list[str | None] = []

    def get(self, _url: str, **_kwargs: object) -> _FakeResponse:
        self.proxy_calls.append(self.proxies.get("https"))
        if len(self.proxy_calls) == 1:
            raise OSError("primary proxy TLS EOF")
        return _FakeResponse([{"id": "market-via-fallback"}])
