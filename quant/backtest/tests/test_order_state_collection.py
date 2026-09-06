from quant.backtest.order_state_collection import (
    build_collection_request_params,
    event_time_watermark,
    extract_response_cursor,
)


def test_build_collection_request_params_prefers_cursor() -> None:
    params = build_collection_request_params(
        {"limit": "100"},
        {"last_cursor": "abc", "last_event_time": "2026-06-25T00:00:00Z"},
        since_param="updated_after",
        cursor_param="cursor",
    )

    assert params == {"limit": "100", "cursor": "abc"}


def test_build_collection_request_params_uses_since_watermark() -> None:
    params = build_collection_request_params(
        {"limit": "100"},
        {"last_event_time": "2026-06-25T00:00:00Z"},
        since_param="updated_after",
        cursor_param="cursor",
    )

    assert params["limit"] == "100"
    assert params["updated_after"] == "2026-06-25T00:00:00+00:00"
    assert "cursor" not in params


def test_build_collection_request_params_uses_initial_since() -> None:
    params = build_collection_request_params(
        {},
        None,
        since_param="updated_after",
        initial_since="2026-06-24T00:00:00Z",
    )

    assert params == {"updated_after": "2026-06-24T00:00:00+00:00"}


def test_extract_response_cursor_accepts_common_shapes() -> None:
    assert extract_response_cursor({"nextCursor": "n1"}) == "n1"
    assert extract_response_cursor({"pagination": {"nextPageToken": "n2"}}) == "n2"
    assert extract_response_cursor({"meta": {"cursor": "n3"}}) == "n3"
    assert extract_response_cursor({"items": []}) is None


def test_event_time_watermark_returns_latest_iso_timestamp() -> None:
    watermark = event_time_watermark([
        {"event_time": "2026-06-25T00:00:00Z"},
        {"event_time": "2026-06-25T00:00:03+00:00"},
        {"event_time": "bad"},
    ])

    assert watermark == "2026-06-25T00:00:03+00:00"
