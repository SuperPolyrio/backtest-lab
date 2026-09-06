"""Postgres schema for quant price production tables."""

from __future__ import annotations

import re
from typing import Any


CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS quant"

OPTIONAL_EXTENSION_SQL: tuple[str, ...] = (
    "CREATE EXTENSION IF NOT EXISTS pg_trgm",
)


CREATE_TABLE_SQL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_metadata (
        market_id BIGINT NOT NULL,
        gamma_market_id TEXT,
        market_slug TEXT,
        condition_id TEXT,
        question_id TEXT,
        market_title TEXT,
        token_id TEXT NOT NULL PRIMARY KEY,
        token_id_hex TEXT,
        token_side TEXT NOT NULL,
        outcome_index INTEGER,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        closed BOOLEAN NOT NULL DEFAULT FALSE,
        archived BOOLEAN NOT NULL DEFAULT FALSE,
        deprecated BOOLEAN NOT NULL DEFAULT FALSE,
        duplicate_group_key TEXT,
        end_date TIMESTAMPTZ,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_event_metadata (
        event_id TEXT NOT NULL,
        event_slug TEXT NOT NULL PRIMARY KEY,
        event_title TEXT NOT NULL,
        event_category TEXT,
        event_subcategory TEXT,
        event_image_url TEXT,
        event_icon_url TEXT,
        description TEXT,
        start_date TIMESTAMPTZ,
        end_date TIMESTAMPTZ,
        resolution_date TIMESTAMPTZ,
        status TEXT NOT NULL DEFAULT 'unknown',
        volume NUMERIC(38, 10),
        liquidity NUMERIC(38, 10),
        grouping_confidence TEXT NOT NULL DEFAULT 'official',
        source TEXT NOT NULL DEFAULT 'core.markets',
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_event_members (
        event_slug TEXT NOT NULL REFERENCES quant.market_event_metadata(event_slug) ON DELETE CASCADE,
        event_id TEXT NOT NULL,
        market_id BIGINT NOT NULL,
        market_slug TEXT NOT NULL,
        condition_id TEXT,
        question TEXT,
        outcome_label TEXT NOT NULL,
        outcome_key TEXT NOT NULL,
        outcome_order INTEGER NOT NULL DEFAULT 0,
        token_yes_id TEXT,
        token_no_id TEXT,
        clob_token_ids JSONB,
        status TEXT NOT NULL DEFAULT 'unknown',
        active BOOLEAN NOT NULL DEFAULT FALSE,
        closed BOOLEAN NOT NULL DEFAULT FALSE,
        resolved BOOLEAN NOT NULL DEFAULT FALSE,
        volume NUMERIC(38, 10),
        liquidity NUMERIC(38, 10),
        block_rows BIGINT NOT NULL DEFAULT 0,
        frontend_rows BIGINT NOT NULL DEFAULT 0,
        orderfilled_rows BIGINT NOT NULL DEFAULT 0,
        latest_yes NUMERIC(20, 10),
        latest_no NUMERIC(20, 10),
        latest_block BIGINT,
        latest_timestamp TIMESTAMPTZ,
        coverage_status TEXT NOT NULL DEFAULT 'none',
        grouping_confidence TEXT NOT NULL DEFAULT 'official',
        source TEXT NOT NULL DEFAULT 'core.markets',
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (event_slug, market_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_price_eligibility (
        token_id TEXT PRIMARY KEY REFERENCES quant.market_token_metadata(token_id) ON DELETE CASCADE,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        eligible BOOLEAN NOT NULL DEFAULT FALSE,
        has_orderfilled_trades BOOLEAN NOT NULL DEFAULT FALSE,
        is_archived BOOLEAN NOT NULL DEFAULT FALSE,
        is_deprecated BOOLEAN NOT NULL DEFAULT FALSE,
        is_duplicate_market BOOLEAN NOT NULL DEFAULT FALSE,
        skip_reason TEXT,
        orderfilled_trade_count BIGINT NOT NULL DEFAULT 0,
        first_orderfilled_block BIGINT,
        last_orderfilled_block BIGINT,
        frontend_points BIGINT NOT NULL DEFAULT 0,
        checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_frontend_price_1m (
        token_id TEXT NOT NULL REFERENCES quant.market_token_metadata(token_id) ON DELETE CASCADE,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        ts_minute TIMESTAMPTZ NOT NULL,
        timestamp BIGINT NOT NULL,
        price NUMERIC(20, 10) NOT NULL,
        source TEXT NOT NULL DEFAULT 'prices-history',
        fidelity_minutes INTEGER NOT NULL DEFAULT 1,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (token_id, ts_minute)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_block_close (
        token_id TEXT NOT NULL,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        block_number BIGINT NOT NULL,
        block_timestamp TIMESTAMPTZ,
        open_price NUMERIC(20, 10),
        high_price NUMERIC(20, 10),
        low_price NUMERIC(20, 10),
        close_price NUMERIC(20, 10) NOT NULL,
        yes_probability_close NUMERIC(20, 10),
        vwap_price NUMERIC(20, 10),
        yes_probability_vwap NUMERIC(20, 10),
        close_raw_price NUMERIC(20, 10),
        close_price_source TEXT NOT NULL DEFAULT 'unknown',
        close_tx_hash TEXT,
        close_log_index INTEGER,
        close_maker_amount NUMERIC(38, 0),
        close_taker_amount NUMERIC(38, 0),
        trade_count BIGINT NOT NULL DEFAULT 0,
        raw_trade_count BIGINT NOT NULL DEFAULT 0,
        internal_filtered_count BIGINT NOT NULL DEFAULT 0,
        invalid_size_count BIGINT NOT NULL DEFAULT 0,
        invalid_price_count BIGINT NOT NULL DEFAULT 0,
        amount_ratio_count BIGINT NOT NULL DEFAULT 0,
        raw_price_fallback_count BIGINT NOT NULL DEFAULT 0,
        extreme_trade_count BIGINT NOT NULL DEFAULT 0,
        anomaly_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
        volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        buy_volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        sell_volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        first_log_index INTEGER,
        last_log_index INTEGER,
        first_tx_hash TEXT,
        last_tx_hash TEXT,
        source TEXT NOT NULL DEFAULT 'clean_orderfilled_fact',
        built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (token_id, block_number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_api_trades (
        token_id TEXT NOT NULL,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        trade_key TEXT NOT NULL,
        transaction_hash TEXT,
        timestamp BIGINT NOT NULL,
        trade_time TIMESTAMPTZ NOT NULL,
        price NUMERIC(20, 10) NOT NULL,
        yes_probability NUMERIC(20, 10),
        size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        side TEXT,
        proxy_wallet TEXT,
        condition_id TEXT,
        market_filter_condition_id TEXT,
        outcome TEXT,
        outcome_index INTEGER,
        source_endpoint TEXT NOT NULL DEFAULT 'https://data-api.polymarket.com/trades',
        taker_only BOOLEAN NOT NULL DEFAULT FALSE,
        source TEXT NOT NULL DEFAULT 'data-api-trades',
        built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (token_id, trade_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_price_series_tiles (
        tile_key TEXT PRIMARY KEY,
        key_version INTEGER NOT NULL DEFAULT 1,
        entity_type TEXT NOT NULL DEFAULT 'event',
        tile_kind TEXT NOT NULL DEFAULT 'series',
        scope TEXT NOT NULL,
        entity_slug TEXT NOT NULL,
        price_source TEXT NOT NULL,
        range_name TEXT NOT NULL,
        resolution TEXT NOT NULL,
        point_format TEXT NOT NULL DEFAULT 'lite',
        top_n INTEGER NOT NULL,
        max_points INTEGER NOT NULL,
        window_from_x BIGINT,
        window_to_x BIGINT,
        payload JSONB NOT NULL,
        payload_bytes BIGINT NOT NULL DEFAULT 0,
        row_count BIGINT NOT NULL DEFAULT 0,
        data_min_x BIGINT,
        data_max_x BIGINT,
        cache_ttl_seconds INTEGER,
        updated_reason TEXT,
        expires_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_price_build_runs (
        run_id BIGSERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        mode TEXT NOT NULL DEFAULT 'once',
        status TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ,
        requested_from_ts TIMESTAMPTZ,
        requested_to_ts TIMESTAMPTZ,
        requested_from_block BIGINT,
        requested_to_block BIGINT,
        markets_total BIGINT NOT NULL DEFAULT 0,
        markets_complete BIGINT NOT NULL DEFAULT 0,
        rows_written BIGINT NOT NULL DEFAULT 0,
        error_count BIGINT NOT NULL DEFAULT 0,
        last_error TEXT,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_block_close_rebuild_runs (
        rebuild_id UUID PRIMARY KEY,
        build_run_id BIGINT NOT NULL UNIQUE
            REFERENCES quant.market_price_build_runs(run_id) ON DELETE CASCADE,
        status TEXT NOT NULL DEFAULT 'initialized'
            CHECK (status IN (
                'initialized', 'running', 'ready_to_verify', 'verified',
                'swapped', 'reconciled', 'failed'
            )),
        source_table TEXT NOT NULL,
        source_cutoff TIMESTAMPTZ NOT NULL,
        source_highwater_block BIGINT NOT NULL,
        source_snapshot JSONB NOT NULL,
        timestamp_anchor_block BIGINT,
        timestamp_anchor_time TIMESTAMPTZ,
        algorithm_version TEXT NOT NULL,
        serving_semantics TEXT NOT NULL,
        requested_from_block BIGINT NOT NULL,
        requested_to_block BIGINT NOT NULL,
        shard_count INTEGER NOT NULL CHECK (shard_count > 0),
        chunk_size BIGINT NOT NULL CHECK (chunk_size > 0),
        shadow_tablespace TEXT NOT NULL
            CHECK (
                shadow_tablespace ~ '^[A-Za-z_][A-Za-z0-9_]*$'
                AND shadow_tablespace NOT IN ('pg_default', 'pg_global')
                AND octet_length(shadow_tablespace) <= 63
            ),
        shadow_table TEXT NOT NULL UNIQUE
            CHECK (
                length(shadow_table) <= 63
                AND shadow_table ~ '^quant\\.mtbc_rebuild_[0-9a-f]{32}$'
            ),
        backup_table TEXT
            CHECK (
                backup_table IS NULL
                OR (
                    length(backup_table) <= 63
                    AND backup_table ~ '^quant\\.mtbc_backup_[0-9a-f]{32}$'
                )
            ),
        manifest_hash TEXT NOT NULL,
        expected_canonical_generation BIGINT NOT NULL DEFAULT 0,
        expected_canonical_rebuild_id UUID,
        published_generation BIGINT,
        verification_receipt JSONB,
        verification_receipt_hash TEXT,
        verified_at TIMESTAMPTZ,
        swapped_at TIMESTAMPTZ,
        reconciled_at TIMESTAMPTZ,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        CHECK (requested_from_block <= requested_to_block),
        CHECK (
            (timestamp_anchor_block IS NULL) =
            (timestamp_anchor_time IS NULL)
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_block_close_generation (
        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
        generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
        rebuild_id UUID,
        verification_receipt_hash TEXT,
        source_cutoff TIMESTAMPTZ,
        source_highwater_block BIGINT,
        reconciled BOOLEAN NOT NULL DEFAULT TRUE,
        published_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        CHECK (
            (rebuild_id IS NULL AND generation = 0)
            OR (rebuild_id IS NOT NULL AND generation > 0)
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_block_close_rebuild_targets (
        rebuild_id UUID NOT NULL
            REFERENCES quant.market_token_block_close_rebuild_runs(rebuild_id)
            ON DELETE CASCADE,
        token_id TEXT NOT NULL,
        token_id_hex TEXT NOT NULL,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        outcome_index INTEGER,
        shard_index INTEGER NOT NULL CHECK (shard_index >= 0),
        requested_from_block BIGINT NOT NULL,
        requested_to_block BIGINT NOT NULL,
        metadata_hash TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (rebuild_id, token_id),
        UNIQUE (rebuild_id, token_id_hex),
        CHECK (requested_from_block <= requested_to_block)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_block_close_rebuild_chunks (
        chunk_id BIGSERIAL PRIMARY KEY,
        rebuild_id UUID NOT NULL
            REFERENCES quant.market_token_block_close_rebuild_runs(rebuild_id)
            ON DELETE CASCADE,
        shard_index INTEGER NOT NULL CHECK (shard_index >= 0),
        market_id BIGINT,
        from_block BIGINT NOT NULL,
        to_block BIGINT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK (status IN ('pending', 'leased', 'complete', 'failed')),
        lease_owner TEXT,
        lease_expires_at TIMESTAMPTZ,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        source_rows BIGINT,
        mapped_rows BIGINT,
        inserted_rows BIGINT,
        identical_conflicts BIGINT,
        row_checksum TEXT,
        receipt JSONB,
        started_at TIMESTAMPTZ,
        committed_at TIMESTAMPTZ,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        UNIQUE (rebuild_id, shard_index, from_block, to_block),
        CHECK (from_block <= to_block),
        CHECK (
            status <> 'complete'
            OR (
                source_rows IS NOT NULL
                AND mapped_rows IS NOT NULL
                AND inserted_rows IS NOT NULL
                AND identical_conflicts IS NOT NULL
                AND row_checksum IS NOT NULL
                AND receipt IS NOT NULL
                AND committed_at IS NOT NULL
                AND mapped_rows = inserted_rows + identical_conflicts
            )
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_price_build_market_state (
        source TEXT NOT NULL,
        token_id TEXT NOT NULL REFERENCES quant.market_token_metadata(token_id) ON DELETE CASCADE,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        next_from_ts TIMESTAMPTZ,
        next_to_ts TIMESTAMPTZ,
        next_from_block BIGINT,
        next_to_block BIGINT,
        last_complete_ts TIMESTAMPTZ,
        last_complete_block BIGINT,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (source, token_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_price_build_market_progress (
        market_id BIGINT PRIMARY KEY,
        market_slug TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        token_count INTEGER NOT NULL DEFAULT 0,
        eligible_token_count INTEGER NOT NULL DEFAULT 0,
        first_orderfilled_block BIGINT,
        last_orderfilled_block BIGINT,
        min_block_complete BIGINT,
        max_block_complete BIGINT,
        min_frontend_complete_ts TIMESTAMPTZ,
        max_frontend_complete_ts TIMESTAMPTZ,
        block_rows_written BIGINT NOT NULL DEFAULT 0,
        frontend_rows_written BIGINT NOT NULL DEFAULT 0,
        error_count BIGINT NOT NULL DEFAULT 0,
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_orderfilled_market_stats (
        market_id BIGINT PRIMARY KEY,
        trade_count BIGINT NOT NULL DEFAULT 0,
        first_orderfilled_block BIGINT,
        last_orderfilled_block BIGINT,
        refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_orderfilled_token_stats (
        market_id BIGINT NOT NULL,
        token_id_hex TEXT NOT NULL,
        trade_count BIGINT NOT NULL DEFAULT 0,
        first_orderfilled_block BIGINT,
        last_orderfilled_block BIGINT,
        refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (market_id, token_id_hex)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_token_execution_summary (
        token_id TEXT PRIMARY KEY,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        first_block BIGINT,
        last_block BIGINT,
        block_row_count BIGINT NOT NULL DEFAULT 0,
        trade_count BIGINT NOT NULL DEFAULT 0,
        raw_trade_count BIGINT NOT NULL DEFAULT 0,
        volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        maker_amount NUMERIC(38, 0) NOT NULL DEFAULT 0,
        taker_amount NUMERIC(38, 0) NOT NULL DEFAULT 0,
        maker_share NUMERIC(20, 10),
        taker_share NUMERIC(20, 10),
        internal_filtered_count BIGINT NOT NULL DEFAULT 0,
        invalid_size_count BIGINT NOT NULL DEFAULT 0,
        invalid_price_count BIGINT NOT NULL DEFAULT 0,
        amount_ratio_count BIGINT NOT NULL DEFAULT 0,
        raw_price_fallback_count BIGINT NOT NULL DEFAULT 0,
        extreme_trade_count BIGINT NOT NULL DEFAULT 0,
        anomaly_count BIGINT NOT NULL DEFAULT 0,
        anomaly_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
        side_bucket TEXT NOT NULL DEFAULT 'unknown',
        liquidity_bucket TEXT NOT NULL DEFAULT 'unknown',
        refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_execution_summary (
        market_id BIGINT PRIMARY KEY,
        market_slug TEXT,
        token_count BIGINT NOT NULL DEFAULT 0,
        first_block BIGINT,
        last_block BIGINT,
        block_row_count BIGINT NOT NULL DEFAULT 0,
        trade_count BIGINT NOT NULL DEFAULT 0,
        raw_trade_count BIGINT NOT NULL DEFAULT 0,
        volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        maker_amount NUMERIC(38, 0) NOT NULL DEFAULT 0,
        taker_amount NUMERIC(38, 0) NOT NULL DEFAULT 0,
        maker_share NUMERIC(20, 10),
        taker_share NUMERIC(20, 10),
        anomaly_count BIGINT NOT NULL DEFAULT 0,
        side_bucket_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
        liquidity_bucket_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
        dominant_side_bucket TEXT NOT NULL DEFAULT 'unknown',
        dominant_liquidity_bucket TEXT NOT NULL DEFAULT 'unknown',
        refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.event_execution_summary (
        event_slug TEXT PRIMARY KEY,
        event_title TEXT,
        market_count BIGINT NOT NULL DEFAULT 0,
        token_count BIGINT NOT NULL DEFAULT 0,
        first_block BIGINT,
        last_block BIGINT,
        block_row_count BIGINT NOT NULL DEFAULT 0,
        trade_count BIGINT NOT NULL DEFAULT 0,
        raw_trade_count BIGINT NOT NULL DEFAULT 0,
        volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        maker_amount NUMERIC(38, 0) NOT NULL DEFAULT 0,
        taker_amount NUMERIC(38, 0) NOT NULL DEFAULT 0,
        maker_share NUMERIC(20, 10),
        taker_share NUMERIC(20, 10),
        anomaly_count BIGINT NOT NULL DEFAULT 0,
        side_bucket_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
        liquidity_bucket_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
        dominant_side_bucket TEXT NOT NULL DEFAULT 'unknown',
        dominant_liquidity_bucket TEXT NOT NULL DEFAULT 'unknown',
        refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_price_build_targets (
        source TEXT NOT NULL,
        token_id TEXT NOT NULL REFERENCES quant.market_token_metadata(token_id) ON DELETE CASCADE,
        market_id BIGINT NOT NULL,
        market_slug TEXT,
        token_side TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 100,
        reason TEXT,
        requested_from_ts TIMESTAMPTZ,
        requested_to_ts TIMESTAMPTZ,
        requested_from_block BIGINT,
        requested_to_block BIGINT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (source, token_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_orderbook_snapshots (
        snapshot_id BIGSERIAL PRIMARY KEY,
        market_id BIGINT,
        condition_id TEXT,
        market_slug TEXT,
        token_id TEXT NOT NULL,
        side TEXT NOT NULL DEFAULT 'YES',
        paired_token_id TEXT,
        market_title TEXT,
        source TEXT NOT NULL DEFAULT 'clob-book',
        event_type TEXT NOT NULL DEFAULT 'snapshot',
        book_generation BIGINT NOT NULL DEFAULT 0,
        book_status TEXT NOT NULL DEFAULT 'unknown',
        block_number BIGINT,
        snapshot_timestamp TIMESTAMPTZ,
        best_bid NUMERIC(20, 10),
        best_ask NUMERIC(20, 10),
        spread NUMERIC(20, 10),
        mid NUMERIC(20, 10),
        bid_depth NUMERIC(38, 10) NOT NULL DEFAULT 0,
        ask_depth NUMERIC(38, 10) NOT NULL DEFAULT 0,
        depth_total NUMERIC(38, 10) NOT NULL DEFAULT 0,
        imbalance NUMERIC(20, 10),
        level_count_bid INTEGER NOT NULL DEFAULT 0,
        level_count_ask INTEGER NOT NULL DEFAULT 0,
        storage_tier TEXT NOT NULL DEFAULT 'sampled',
        payload JSONB NOT NULL,
        snapshot_version TEXT,
        fetched_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_raw_events (
        raw_event_id BIGSERIAL PRIMARY KEY,
        source TEXT NOT NULL DEFAULT 'polymarket_market_ws_raw',
        ws_url TEXT,
        shard_id INTEGER,
        shard_count INTEGER,
        asset_id TEXT,
        market TEXT,
        event_type TEXT NOT NULL,
        event_ts_ms BIGINT,
        event_ts TIMESTAMPTZ,
        received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        ingest_hour TIMESTAMPTZ NOT NULL DEFAULT date_trunc('hour', clock_timestamp()),
        sequence_in_message INTEGER NOT NULL DEFAULT 0,
        raw_payload_hash TEXT NOT NULL,
        raw_payload JSONB NOT NULL,
        asset_payload JSONB,
        best_bid NUMERIC(20, 10),
        best_ask NUMERIC(20, 10),
        price NUMERIC(20, 10),
        size NUMERIC(38, 10),
        side TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_archive_manifest (
        manifest_id BIGSERIAL PRIMARY KEY,
        archive_hour TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL DEFAULT 'polymarket_market_ws_archive',
        ws_url TEXT,
        shard_id INTEGER,
        shard_count INTEGER,
        path TEXT NOT NULL UNIQUE,
        format TEXT NOT NULL DEFAULT 'parquet',
        compression TEXT NOT NULL DEFAULT 'zstd',
        compression_level INTEGER NOT NULL DEFAULT 9,
        row_group_size BIGINT NOT NULL DEFAULT 1048576,
        sort_key JSONB NOT NULL DEFAULT '[]'::jsonb,
        schema_version TEXT NOT NULL,
        event_count BIGINT NOT NULL DEFAULT 0,
        asset_count BIGINT NOT NULL DEFAULT 0,
        market_count BIGINT NOT NULL DEFAULT 0,
        first_event_ts TIMESTAMPTZ,
        last_event_ts TIMESTAMPTZ,
        first_received_at TIMESTAMPTZ,
        last_received_at TIMESTAMPTZ,
        file_size_bytes BIGINT NOT NULL DEFAULT 0,
        sha256 TEXT,
        status TEXT NOT NULL DEFAULT 'ready',
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        written_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_connection_state (
        shard_id INTEGER PRIMARY KEY,
        connection_id TEXT NOT NULL,
        connection_generation BIGINT NOT NULL,
        transport_state TEXT NOT NULL,
        assigned_token_count INTEGER NOT NULL DEFAULT 0,
        pending_snapshot_count INTEGER NOT NULL DEFAULT 0,
        reconnect_count BIGINT NOT NULL DEFAULT 0,
        connected_at TIMESTAMPTZ,
        last_message_at TIMESTAMPTZ,
        last_status_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_subscription_state (
        asset_id TEXT PRIMARY KEY,
        market_id BIGINT,
        condition_id TEXT,
        desired BOOLEAN NOT NULL DEFAULT FALSE,
        desired_generation BIGINT NOT NULL DEFAULT 0,
        shard_id INTEGER,
        connection_id TEXT,
        connection_generation BIGINT,
        state TEXT NOT NULL DEFAULT 'DESIRED',
        assigned_at TIMESTAMPTZ,
        subscribe_sent_at TIMESTAMPTZ,
        first_snapshot_at TIMESTAMPTZ,
        first_event_at TIMESTAMPTZ,
        last_event_at TIMESTAMPTZ,
        last_book_at TIMESTAMPTZ,
        last_price_change_at TIMESTAMPTZ,
        last_trade_at TIMESTAMPTZ,
        snapshot_wait_ms BIGINT,
        event_age_ms BIGINT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        last_retry_at TIMESTAMPTZ,
        last_error_code TEXT,
        last_error_detail TEXT,
        rest_book_status TEXT,
        rest_book_checked_at TIMESTAMPTZ,
        lifecycle_state TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_current_coverage (
        asset_id TEXT PRIMARY KEY,
        shard_id INTEGER,
        connection_id TEXT,
        connection_generation BIGINT,
        subscription_state TEXT NOT NULL,
        coverage_grade TEXT NOT NULL,
        last_snapshot_ts TIMESTAMPTZ,
        last_exchange_ts TIMESTAMPTZ,
        last_receive_ts TIMESTAMPTZ,
        last_book_apply_ts TIMESTAMPTZ,
        last_archive_enqueue_ts TIMESTAMPTZ,
        last_archive_write_ts TIMESTAMPTZ,
        receive_age_ms BIGINT,
        book_age_ms BIGINT,
        archive_age_ms BIGINT,
        has_gap BOOLEAN NOT NULL DEFAULT FALSE,
        rest_reconciled BOOLEAN NOT NULL DEFAULT FALSE,
        redundant_feed_match BOOLEAN,
        current_book_hash TEXT,
        current_best_bid NUMERIC(20, 10),
        current_best_ask NUMERIC(20, 10),
        coverage_reason TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_rest_audits (
        audit_id BIGSERIAL PRIMARY KEY,
        asset_id TEXT NOT NULL,
        shard_id INTEGER,
        audit_ts TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        audit_kind TEXT NOT NULL DEFAULT 'reconciliation',
        local_book_hash TEXT,
        rest_book_hash TEXT,
        hash_match BOOLEAN,
        local_best_bid NUMERIC(20, 10),
        rest_best_bid NUMERIC(20, 10),
        local_best_ask NUMERIC(20, 10),
        rest_best_ask NUMERIC(20, 10),
        bbo_match BOOLEAN,
        top5_match BOOLEAN,
        rest_book_status TEXT NOT NULL,
        classification TEXT,
        mismatch_reason TEXT,
        repair_action TEXT,
        repaired_at TIMESTAMPTZ,
        raw_rest_payload JSONB,
        error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_gap_incidents (
        incident_id BIGSERIAL PRIMARY KEY,
        asset_id TEXT,
        shard_id INTEGER,
        connection_id TEXT,
        detected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        suspected_start_ts TIMESTAMPTZ,
        suspected_end_ts TIMESTAMPTZ,
        incident_type TEXT NOT NULL,
        severity TEXT NOT NULL,
        evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
        execution_disabled_at TIMESTAMPTZ,
        repair_action TEXT,
        repair_started_at TIMESTAMPTZ,
        repair_finished_at TIMESTAMPTZ,
        post_repair_rest_match BOOLEAN,
        post_repair_replay_match BOOLEAN,
        status TEXT NOT NULL DEFAULT 'open'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_canary_connection_state (
        worker_id TEXT PRIMARY KEY,
        connection_id TEXT NOT NULL,
        transport_state TEXT NOT NULL,
        assigned_token_count INTEGER NOT NULL DEFAULT 0,
        snapshot_confirmed_count INTEGER NOT NULL DEFAULT 0,
        last_message_at TIMESTAMPTZ,
        last_audit_at TIMESTAMPTZ,
        reconnect_count BIGINT NOT NULL DEFAULT 0,
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_canary_state (
        asset_id TEXT PRIMARY KEY,
        shard_id INTEGER,
        connection_id TEXT,
        selected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        ws_snapshot_at TIMESTAMPTZ,
        secondary_last_event_at TIMESTAMPTZ,
        primary_best_bid NUMERIC(20, 10),
        primary_best_ask NUMERIC(20, 10),
        secondary_best_bid NUMERIC(20, 10),
        secondary_best_ask NUMERIC(20, 10),
        rest_best_bid NUMERIC(20, 10),
        rest_best_ask NUMERIC(20, 10),
        primary_secondary_match BOOLEAN,
        primary_rest_match BOOLEAN,
        secondary_rest_match BOOLEAN,
        consecutive_primary_mismatch INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'SELECTED',
        last_classification TEXT,
        last_error TEXT,
        last_audit_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_canary_audits (
        audit_id BIGSERIAL PRIMARY KEY,
        asset_id TEXT NOT NULL,
        shard_id INTEGER,
        connection_id TEXT,
        audit_ts TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        primary_best_bid NUMERIC(20, 10),
        primary_best_ask NUMERIC(20, 10),
        secondary_best_bid NUMERIC(20, 10),
        secondary_best_ask NUMERIC(20, 10),
        rest_best_bid NUMERIC(20, 10),
        rest_best_ask NUMERIC(20, 10),
        primary_secondary_match BOOLEAN,
        primary_rest_match BOOLEAN,
        secondary_rest_match BOOLEAN,
        classification TEXT NOT NULL,
        execution_disabled BOOLEAN NOT NULL DEFAULT FALSE,
        raw_rest_payload JSONB,
        error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_redundant_connection_state (
        shard_id INTEGER PRIMARY KEY,
        connection_id TEXT NOT NULL,
        transport_state TEXT NOT NULL,
        assigned_token_count INTEGER NOT NULL DEFAULT 0,
        snapshot_confirmed_count INTEGER NOT NULL DEFAULT 0,
        last_message_at TIMESTAMPTZ,
        last_audit_at TIMESTAMPTZ,
        reconnect_count BIGINT NOT NULL DEFAULT 0,
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_redundant_feed_state (
        asset_id TEXT PRIMARY KEY,
        shard_id INTEGER NOT NULL,
        connection_id TEXT NOT NULL,
        ws_snapshot_at TIMESTAMPTZ,
        secondary_last_event_at TIMESTAMPTZ,
        primary_best_bid NUMERIC(20, 10),
        primary_best_ask NUMERIC(20, 10),
        secondary_best_bid NUMERIC(20, 10),
        secondary_best_ask NUMERIC(20, 10),
        rest_best_bid NUMERIC(20, 10),
        rest_best_ask NUMERIC(20, 10),
        final_state_match BOOLEAN,
        classification TEXT NOT NULL,
        consecutive_primary_mismatch INTEGER NOT NULL DEFAULT 0,
        execution_disabled BOOLEAN NOT NULL DEFAULT FALSE,
        last_compared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_redundant_mismatch_audits (
        audit_id BIGSERIAL PRIMARY KEY,
        asset_id TEXT NOT NULL,
        shard_id INTEGER NOT NULL,
        connection_id TEXT NOT NULL,
        audit_ts TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        primary_best_bid NUMERIC(20, 10),
        primary_best_ask NUMERIC(20, 10),
        secondary_best_bid NUMERIC(20, 10),
        secondary_best_ask NUMERIC(20, 10),
        rest_best_bid NUMERIC(20, 10),
        rest_best_ask NUMERIC(20, 10),
        classification TEXT NOT NULL,
        execution_disabled BOOLEAN NOT NULL DEFAULT FALSE,
        error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_redundant_audit_summary (
        summary_id BIGSERIAL PRIMARY KEY,
        shard_id INTEGER NOT NULL,
        connection_id TEXT NOT NULL,
        audit_ts TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        assigned INTEGER NOT NULL,
        snapshots INTEGER NOT NULL,
        compared INTEGER NOT NULL,
        classification_counts JSONB NOT NULL,
        execution_disabled INTEGER NOT NULL DEFAULT 0,
        rest_checked INTEGER NOT NULL DEFAULT 0,
        rest_error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.clob_l2_watermarks (
        worker_id TEXT PRIMARY KEY,
        shard_id INTEGER,
        connection_generation BIGINT,
        feed_receive_watermark TIMESTAMPTZ,
        book_apply_watermark TIMESTAMPTZ,
        archive_write_watermark TIMESTAMPTZ,
        coverage_finalize_watermark TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_market_registry_state (
        key TEXT PRIMARY KEY,
        value TEXT,
        value_json JSONB,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_market_registry_tokens (
        asset_id TEXT PRIMARY KEY,
        market_id BIGINT,
        gamma_market_id TEXT,
        condition_id TEXT,
        market_slug TEXT,
        market_title TEXT,
        outcome_name TEXT NOT NULL DEFAULT 'UNKNOWN',
        outcome_index INTEGER,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        closed BOOLEAN NOT NULL DEFAULT FALSE,
        resolved BOOLEAN NOT NULL DEFAULT FALSE,
        archived BOOLEAN NOT NULL DEFAULT FALSE,
        deprecated BOOLEAN NOT NULL DEFAULT FALSE,
        status_present BOOLEAN NOT NULL DEFAULT FALSE,
        completion_status TEXT,
        token_count INTEGER NOT NULL DEFAULT 0,
        market_state TEXT NOT NULL DEFAULT 'UNKNOWN',
        subscription_eligible BOOLEAN NOT NULL DEFAULT FALSE,
        execution_eligible BOOLEAN NOT NULL DEFAULT FALSE,
        subscription_reason TEXT,
        execution_reason TEXT,
        book_quality TEXT NOT NULL DEFAULT 'NOT_CHECKED',
        book_status TEXT,
        latest_book_at TIMESTAMPTZ,
        book_age_ms BIGINT,
        best_bid NUMERIC(20, 10),
        best_ask NUMERIC(20, 10),
        current_tick_size NUMERIC(20, 10),
        min_order_size NUMERIC(38, 10),
        book_source TEXT,
        storage_tier TEXT,
        book_seen_first_at TIMESTAMPTZ,
        book_seen_last_at TIMESTAMPTZ,
        last_l2_event_at TIMESTAMPTZ,
        last_book_update_at TIMESTAMPTZ,
        last_book_quality TEXT,
        winning_asset_id TEXT,
        winning_outcome TEXT,
        resolution_status TEXT,
        resolution_source TEXT,
        resolved_time TIMESTAMPTZ,
        metadata_hash TEXT,
        token_mapping_confidence TEXT,
        desired_subscribed BOOLEAN NOT NULL DEFAULT FALSE,
        actual_subscribed BOOLEAN NOT NULL DEFAULT FALSE,
        last_source TEXT NOT NULL DEFAULT 'unknown',
        last_run_id BIGINT,
        last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_transition_at TIMESTAMPTZ,
        raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_market_registry_markets (
        market_key TEXT PRIMARY KEY,
        market_id BIGINT,
        gamma_market_id TEXT,
        condition_id TEXT,
        market_slug TEXT,
        market_title TEXT,
        market_state TEXT NOT NULL DEFAULT 'DISCOVERED',
        active BOOLEAN NOT NULL DEFAULT TRUE,
        closed BOOLEAN NOT NULL DEFAULT FALSE,
        resolved BOOLEAN NOT NULL DEFAULT FALSE,
        archived BOOLEAN NOT NULL DEFAULT FALSE,
        deprecated BOOLEAN NOT NULL DEFAULT FALSE,
        token_count INTEGER NOT NULL DEFAULT 0,
        subscription_token_count INTEGER NOT NULL DEFAULT 0,
        execution_token_count INTEGER NOT NULL DEFAULT 0,
        winning_asset_id TEXT,
        winning_outcome TEXT,
        resolution_status TEXT,
        resolution_source TEXT,
        resolved_time TIMESTAMPTZ,
        metadata_hash TEXT,
        last_source TEXT NOT NULL DEFAULT 'unknown',
        last_run_id BIGINT,
        raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_market_lifecycle_events (
        event_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT,
        asset_id TEXT,
        market_id BIGINT,
        gamma_market_id TEXT,
        condition_id TEXT,
        event_type TEXT NOT NULL,
        old_state TEXT,
        new_state TEXT,
        old_execution_eligible BOOLEAN,
        new_execution_eligible BOOLEAN,
        source TEXT NOT NULL DEFAULT 'registry',
        reason TEXT,
        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_token_universe_snapshots (
        snapshot_id BIGSERIAL PRIMARY KEY,
        generation BIGINT NOT NULL,
        snapshot_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        source TEXT NOT NULL DEFAULT 'registry',
        subscription_asset_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        execution_asset_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        added_subscription_asset_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        removed_subscription_asset_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        added_execution_asset_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        removed_execution_asset_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        subscription_count INTEGER NOT NULL DEFAULT 0,
        execution_count INTEGER NOT NULL DEFAULT 0,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_lob_subscription_targets (
        asset_id TEXT PRIMARY KEY,
        market_id BIGINT,
        condition_id TEXT,
        desired_subscribed BOOLEAN NOT NULL DEFAULT FALSE,
        actual_subscribed BOOLEAN NOT NULL DEFAULT FALSE,
        target_status TEXT NOT NULL DEFAULT 'inactive',
        subscription_state TEXT NOT NULL DEFAULT 'PENDING',
        subscription_generation BIGINT,
        reason TEXT,
        last_requested_at TIMESTAMPTZ,
        last_subscribe_request_at TIMESTAMPTZ,
        last_unsubscribe_request_at TIMESTAMPTZ,
        last_actual_subscribe_at TIMESTAMPTZ,
        last_actual_unsubscribe_at TIMESTAMPTZ,
        last_book_snapshot_at TIMESTAMPTZ,
        last_book_update_at TIMESTAMPTZ,
        last_l2_book_at TIMESTAMPTZ,
        last_l2_event_at TIMESTAMPTZ,
        last_book_quality TEXT,
        last_error TEXT,
        best_bid NUMERIC(20, 10),
        best_ask NUMERIC(20, 10),
        last_outbox_id BIGINT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_registry_sync_runs (
        run_id BIGSERIAL PRIMARY KEY,
        sync_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running',
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ,
        tokens_seen BIGINT NOT NULL DEFAULT 0,
        tokens_upserted BIGINT NOT NULL DEFAULT 0,
        transitions_written BIGINT NOT NULL DEFAULT 0,
        outbox_events BIGINT NOT NULL DEFAULT 0,
        error TEXT,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_registry_health_snapshots (
        health_id BIGSERIAL PRIMARY KEY,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        label TEXT NOT NULL,
        uptime_seconds NUMERIC(20, 3),
        tokens_total BIGINT NOT NULL DEFAULT 0,
        subscription_universe_count BIGINT NOT NULL DEFAULT 0,
        execution_universe_count BIGINT NOT NULL DEFAULT 0,
        stale_count BIGINT NOT NULL DEFAULT 0,
        pending_book_count BIGINT NOT NULL DEFAULT 0,
        pending_outbox_count BIGINT NOT NULL DEFAULT 0,
        generation BIGINT,
        last_full_sync_at TIMESTAMPTZ,
        last_delta_poll_at TIMESTAMPTZ,
        ws_connected BOOLEAN NOT NULL DEFAULT FALSE,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_registry_outbox (
        outbox_id BIGSERIAL PRIMARY KEY,
        event_type TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        market_id BIGINT,
        condition_id TEXT,
        generation BIGINT,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        published_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_runs (
        run_id BIGSERIAL PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'queued',
        market_slug TEXT NOT NULL,
        token_side TEXT NOT NULL,
        price_source TEXT NOT NULL,
        backtest_engine TEXT NOT NULL DEFAULT 'builtin',
        from_ts BIGINT,
        to_ts BIGINT,
        from_block BIGINT,
        to_block BIGINT,
        rows_processed BIGINT NOT NULL DEFAULT 0,
        error TEXT,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_run_progress (
        run_id BIGINT PRIMARY KEY REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        status TEXT NOT NULL DEFAULT 'queued',
        phase TEXT NOT NULL DEFAULT 'waiting for backtest worker',
        progress INTEGER NOT NULL DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
        current_x BIGINT,
        x_axis TEXT,
        rows_processed BIGINT NOT NULL DEFAULT 0,
        total_rows BIGINT,
        eta_seconds NUMERIC(20, 3),
        worker_id TEXT,
        error TEXT,
        run_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        artifact_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_financial_finalizations (
        run_id BIGINT PRIMARY KEY REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        status TEXT NOT NULL DEFAULT 'SETTLEMENT_PENDING',
        cutoff_ts TIMESTAMPTZ NOT NULL,
        execution_manifest_sha256 TEXT NOT NULL,
        settlement_catalog_sha256 TEXT,
        summary JSONB NOT NULL DEFAULT '{}'::jsonb,
        error_code TEXT,
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_market_settlement_snapshots (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        market_id BIGINT NOT NULL,
        cutoff_ts TIMESTAMPTZ NOT NULL,
        classification TEXT NOT NULL,
        record_sha256 TEXT NOT NULL,
        record JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (run_id, market_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_financial_positions (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        position_id TEXT NOT NULL,
        market_id BIGINT NOT NULL,
        condition_id TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        outcome TEXT NOT NULL,
        filled_size NUMERIC(38, 10) NOT NULL,
        entry_notional NUMERIC(38, 10) NOT NULL,
        recorded_fee NUMERIC(38, 10) NOT NULL DEFAULT 0,
        settlement_classification TEXT NOT NULL,
        settlement_complete BOOLEAN NOT NULL DEFAULT FALSE,
        payout_per_share NUMERIC(38, 18),
        settlement_payout NUMERIC(38, 10),
        net_pnl NUMERIC(38, 10),
        result JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (run_id, position_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.fill_only_market_settlement_catalog (
        cutoff_ts TIMESTAMPTZ NOT NULL,
        market_id BIGINT NOT NULL,
        classification TEXT NOT NULL,
        record_sha256 TEXT NOT NULL,
        record JSONB NOT NULL,
        source_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (cutoff_ts, market_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fill_only_market_settlement_catalog_class
    ON quant.fill_only_market_settlement_catalog (cutoff_ts, classification, market_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_parameters (
        run_id BIGINT PRIMARY KEY REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        entry_threshold NUMERIC(20, 10) NOT NULL,
        exit_threshold NUMERIC(20, 10) NOT NULL,
        stop_loss NUMERIC(20, 10) NOT NULL,
        take_profit NUMERIC(20, 10) NOT NULL,
        max_holding_bars INTEGER NOT NULL,
        initial_capital NUMERIC(38, 10) NOT NULL DEFAULT 100000,
        position_size NUMERIC(38, 10) NOT NULL DEFAULT 100,
        fee_bps NUMERIC(20, 10) NOT NULL DEFAULT 0,
        maker_fee_bps NUMERIC(20, 10),
        taker_fee_bps NUMERIC(20, 10),
        maker_rebate_bps NUMERIC(20, 10) NOT NULL DEFAULT 0,
        slippage_bps NUMERIC(20, 10) NOT NULL DEFAULT 0,
        liquidity_cap_pct NUMERIC(20, 10) NOT NULL DEFAULT 100,
        max_position_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        min_fill_pct NUMERIC(20, 10) NOT NULL DEFAULT 0,
        execution_price_mode TEXT NOT NULL DEFAULT 'ORDERFILLED_CROSS',
        execution_profile TEXT NOT NULL DEFAULT 'realistic',
        pml2_audit_mode TEXT NOT NULL DEFAULT 'CHAIN_ONLY',
        order_role TEXT NOT NULL DEFAULT 'taker',
        latency_blocks BIGINT NOT NULL DEFAULT 0,
        adverse_slippage_cents NUMERIC(20, 10) NOT NULL DEFAULT 0.005,
        fill_probability_haircut_pct NUMERIC(20, 10) NOT NULL DEFAULT 20,
        latency_seconds NUMERIC(20, 10) NOT NULL DEFAULT 0,
        max_book_staleness_seconds NUMERIC(20, 10) NOT NULL DEFAULT 900,
        allow_partial_fill BOOLEAN NOT NULL DEFAULT TRUE,
        min_fill_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        reject_on_stale_book BOOLEAN NOT NULL DEFAULT TRUE,
        final_valuation_mode TEXT NOT NULL DEFAULT 'SETTLEMENT',
        max_entry_price NUMERIC(20, 10) NOT NULL DEFAULT 1,
        min_exit_price NUMERIC(20, 10) NOT NULL DEFAULT 0,
        buy_limit_price NUMERIC(20, 10),
        sell_limit_price NUMERIC(20, 10),
        settlement_value NUMERIC(20, 10),
        gas_cost_per_order NUMERIC(38, 10) NOT NULL DEFAULT 0,
        settlement_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        redeem_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        capital_cost_bps NUMERIC(20, 10) NOT NULL DEFAULT 0,
        cancel_after_blocks BIGINT NOT NULL DEFAULT 0,
        cancel_ack_delay_blocks BIGINT NOT NULL DEFAULT 0,
        cancel_fail BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_metrics (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        metric_key TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        metric_group TEXT NOT NULL DEFAULT 'overview',
        value NUMERIC(38, 10),
        formatted_value TEXT,
        delta TEXT,
        status TEXT NOT NULL DEFAULT 'neutral',
        tooltip TEXT,
        sort_order INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (run_id, metric_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_equity (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        point_index INTEGER NOT NULL,
        x_axis TEXT NOT NULL,
        x_value BIGINT NOT NULL,
        equity NUMERIC(38, 10) NOT NULL,
        drawdown NUMERIC(38, 10) NOT NULL,
        drawdown_pct NUMERIC(20, 10) NOT NULL,
        cumulative_return NUMERIC(20, 10) NOT NULL,
        PRIMARY KEY (run_id, point_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_trades (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        trade_id TEXT NOT NULL,
        market_slug TEXT NOT NULL,
        token_side TEXT NOT NULL,
        side TEXT NOT NULL DEFAULT 'LONG',
        x_axis TEXT NOT NULL,
        entry_x BIGINT NOT NULL,
        exit_x BIGINT NOT NULL,
        entry_price NUMERIC(20, 10) NOT NULL,
        exit_price NUMERIC(20, 10) NOT NULL,
        size NUMERIC(38, 10) NOT NULL,
        notional NUMERIC(38, 10) NOT NULL,
        requested_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        filled_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        requested_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        filled_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        unfilled_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        fill_pct NUMERIC(20, 10) NOT NULL DEFAULT 100,
        fill_status TEXT NOT NULL DEFAULT 'FILLED',
        book_snapshot_id BIGINT,
        snapshot_version TEXT,
        staleness_seconds NUMERIC(20, 10),
        staleness_blocks BIGINT,
        avg_fill_price NUMERIC(20, 10),
        fill_probability NUMERIC(20, 10) NOT NULL DEFAULT 0,
        block_volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        trade_count BIGINT NOT NULL DEFAULT 0,
        available_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        execution_source TEXT NOT NULL DEFAULT 'unknown',
        fee_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        rebate_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        slippage_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        execution_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        entry_order_id TEXT,
        exit_order_id TEXT,
        pnl NUMERIC(38, 10) NOT NULL,
        pnl_pct NUMERIC(20, 10) NOT NULL,
        holding_bars INTEGER NOT NULL,
        exit_reason TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, trade_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_orders (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        order_id TEXT NOT NULL,
        signal_index INTEGER NOT NULL DEFAULT 0,
        trade_id TEXT,
        x_axis TEXT NOT NULL,
        signal_x BIGINT NOT NULL,
        submit_x BIGINT NOT NULL,
        decision_price NUMERIC(20, 10) NOT NULL DEFAULT 0,
        requested_price NUMERIC(20, 10),
        side TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'taker',
        order_type TEXT NOT NULL DEFAULT 'market_like_limit',
        status TEXT NOT NULL,
        requested_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        requested_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        expected_fill_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        expected_fill_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        actual_fill_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        actual_fill_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        filled_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        filled_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        unfilled_size NUMERIC(38, 10) NOT NULL DEFAULT 0,
        avg_fill_price NUMERIC(20, 10),
        fill_probability NUMERIC(20, 10) NOT NULL DEFAULT 0,
        fill_pct NUMERIC(20, 10) NOT NULL DEFAULT 0,
        block_volume NUMERIC(38, 10) NOT NULL DEFAULT 0,
        trade_count BIGINT NOT NULL DEFAULT 0,
        available_notional NUMERIC(38, 10) NOT NULL DEFAULT 0,
        participation_rate NUMERIC(20, 10) NOT NULL DEFAULT 0,
        fee_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        rebate_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        slippage_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        execution_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        latency_blocks BIGINT NOT NULL DEFAULT 0,
        latency_seconds NUMERIC(20, 10) NOT NULL DEFAULT 0,
        no_fill_reason TEXT,
        execution_source TEXT NOT NULL DEFAULT 'unknown',
        execution_evidence_type TEXT NOT NULL DEFAULT 'unknown',
        raw_candidate_event_count BIGINT NOT NULL DEFAULT 0,
        raw_consumed_event_count BIGINT NOT NULL DEFAULT 0,
        block_bar_crossed BOOLEAN NOT NULL DEFAULT false,
        block_bar_cross_field TEXT,
        block_bar_cross_price NUMERIC(20, 10),
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, order_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_ledger (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        ledger_id TEXT NOT NULL,
        order_id TEXT,
        trade_id TEXT,
        event_type TEXT NOT NULL,
        x_axis TEXT NOT NULL,
        x_value BIGINT NOT NULL,
        market_slug TEXT NOT NULL,
        token_side TEXT NOT NULL,
        shares_delta NUMERIC(38, 10) NOT NULL DEFAULT 0,
        cash_delta NUMERIC(38, 10) NOT NULL DEFAULT 0,
        fee NUMERIC(38, 10) NOT NULL DEFAULT 0,
        rebate NUMERIC(38, 10) NOT NULL DEFAULT 0,
        slippage_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        execution_cost NUMERIC(38, 10) NOT NULL DEFAULT 0,
        realized_pnl NUMERIC(38, 10) NOT NULL DEFAULT 0,
        position_after NUMERIC(38, 10) NOT NULL DEFAULT 0,
        cash_after NUMERIC(38, 10) NOT NULL DEFAULT 0,
        price NUMERIC(20, 10),
        source TEXT NOT NULL DEFAULT 'backtest',
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, ledger_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_events (
        run_id BIGINT NOT NULL REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        event_index INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        x_axis TEXT NOT NULL,
        x_value BIGINT NOT NULL,
        trade_id TEXT,
        price NUMERIC(20, 10),
        message TEXT,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, event_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.real_order_state_events (
        event_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        order_id TEXT,
        external_order_id TEXT,
        market_slug TEXT,
        token_id TEXT,
        token_side TEXT,
        event_time TIMESTAMPTZ,
        event_type TEXT NOT NULL DEFAULT 'order_state',
        source TEXT NOT NULL DEFAULT 'manual',
        submit_status TEXT,
        accepted_status TEXT,
        cancel_status TEXT,
        api_order_status TEXT,
        chain_order_status TEXT,
        clob_order_status TEXT,
        submit_at TIMESTAMPTZ,
        accepted_at TIMESTAMPTZ,
        cancel_submitted_at TIMESTAMPTZ,
        cancel_accepted_at TIMESTAMPTZ,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, external_order_id, event_type, event_time)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.real_order_state_collection_state (
        state_key TEXT PRIMARY KEY,
        source TEXT NOT NULL DEFAULT 'order-api',
        endpoint TEXT,
        params JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_event_time TIMESTAMPTZ,
        last_cursor TEXT,
        last_payload_count BIGINT NOT NULL DEFAULT 0,
        last_events_written BIGINT NOT NULL DEFAULT 0,
        last_error TEXT,
        last_success_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.external_source_import_state (
        state_key TEXT PRIMARY KEY,
        source_type TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        endpoint TEXT,
        params JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_payload_count BIGINT NOT NULL DEFAULT 0,
        last_rows_written BIGINT NOT NULL DEFAULT 0,
        last_error TEXT,
        last_success_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.external_signal_events (
        signal_event_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        signal_id TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        event_type TEXT NOT NULL DEFAULT 'external_signal',
        market_slug TEXT,
        token_id TEXT,
        token_side TEXT,
        observed_at TIMESTAMPTZ,
        observed_block BIGINT,
        latency_seconds NUMERIC(20, 10),
        payload_hash TEXT NOT NULL,
        resolution_source TEXT,
        settlement_rule TEXT,
        price_to_beat_source TEXT,
        oracle_source TEXT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, signal_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_calibration_orders (
        calibration_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL,
        sample_id TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        market_slug TEXT,
        token_id TEXT,
        token_side TEXT,
        side TEXT,
        role TEXT,
        order_type TEXT,
        observed_at TIMESTAMPTZ,
        observed_block BIGINT,
        simulated_order_id TEXT,
        live_order_id TEXT,
        requested_price NUMERIC(20, 10),
        requested_size NUMERIC(38, 10),
        simulated_status TEXT,
        live_status TEXT,
        status_error BOOLEAN NOT NULL DEFAULT FALSE,
        simulated_fill_price NUMERIC(20, 10),
        live_fill_price NUMERIC(20, 10),
        price_error NUMERIC(20, 10) NOT NULL DEFAULT 0,
        simulated_fill_size NUMERIC(38, 10),
        live_fill_size NUMERIC(38, 10),
        size_error NUMERIC(38, 10) NOT NULL DEFAULT 0,
        simulated_slippage NUMERIC(20, 10),
        live_slippage NUMERIC(20, 10),
        slippage_error NUMERIC(20, 10) NOT NULL DEFAULT 0,
        simulated_fee NUMERIC(38, 10),
        live_fee NUMERIC(38, 10),
        fee_error NUMERIC(38, 10) NOT NULL DEFAULT 0,
        simulated_rebate NUMERIC(38, 10),
        live_rebate NUMERIC(38, 10),
        rebate_error NUMERIC(38, 10) NOT NULL DEFAULT 0,
        simulated_cash_delta NUMERIC(38, 10),
        live_cash_delta NUMERIC(38, 10),
        cash_error NUMERIC(38, 10) NOT NULL DEFAULT 0,
        simulated_position_delta NUMERIC(38, 10),
        live_position_delta NUMERIC(38, 10),
        position_error NUMERIC(38, 10) NOT NULL DEFAULT 0,
        simulated_pnl NUMERIC(38, 10),
        live_pnl NUMERIC(38, 10),
        pnl_error NUMERIC(38, 10) NOT NULL DEFAULT 0,
        simulated_latency_seconds NUMERIC(20, 10),
        live_latency_seconds NUMERIC(20, 10),
        latency_error_seconds NUMERIC(20, 10) NOT NULL DEFAULT 0,
        liquidity_bucket TEXT,
        volatility_bucket TEXT,
        time_to_expiry_bucket TEXT,
        verdict TEXT NOT NULL DEFAULT 'unknown',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, sample_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.real_backtest_cost_events (
        event_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE,
        cost_id TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        market_slug TEXT,
        token_id TEXT,
        token_side TEXT,
        order_id TEXT,
        trade_id TEXT,
        event_type TEXT NOT NULL,
        observed_at TIMESTAMPTZ,
        observed_block BIGINT,
        amount NUMERIC(38, 10) NOT NULL DEFAULT 0,
        currency TEXT NOT NULL DEFAULT 'USDC',
        tx_hash TEXT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, cost_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_cost_calibration (
        cost_calibration_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL,
        sample_id TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        market_slug TEXT,
        token_id TEXT,
        token_side TEXT,
        order_id TEXT,
        trade_id TEXT,
        event_type TEXT NOT NULL,
        simulated_amount NUMERIC(38, 10) NOT NULL DEFAULT 0,
        live_amount NUMERIC(38, 10) NOT NULL DEFAULT 0,
        amount_error NUMERIC(38, 10) NOT NULL DEFAULT 0,
        simulated_count BIGINT NOT NULL DEFAULT 0,
        live_count BIGINT NOT NULL DEFAULT 0,
        observed_at TIMESTAMPTZ,
        observed_block BIGINT,
        verdict TEXT NOT NULL DEFAULT 'unknown',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, sample_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.platform_incidents (
        incident_id BIGSERIAL PRIMARY KEY,
        incident_key TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        severity TEXT NOT NULL DEFAULT 'info',
        component TEXT NOT NULL DEFAULT 'platform',
        title TEXT NOT NULL DEFAULT '',
        description TEXT,
        market_slug TEXT,
        token_id TEXT,
        token_side TEXT,
        start_ts TIMESTAMPTZ,
        end_ts TIMESTAMPTZ,
        start_block BIGINT,
        end_block BIGINT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, incident_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.execution_profile_overrides (
        override_id BIGSERIAL PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'pending',
        scope TEXT NOT NULL DEFAULT 'overall',
        bucket_field TEXT,
        bucket_value TEXT,
        execution_profile TEXT NOT NULL,
        order_role TEXT,
        latency_blocks BIGINT NOT NULL DEFAULT 0,
        adverse_slippage_cents NUMERIC(20, 10) NOT NULL DEFAULT 0,
        fill_probability_haircut_pct NUMERIC(20, 10) NOT NULL DEFAULT 0,
        source TEXT NOT NULL DEFAULT 'manual',
        calibration_sample_count BIGINT NOT NULL DEFAULT 0,
        calibration_window_start TIMESTAMPTZ,
        calibration_window_end TIMESTAMPTZ,
        reason TEXT,
        evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
        approved_by TEXT,
        approved_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.strategy_activation_decisions (
        decision_id BIGSERIAL PRIMARY KEY,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL,
        target_mode TEXT NOT NULL,
        activation_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        decision_verdict TEXT NOT NULL DEFAULT 'missing',
        strategy_name TEXT NOT NULL DEFAULT 'unknown',
        strategy_version TEXT NOT NULL DEFAULT 'unknown',
        market_slug TEXT,
        token_side TEXT,
        price_source TEXT,
        actual_execution_engine TEXT NOT NULL DEFAULT 'unknown',
        promotion_verdict TEXT NOT NULL DEFAULT 'missing',
        paper_promotion_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        production_promotion_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        paper_live_evidence_gate_status TEXT NOT NULL DEFAULT 'missing',
        paper_live_paper_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        paper_live_live_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        allowed_next_modes JSONB NOT NULL DEFAULT '[]'::jsonb,
        blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        review_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        missing_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        decision_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        requested_by TEXT,
        notes TEXT,
        promotion_gate_report JSONB NOT NULL DEFAULT '{}'::jsonb,
        paper_live_evidence_gate_report JSONB NOT NULL DEFAULT '{}'::jsonb,
        artifact_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.strategy_enable_state (
        enable_id BIGSERIAL PRIMARY KEY,
        decision_id BIGINT REFERENCES quant.strategy_activation_decisions(decision_id) ON DELETE SET NULL,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL,
        target_mode TEXT NOT NULL,
        strategy_name TEXT NOT NULL DEFAULT 'unknown',
        strategy_version TEXT NOT NULL DEFAULT 'unknown',
        market_slug TEXT NOT NULL DEFAULT '',
        token_side TEXT NOT NULL DEFAULT '',
        price_source TEXT NOT NULL DEFAULT '',
        actual_execution_engine TEXT NOT NULL DEFAULT 'unknown',
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        enable_status TEXT NOT NULL DEFAULT 'disabled',
        activation_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        decision_verdict TEXT NOT NULL DEFAULT 'missing',
        promotion_verdict TEXT NOT NULL DEFAULT 'missing',
        paper_live_evidence_gate_status TEXT NOT NULL DEFAULT 'missing',
        paper_live_paper_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        paper_live_live_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        review_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        missing_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        enable_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        requested_by TEXT,
        reason TEXT,
        activation_decision JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_benchmark_runs (
        benchmark_id BIGSERIAL PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'queued',
        universe_type TEXT NOT NULL DEFAULT 'preset',
        universe_name TEXT NOT NULL,
        market_count INTEGER NOT NULL DEFAULT 0,
        strategy_name TEXT NOT NULL,
        parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
        profiles JSONB NOT NULL DEFAULT '{}'::jsonb,
        summary JSONB NOT NULL DEFAULT '{}'::jsonb,
        data_version TEXT,
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_benchmark_rows (
        benchmark_id BIGINT NOT NULL REFERENCES quant.quant_backtest_benchmark_runs(benchmark_id) ON DELETE CASCADE,
        row_index INTEGER NOT NULL,
        market_id BIGINT,
        market_slug TEXT NOT NULL,
        title TEXT,
        event_time TIMESTAMPTZ,
        outcome TEXT,
        signal_time TIMESTAMPTZ,
        fast_status TEXT,
        accurate_status TEXT,
        fast_pnl NUMERIC(38, 10) NOT NULL DEFAULT 0,
        accurate_pnl NUMERIC(38, 10) NOT NULL DEFAULT 0,
        pnl_diff NUMERIC(38, 10) NOT NULL DEFAULT 0,
        fast_fill_block BIGINT NOT NULL DEFAULT 0,
        accurate_fill_block BIGINT NOT NULL DEFAULT 0,
        data_quality TEXT NOT NULL DEFAULT 'unknown',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (benchmark_id, row_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.quant_backtest_benchmark_artifacts (
        benchmark_id BIGINT NOT NULL REFERENCES quant.quant_backtest_benchmark_runs(benchmark_id) ON DELETE CASCADE,
        artifact_key TEXT NOT NULL,
        artifact_kind TEXT NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (benchmark_id, artifact_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.parameter_search_batches (
        batch_id BIGSERIAL PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'queued',
        source TEXT NOT NULL DEFAULT 'parameter-search-scheduler',
        universe_name TEXT NOT NULL DEFAULT '',
        strategy_name TEXT NOT NULL DEFAULT 'unknown',
        strategy_version TEXT NOT NULL DEFAULT 'unknown',
        plan JSONB NOT NULL DEFAULT '{}'::jsonb,
        summary JSONB NOT NULL DEFAULT '{}'::jsonb,
        planned_run_count INTEGER NOT NULL DEFAULT 0,
        queued_count INTEGER NOT NULL DEFAULT 0,
        running_count INTEGER NOT NULL DEFAULT 0,
        succeeded_count INTEGER NOT NULL DEFAULT 0,
        failed_count INTEGER NOT NULL DEFAULT 0,
        retryable_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 1,
        error TEXT,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.parameter_search_batch_items (
        item_id BIGSERIAL PRIMARY KEY,
        batch_id BIGINT NOT NULL REFERENCES quant.parameter_search_batches(batch_id) ON DELETE CASCADE,
        item_index INTEGER NOT NULL,
        item_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        parameter_fingerprint TEXT NOT NULL DEFAULT '',
        evidence_mode TEXT NOT NULL DEFAULT '',
        parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
        request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        result_row JSONB NOT NULL DEFAULT '{}'::jsonb,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 1,
        worker_id TEXT,
        error TEXT,
        claimed_at TIMESTAMPTZ,
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (batch_id, item_index),
        UNIQUE (batch_id, item_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.production_parameter_staging (
        staging_id BIGSERIAL PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'pending',
        source TEXT NOT NULL DEFAULT 'parameter-search-results',
        benchmark_id BIGINT REFERENCES quant.quant_backtest_benchmark_runs(benchmark_id) ON DELETE SET NULL,
        run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL,
        strategy_name TEXT NOT NULL DEFAULT 'unknown',
        strategy_version TEXT NOT NULL DEFAULT 'unknown',
        universe_name TEXT NOT NULL DEFAULT '',
        parameter_fingerprint TEXT NOT NULL DEFAULT '',
        parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
        staging_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        coverage_pct NUMERIC(20, 10) NOT NULL DEFAULT 0,
        robustness_verdict TEXT NOT NULL DEFAULT 'missing',
        default_action TEXT NOT NULL DEFAULT 'do_not_stage',
        blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        reason TEXT,
        evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
        parameter_search_results JSONB NOT NULL DEFAULT '{}'::jsonb,
        approved_by TEXT,
        approved_at TIMESTAMPTZ,
        reviewed_by TEXT,
        reviewed_at TIMESTAMPTZ,
        review_note TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
)


ALTER_TABLE_SQL: tuple[str, ...] = (
    "ALTER TABLE quant.market_token_block_close_rebuild_runs ADD COLUMN IF NOT EXISTS serving_semantics TEXT",
    "ALTER TABLE quant.market_token_block_close_rebuild_runs ADD COLUMN IF NOT EXISTS shadow_tablespace TEXT",
    "ALTER TABLE quant.market_token_block_close_rebuild_runs ADD COLUMN IF NOT EXISTS expected_canonical_generation BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close_rebuild_runs ADD COLUMN IF NOT EXISTS expected_canonical_rebuild_id UUID",
    "ALTER TABLE quant.market_token_block_close_rebuild_runs ADD COLUMN IF NOT EXISTS published_generation BIGINT",
    "ALTER TABLE quant.market_token_block_close_rebuild_chunks ALTER COLUMN market_id DROP NOT NULL",
    "ALTER TABLE quant.market_token_metadata ADD COLUMN IF NOT EXISTS token_id_hex TEXT",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS yes_probability_close NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS block_timestamp TIMESTAMPTZ",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS open_price NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS high_price NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS low_price NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS vwap_price NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS yes_probability_vwap NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS close_raw_price NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS close_price_source TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS close_maker_amount NUMERIC(38, 0)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS close_taker_amount NUMERIC(38, 0)",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS raw_trade_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS internal_filtered_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS invalid_size_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS invalid_price_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS amount_ratio_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS raw_price_fallback_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS extreme_trade_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS anomaly_flags JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS buy_volume NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS sell_volume NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS first_log_index INTEGER",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS last_log_index INTEGER",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS first_tx_hash TEXT",
    "ALTER TABLE quant.market_token_block_close ADD COLUMN IF NOT EXISTS last_tx_hash TEXT",
    "ALTER TABLE quant.market_token_block_close ALTER COLUMN source SET DEFAULT 'clean_orderfilled_fact'",
    "ALTER TABLE quant.market_token_api_trades ADD COLUMN IF NOT EXISTS yes_probability NUMERIC(20, 10)",
    "ALTER TABLE quant.market_token_api_trades ADD COLUMN IF NOT EXISTS source_endpoint TEXT NOT NULL DEFAULT 'https://data-api.polymarket.com/trades'",
    "ALTER TABLE quant.market_token_api_trades ADD COLUMN IF NOT EXISTS taker_only BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.market_token_api_trades ALTER COLUMN source SET DEFAULT 'data-api-trades'",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS key_version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS entity_type TEXT NOT NULL DEFAULT 'event'",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS tile_kind TEXT NOT NULL DEFAULT 'series'",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS point_format TEXT NOT NULL DEFAULT 'lite'",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS window_from_x BIGINT",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS window_to_x BIGINT",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS cache_ttl_seconds INTEGER",
    "ALTER TABLE quant.quant_price_series_tiles ADD COLUMN IF NOT EXISTS updated_reason TEXT",
    "ALTER TABLE quant.quant_backtest_runs ADD COLUMN IF NOT EXISTS backtest_engine TEXT NOT NULL DEFAULT 'builtin'",
    "ALTER TABLE quant.quant_backtest_run_progress ADD COLUMN IF NOT EXISTS worker_id TEXT",
    "ALTER TABLE quant.quant_backtest_run_progress ADD COLUMN IF NOT EXISTS error TEXT",
    "ALTER TABLE quant.quant_backtest_run_progress ADD COLUMN IF NOT EXISTS run_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.quant_backtest_run_progress ADD COLUMN IF NOT EXISTS artifact_summary JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.quant_backtest_run_progress ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()",
    "ALTER TABLE quant.quant_backtest_run_progress ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ",
    "ALTER TABLE quant.quant_backtest_run_progress ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS fee_bps NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS maker_fee_bps NUMERIC(20, 10)",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS taker_fee_bps NUMERIC(20, 10)",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS maker_rebate_bps NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS slippage_bps NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS liquidity_cap_pct NUMERIC(20, 10) NOT NULL DEFAULT 100",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS max_position_notional NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS min_fill_pct NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS execution_price_mode TEXT NOT NULL DEFAULT 'ORDERFILLED_CROSS'",
    "ALTER TABLE quant.quant_backtest_parameters ALTER COLUMN execution_price_mode SET DEFAULT 'ORDERFILLED_CROSS'",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS execution_profile TEXT NOT NULL DEFAULT 'realistic'",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS pml2_audit_mode TEXT NOT NULL DEFAULT 'FULL'",
    "ALTER TABLE quant.quant_backtest_parameters ALTER COLUMN pml2_audit_mode SET DEFAULT 'CHAIN_ONLY'",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS order_role TEXT NOT NULL DEFAULT 'taker'",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS latency_blocks BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS adverse_slippage_cents NUMERIC(20, 10) NOT NULL DEFAULT 0.005",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS fill_probability_haircut_pct NUMERIC(20, 10) NOT NULL DEFAULT 20",
    "ALTER TABLE quant.quant_backtest_parameters ALTER COLUMN adverse_slippage_cents SET DEFAULT 0.005",
    "ALTER TABLE quant.quant_backtest_parameters ALTER COLUMN fill_probability_haircut_pct SET DEFAULT 20",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS latency_seconds NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS max_book_staleness_seconds NUMERIC(20, 10) NOT NULL DEFAULT 900",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS allow_partial_fill BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS min_fill_size NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS reject_on_stale_book BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS final_valuation_mode TEXT NOT NULL DEFAULT 'SETTLEMENT'",
    "ALTER TABLE quant.quant_backtest_parameters ALTER COLUMN final_valuation_mode SET DEFAULT 'SETTLEMENT'",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS max_entry_price NUMERIC(20, 10) NOT NULL DEFAULT 1",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS min_exit_price NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS buy_limit_price NUMERIC(20, 10)",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS sell_limit_price NUMERIC(20, 10)",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS settlement_value NUMERIC(20, 10)",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS gas_cost_per_order NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS settlement_cost NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS redeem_cost NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS capital_cost_bps NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS cancel_after_blocks BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS cancel_ack_delay_blocks BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_parameters ADD COLUMN IF NOT EXISTS cancel_fail BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'order-api'",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS endpoint TEXT",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS params JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS last_event_time TIMESTAMPTZ",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS last_cursor TEXT",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS last_payload_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS last_events_written BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS last_error TEXT",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.real_order_state_collection_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'external'",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS endpoint TEXT",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS params JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS last_payload_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS last_rows_written BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS last_error TEXT",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.external_source_import_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS signal_id TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'external_signal'",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS market_slug TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS token_id TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS token_side TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS observed_block BIGINT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS latency_seconds NUMERIC(20, 10)",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS payload_hash TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS resolution_source TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS settlement_rule TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS price_to_beat_source TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS oracle_source TEXT",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.external_signal_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE CASCADE",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS cost_id TEXT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS market_slug TEXT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS token_id TEXT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS token_side TEXT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS order_id TEXT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS trade_id TEXT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'COST'",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS observed_block BIGINT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS amount NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'USDC'",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS tx_hash TEXT",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.real_backtest_cost_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.quant_backtest_calibration_orders ADD COLUMN IF NOT EXISTS simulated_pnl NUMERIC(38, 10)",
    "ALTER TABLE quant.quant_backtest_calibration_orders ADD COLUMN IF NOT EXISTS live_pnl NUMERIC(38, 10)",
    "ALTER TABLE quant.quant_backtest_calibration_orders ADD COLUMN IF NOT EXISTS pnl_error NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS sample_id TEXT",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS market_slug TEXT",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS token_id TEXT",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS token_side TEXT",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS order_id TEXT",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS trade_id TEXT",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'COST'",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS simulated_amount NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS live_amount NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS amount_error NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS simulated_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS live_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS observed_block BIGINT",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS verdict TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.quant_backtest_cost_calibration ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS incident_key TEXT",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'info'",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS component TEXT NOT NULL DEFAULT 'platform'",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS market_slug TEXT",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS token_id TEXT",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS token_side TEXT",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS start_block BIGINT",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS end_block BIGINT",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.platform_incidents ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending'",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'overall'",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS bucket_field TEXT",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS bucket_value TEXT",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS execution_profile TEXT NOT NULL DEFAULT 'realistic'",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS order_role TEXT",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS latency_blocks BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS adverse_slippage_cents NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS fill_probability_haircut_pct NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS calibration_sample_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS calibration_window_start TIMESTAMPTZ",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS calibration_window_end TIMESTAMPTZ",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS reason TEXT",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS evidence JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS approved_by TEXT",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.execution_profile_overrides ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'queued'",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'parameter-search-scheduler'",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS universe_name TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS strategy_name TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS strategy_version TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS plan JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS summary JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS planned_run_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS queued_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS running_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS succeeded_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS failed_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS retryable_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS error TEXT",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS created_by TEXT",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ",
    "ALTER TABLE quant.parameter_search_batches ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS batch_id BIGINT REFERENCES quant.parameter_search_batches(batch_id) ON DELETE CASCADE",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS item_index INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS item_key TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'queued'",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS parameter_fingerprint TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS evidence_mode TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS parameters JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS request_payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS result_row JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS worker_id TEXT",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS error TEXT",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.parameter_search_batch_items ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending'",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'parameter-search-results'",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS benchmark_id BIGINT REFERENCES quant.quant_backtest_benchmark_runs(benchmark_id) ON DELETE SET NULL",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS strategy_name TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS strategy_version TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS universe_name TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS parameter_fingerprint TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS parameters JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS staging_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS coverage_pct NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS robustness_verdict TEXT NOT NULL DEFAULT 'missing'",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS default_action TEXT NOT NULL DEFAULT 'do_not_stage'",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS reason TEXT",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS evidence JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS parameter_search_results JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS approved_by TEXT",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS reviewed_by TEXT",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS review_note TEXT",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.production_parameter_staging ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS target_mode TEXT NOT NULL DEFAULT 'paper'",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS activation_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS decision_verdict TEXT NOT NULL DEFAULT 'missing'",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS strategy_name TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS strategy_version TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS market_slug TEXT",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS token_side TEXT",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS price_source TEXT",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS actual_execution_engine TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS promotion_verdict TEXT NOT NULL DEFAULT 'missing'",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS paper_promotion_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS production_promotion_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS paper_live_evidence_gate_status TEXT NOT NULL DEFAULT 'missing'",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS paper_live_paper_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS paper_live_live_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS allowed_next_modes JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS review_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS missing_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS decision_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS requested_by TEXT",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS notes TEXT",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS promotion_gate_report JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS paper_live_evidence_gate_report JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS artifact_summary JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.strategy_activation_decisions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS decision_id BIGINT REFERENCES quant.strategy_activation_decisions(decision_id) ON DELETE SET NULL",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES quant.quant_backtest_runs(run_id) ON DELETE SET NULL",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS target_mode TEXT NOT NULL DEFAULT 'paper'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS strategy_name TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS strategy_version TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS market_slug TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS token_side TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS price_source TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS actual_execution_engine TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS enable_status TEXT NOT NULL DEFAULT 'disabled'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS activation_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS decision_verdict TEXT NOT NULL DEFAULT 'missing'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS promotion_verdict TEXT NOT NULL DEFAULT 'missing'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS paper_live_evidence_gate_status TEXT NOT NULL DEFAULT 'missing'",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS paper_live_paper_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS paper_live_live_allowed BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS review_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS missing_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS enable_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS requested_by TEXT",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS reason TEXT",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS activation_decision JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.strategy_enable_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS requested_notional NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS filled_notional NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS requested_size NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS filled_size NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS unfilled_size NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS fill_pct NUMERIC(20, 10) NOT NULL DEFAULT 100",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS fill_status TEXT NOT NULL DEFAULT 'FILLED'",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS book_snapshot_id BIGINT",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS snapshot_version TEXT",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS staleness_seconds NUMERIC(20, 10)",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS staleness_blocks BIGINT",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS avg_fill_price NUMERIC(20, 10)",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS fill_probability NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS block_volume NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS trade_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS available_notional NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS execution_source TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS fee_cost NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS rebate_cost NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS slippage_cost NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS execution_cost NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS entry_order_id TEXT",
    "ALTER TABLE quant.quant_backtest_trades ADD COLUMN IF NOT EXISTS exit_order_id TEXT",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS rebate_cost NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS expected_fill_size NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS expected_fill_notional NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS actual_fill_size NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS actual_fill_notional NUMERIC(38, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS participation_rate NUMERIC(20, 10) NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS execution_evidence_type TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS raw_candidate_event_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS raw_consumed_event_count BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS block_bar_crossed BOOLEAN NOT NULL DEFAULT false",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS block_bar_cross_field TEXT",
    "ALTER TABLE quant.quant_backtest_orders ADD COLUMN IF NOT EXISTS block_bar_cross_price NUMERIC(20, 10)",
    "ALTER TABLE quant.market_event_members ADD COLUMN IF NOT EXISTS grouping_confidence TEXT NOT NULL DEFAULT 'official'",
    "ALTER TABLE quant.market_event_metadata ADD COLUMN IF NOT EXISTS grouping_confidence TEXT NOT NULL DEFAULT 'official'",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS block_number BIGINT",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS snapshot_timestamp TIMESTAMPTZ",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS snapshot_version TEXT",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS market_id BIGINT",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS condition_id TEXT",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS market_slug TEXT",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'snapshot'",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS book_generation BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE quant.clob_orderbook_snapshots ADD COLUMN IF NOT EXISTS storage_tier TEXT NOT NULL DEFAULT 'sampled'",
    "ALTER TABLE quant.paper_registry_outbox ADD COLUMN IF NOT EXISTS run_id BIGINT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS status_present BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS completion_status TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS token_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS subscription_reason TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS execution_reason TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS book_status TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS latest_book_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS book_source TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS storage_tier TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS last_source TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS last_run_id BIGINT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS last_transition_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS current_tick_size NUMERIC(20, 10)",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS min_order_size NUMERIC(38, 10)",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS book_seen_first_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS book_seen_last_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS last_l2_event_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS last_book_update_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS last_book_quality TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS winning_asset_id TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS winning_outcome TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS resolution_status TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS resolution_source TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS resolved_time TIMESTAMPTZ",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS metadata_hash TEXT",
    "ALTER TABLE quant.paper_market_registry_tokens ADD COLUMN IF NOT EXISTS token_mapping_confidence TEXT",
    "ALTER TABLE quant.paper_market_registry_markets ADD COLUMN IF NOT EXISTS metadata_hash TEXT",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS subscription_state TEXT NOT NULL DEFAULT 'PENDING'",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS subscription_generation BIGINT",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_subscribe_request_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_unsubscribe_request_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_actual_subscribe_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_actual_unsubscribe_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_book_snapshot_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_book_update_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_l2_book_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_l2_event_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_book_quality TEXT",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS last_error TEXT",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS best_bid NUMERIC(20, 10)",
    "ALTER TABLE quant.paper_lob_subscription_targets ADD COLUMN IF NOT EXISTS best_ask NUMERIC(20, 10)",
    "ALTER TABLE quant.clob_l2_archive_manifest ADD COLUMN IF NOT EXISTS connection_generation BIGINT",
    "ALTER TABLE quant.clob_l2_archive_manifest ADD COLUMN IF NOT EXISTS event_type_counts JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE quant.clob_l2_archive_manifest ADD COLUMN IF NOT EXISTS first_local_seq BIGINT",
    "ALTER TABLE quant.clob_l2_archive_manifest ADD COLUMN IF NOT EXISTS last_local_seq BIGINT",
    "ALTER TABLE quant.clob_l2_archive_manifest ADD COLUMN IF NOT EXISTS writer_version TEXT NOT NULL DEFAULT 'l2_archive_v1'",
    "ALTER TABLE quant.clob_l2_archive_manifest ADD COLUMN IF NOT EXISTS committed_at TIMESTAMPTZ",
    "ALTER TABLE quant.paper_registry_outbox ADD COLUMN IF NOT EXISTS generation BIGINT",
    "ALTER TABLE quant.paper_registry_outbox ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.paper_registry_outbox ADD COLUMN IF NOT EXISTS last_error TEXT",
    "ALTER TABLE quant.paper_registry_outbox ALTER COLUMN created_at SET DEFAULT clock_timestamp()",
    """
    ALTER TABLE quant.paper_registry_outbox SET (
        autovacuum_vacuum_scale_factor = 0.01,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_analyze_scale_factor = 0.01,
        autovacuum_analyze_threshold = 10000
    )
    """,
    "ALTER TABLE quant.paper_registry_health_snapshots ALTER COLUMN recorded_at SET DEFAULT clock_timestamp()",
)


DATA_MIGRATION_SQL: tuple[str, ...] = (
    """
    INSERT INTO quant.market_token_block_close_generation (singleton)
    VALUES (TRUE)
    ON CONFLICT (singleton) DO NOTHING
    """,
    """
    INSERT INTO quant.quant_backtest_run_progress (
        run_id, status, phase, progress, rows_processed, total_rows,
        error, created_at, started_at, finished_at, updated_at
    )
    SELECT
        run_id,
        status,
        CASE
            WHEN status = 'queued' THEN 'waiting for backtest worker'
            WHEN status = 'running' THEN 'running backtest'
            WHEN status = 'succeeded' THEN 'backtest artifacts ready'
            WHEN status = 'failed' THEN 'backtest failed'
            ELSE status
        END,
        CASE WHEN status = 'succeeded' THEN 100 ELSE 0 END,
        rows_processed,
        CASE WHEN status = 'succeeded' THEN rows_processed ELSE NULL END,
        error,
        created_at,
        started_at,
        finished_at,
        COALESCE(finished_at, started_at, created_at, now())
    FROM quant.quant_backtest_runs
    ON CONFLICT (run_id) DO NOTHING
    """,
    """
    UPDATE quant.quant_backtest_run_progress progress
    SET error = run.error,
        created_at = run.created_at,
        started_at = run.started_at,
        finished_at = run.finished_at,
        run_snapshot = jsonb_build_object(
            'market_slug', run.market_slug,
            'token_side', run.token_side,
            'price_source', run.price_source,
            'backtest_engine', run.backtest_engine,
            'from_ts', run.from_ts,
            'to_ts', run.to_ts,
            'from_block', run.from_block,
            'to_block', run.to_block,
            'parameters', jsonb_build_object(
                'entry_threshold', params.entry_threshold,
                'exit_threshold', params.exit_threshold,
                'stop_loss', params.stop_loss,
                'take_profit', params.take_profit,
                'max_holding_bars', params.max_holding_bars,
                'initial_capital', params.initial_capital,
                'position_size', params.position_size,
                'fee_bps', params.fee_bps,
                'maker_fee_bps', params.maker_fee_bps,
                'taker_fee_bps', params.taker_fee_bps,
                'maker_rebate_bps', params.maker_rebate_bps,
                'slippage_bps', params.slippage_bps,
                'liquidity_cap_pct', params.liquidity_cap_pct,
                'max_position_notional', params.max_position_notional,
                'min_fill_pct', params.min_fill_pct,
                'execution_price_mode', params.execution_price_mode,
                'execution_profile', params.execution_profile,
                'order_role', params.order_role,
                'latency_blocks', params.latency_blocks,
                'adverse_slippage_cents', params.adverse_slippage_cents,
                'fill_probability_haircut_pct', params.fill_probability_haircut_pct,
                'latency_seconds', params.latency_seconds,
                'max_book_staleness_seconds', params.max_book_staleness_seconds,
                'allow_partial_fill', params.allow_partial_fill,
                'min_fill_size', params.min_fill_size,
                'reject_on_stale_book', params.reject_on_stale_book,
                'final_valuation_mode', params.final_valuation_mode,
                'max_entry_price', params.max_entry_price,
                'min_exit_price', params.min_exit_price,
                'buy_limit_price', params.buy_limit_price,
                'sell_limit_price', params.sell_limit_price,
                'settlement_value', params.settlement_value
            )
        )
    FROM quant.quant_backtest_runs run
    LEFT JOIN quant.quant_backtest_parameters params ON params.run_id = run.run_id
    WHERE progress.run_id = run.run_id
      AND progress.run_snapshot = '{}'::jsonb
    """,
    """
    UPDATE quant.clob_orderbook_snapshots
    SET snapshot_timestamp = fetched_at
    WHERE snapshot_timestamp IS NULL
      AND fetched_at IS NOT NULL
    """,
    """
    CREATE OR REPLACE VIEW quant.paper_candidate_market_registry_tokens AS
    SELECT *
    FROM quant.paper_market_registry_tokens
    WHERE active = TRUE
      AND closed = FALSE
      AND resolved = FALSE
      AND archived = FALSE
      AND deprecated = FALSE
      AND status_present = TRUE
      AND condition_id IS NOT NULL
      AND condition_id <> ''
      AND token_count >= 2
    """,
    """
    CREATE OR REPLACE VIEW quant.paper_active_market_registry_tokens AS
    SELECT *
    FROM quant.paper_market_registry_tokens
    WHERE subscription_eligible = TRUE
      AND market_state NOT IN ('CLOSING', 'RESOLVED', 'ARCHIVED', 'INVALID_METADATA')
    """,
    """
    CREATE OR REPLACE VIEW quant.paper_execution_market_registry_tokens AS
    SELECT
        r.asset_id,
        r.market_id,
        r.gamma_market_id,
        r.condition_id,
        r.market_slug,
        r.market_title,
        r.outcome_name,
        r.outcome_index,
        r.active,
        r.closed,
        r.resolved,
        r.archived,
        r.deprecated,
        r.status_present,
        r.completion_status,
        r.token_count,
        r.market_state,
        r.subscription_eligible,
        r.execution_eligible,
        r.subscription_reason,
        r.execution_reason,
        COALESCE(t.last_book_quality, r.book_quality) AS book_quality,
        r.book_status,
        GREATEST(
            COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
            COALESCE(r.latest_book_at, 'epoch'::timestamptz)
        ) AS latest_book_at,
        r.book_age_ms,
        COALESCE(t.best_bid, r.best_bid) AS best_bid,
        COALESCE(t.best_ask, r.best_ask) AS best_ask,
        r.book_source,
        r.storage_tier,
        r.desired_subscribed,
        r.actual_subscribed,
        r.last_source,
        r.last_run_id,
        r.last_checked_at,
        r.last_transition_at,
        r.raw_metadata,
        r.updated_at,
        r.current_tick_size,
        r.min_order_size,
        r.book_seen_first_at,
        r.book_seen_last_at,
        t.last_l2_event_at AS last_l2_event_at,
        t.last_book_update_at AS last_book_update_at,
        t.last_book_quality AS last_book_quality,
        r.winning_asset_id,
        r.winning_outcome,
        r.resolution_status,
        r.resolution_source,
        r.resolved_time,
        r.metadata_hash,
        r.token_mapping_confidence
    FROM quant.paper_market_registry_tokens r
    JOIN quant.paper_lob_subscription_targets t ON t.asset_id = r.asset_id
    JOIN quant.clob_l2_subscription_state s ON s.asset_id = r.asset_id
    JOIN quant.clob_l2_current_coverage c ON c.asset_id = r.asset_id
    JOIN quant.clob_l2_connection_state cs
      ON cs.shard_id = s.shard_id
     AND cs.connection_id = s.connection_id
     AND cs.connection_generation = s.connection_generation
    WHERE r.execution_eligible = TRUE
      AND r.market_state = 'LIVE'
      AND t.desired_subscribed = TRUE
      AND t.actual_subscribed = TRUE
      AND s.state IN ('SNAPSHOT_CONFIRMED', 'LIVE_ACTIVE', 'LIVE_QUIET')
      AND s.first_snapshot_at IS NOT NULL
      AND c.subscription_state IN ('SNAPSHOT_CONFIRMED', 'LIVE_ACTIVE', 'LIVE_QUIET')
      AND c.coverage_grade IN ('A_PLUS', 'A', 'B')
      AND c.has_gap = FALSE
      AND c.connection_id = s.connection_id
      AND c.connection_generation = s.connection_generation
      AND cs.transport_state = 'CONNECTED'
      AND cs.last_status_at >= clock_timestamp() - interval '60 seconds'
      AND cs.last_message_at >= clock_timestamp() - interval '60 seconds'
      AND t.last_book_quality IN ('READY_HIGH', 'READY_MEDIUM')
      AND COALESCE(t.last_error, '') NOT LIKE 'l2_archive_ws_gap_%'
      AND t.best_bid IS NOT NULL
      AND t.best_ask IS NOT NULL
      AND t.best_bid > 0
      AND t.best_ask > 0
    """,
    """
    UPDATE quant.paper_market_registry_tokens
    SET
        book_status = 'no_clob_book',
        book_quality = 'NO_CLOB_BOOK',
        raw_metadata = jsonb_set(
            jsonb_set(
                COALESCE(raw_metadata, '{}'::jsonb),
                '{probe,book_status}',
                '"no_clob_book"'::jsonb,
                TRUE
            ),
            '{probe,book_quality}',
            '"NO_CLOB_BOOK"'::jsonb,
            TRUE
        )
    WHERE book_status = 'probe_error'
      AND raw_metadata->'probe'->>'error' = 'bulk /books response did not include this asset_id'
    """,
)


CREATE_INDEX_SQL: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_core_market_tokens_updated_market ON core.market_tokens (updated_at, market_id) WHERE token_id IS NOT NULL AND token_id <> ''",
    "CREATE INDEX IF NOT EXISTS idx_core_market_status_updated_market ON core.market_status_snapshot (updated_at, market_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_slug_side ON quant.market_token_metadata (market_slug, token_side)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_market_side ON quant.market_token_metadata (market_id, token_side)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_token_hex ON quant.market_token_metadata (token_id_hex)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_dates ON quant.market_token_metadata (created_at, end_date)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_frontend_shard6_created ON quant.market_token_metadata ((mod(abs(hashtext(token_id)), 6)), created_at, market_id, token_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_frontend_shard6_active_updated ON quant.market_token_metadata ((mod(abs(hashtext(token_id)), 6)), active, closed, updated_at DESC, created_at DESC, market_id, token_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_live_dates ON quant.market_token_metadata (active, closed, end_date, created_at, market_id) WHERE token_id_hex IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_end_live ON quant.market_token_metadata (end_date, created_at, market_id) WHERE token_id_hex IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_duplicate_rank ON quant.market_token_metadata (duplicate_group_key, token_side, created_at, market_id) WHERE duplicate_group_key IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_quant_eligibility_eligible ON quant.market_price_eligibility (eligible, market_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_eligibility_block_watermark ON quant.market_price_eligibility (eligible, last_orderfilled_block, market_id) WHERE eligible = TRUE AND last_orderfilled_block IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_quant_eligibility_trade_count ON quant.market_price_eligibility (eligible, orderfilled_trade_count DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_frontend_slug_side_time ON quant.market_token_frontend_price_1m (market_slug, token_side, ts_minute)",
    "CREATE INDEX IF NOT EXISTS idx_quant_frontend_market_side_time ON quant.market_token_frontend_price_1m (market_id, token_side, ts_minute)",
    "CREATE INDEX IF NOT EXISTS idx_quant_block_close_slug_side_block ON quant.market_token_block_close (market_slug, token_side, block_number)",
    "CREATE INDEX IF NOT EXISTS idx_quant_block_close_market_side_block ON quant.market_token_block_close (market_id, token_side, block_number)",
    "CREATE INDEX IF NOT EXISTS idx_quant_block_close_token_time ON quant.market_token_block_close (token_id, block_timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_quant_api_trades_slug_side_time ON quant.market_token_api_trades (market_slug, token_side, trade_time)",
    "CREATE INDEX IF NOT EXISTS idx_quant_api_trades_market_side_time ON quant.market_token_api_trades (market_id, token_side, trade_time)",
    "CREATE INDEX IF NOT EXISTS idx_quant_api_trades_token_time ON quant.market_token_api_trades (token_id, trade_time)",
    "CREATE INDEX IF NOT EXISTS idx_quant_build_state_status ON quant.market_price_build_market_state (source, status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_quant_build_state_frontend_watermark ON quant.market_price_build_market_state (last_complete_ts, token_id) WHERE source = 'frontend' AND status NOT IN ('skipped', 'deferred')",
    "CREATE INDEX IF NOT EXISTS idx_quant_build_state_frontend_shard6_watermark ON quant.market_price_build_market_state ((mod(abs(hashtext(token_id)), 6)), last_complete_ts, token_id) WHERE source = 'frontend' AND status NOT IN ('skipped', 'deferred')",
    "CREATE INDEX IF NOT EXISTS idx_quant_build_state_frontend_retry_shard6 ON quant.market_price_build_market_state (status, (mod(abs(hashtext(token_id)), 6)), last_complete_ts, token_id) WHERE source = 'frontend' AND status IN ('skipped', 'deferred')",
    "CREATE INDEX IF NOT EXISTS idx_quant_build_state_block_watermark ON quant.market_price_build_market_state (source, last_complete_block, token_id) WHERE source = 'orderfilled_block_close'",
    "CREATE INDEX IF NOT EXISTS idx_quant_mtbc_rebuild_runs_status ON quant.market_token_block_close_rebuild_runs (status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_quant_mtbc_rebuild_targets_shard_market ON quant.market_token_block_close_rebuild_targets (rebuild_id, shard_index, market_id, token_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_quant_mtbc_rebuild_chunks_shard_range ON quant.market_token_block_close_rebuild_chunks (rebuild_id, shard_index, from_block, to_block)",
    "CREATE INDEX IF NOT EXISTS idx_quant_mtbc_rebuild_chunks_claim ON quant.market_token_block_close_rebuild_chunks (rebuild_id, shard_index, status, lease_expires_at, from_block)",
    "CREATE INDEX IF NOT EXISTS idx_quant_market_progress_status ON quant.market_price_build_market_progress (status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_quant_orderfilled_market_stats_blocks ON quant.market_orderfilled_market_stats (first_orderfilled_block, last_orderfilled_block)",
    "CREATE INDEX IF NOT EXISTS idx_quant_orderfilled_token_stats_token ON quant.market_orderfilled_token_stats (token_id_hex)",
    "CREATE INDEX IF NOT EXISTS idx_quant_execution_summary_market_side ON quant.market_token_execution_summary (market_id, token_side)",
    "CREATE INDEX IF NOT EXISTS idx_quant_execution_summary_volume ON quant.market_token_execution_summary (volume DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_execution_summary_trades ON quant.market_token_execution_summary (trade_count DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_execution_summary_buckets ON quant.market_token_execution_summary (liquidity_bucket, side_bucket)",
    "CREATE INDEX IF NOT EXISTS idx_quant_market_execution_summary_volume ON quant.market_execution_summary (volume DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_market_execution_summary_trades ON quant.market_execution_summary (trade_count DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_execution_summary_volume ON quant.event_execution_summary (volume DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_execution_summary_trades ON quant.event_execution_summary (trade_count DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_build_targets_active ON quant.market_price_build_targets (source, status, priority DESC, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_build_targets_slug_side ON quant.market_price_build_targets (market_slug, token_side, source)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_token_time ON quant.clob_orderbook_snapshots (token_id, fetched_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_token_snapshot_time ON quant.clob_orderbook_snapshots (token_id, snapshot_timestamp DESC, snapshot_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_token_block ON quant.clob_orderbook_snapshots (token_id, block_number DESC, snapshot_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_market_side_time ON quant.clob_orderbook_snapshots (market_id, side, snapshot_timestamp DESC, snapshot_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_condition_time ON quant.clob_orderbook_snapshots (condition_id, snapshot_timestamp DESC, snapshot_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_side_time ON quant.clob_orderbook_snapshots (side, fetched_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_status_time ON quant.clob_orderbook_snapshots (book_status, fetched_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_snapshots_storage_time ON quant.clob_orderbook_snapshots (storage_tier, snapshot_timestamp DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_raw_asset_time ON quant.clob_l2_raw_events (asset_id, event_ts DESC, raw_event_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_raw_ingest_hour ON quant.clob_l2_raw_events (ingest_hour, raw_event_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_raw_type_time ON quant.clob_l2_raw_events (event_type, event_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_raw_market_time ON quant.clob_l2_raw_events (market, event_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_archive_hour ON quant.clob_l2_archive_manifest (archive_hour DESC, shard_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_archive_status ON quant.clob_l2_archive_manifest (status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_subscription_state ON quant.clob_l2_subscription_state (state, desired, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_subscription_desired_asset ON quant.clob_l2_subscription_state (asset_id) WHERE desired = TRUE",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_subscription_shard ON quant.clob_l2_subscription_state (shard_id, connection_generation, state)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_current_grade ON quant.clob_l2_current_coverage (coverage_grade, has_gap, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_rest_audit_asset ON quant.clob_l2_rest_audits (asset_id, audit_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_gap_open ON quant.clob_l2_gap_incidents (status, detected_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_gap_repaired_at ON quant.clob_l2_gap_incidents (repair_finished_at DESC) WHERE status = 'repaired'",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_canary_audits_asset_ts ON quant.clob_l2_canary_audits (asset_id, audit_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_canary_audits_classification_ts ON quant.clob_l2_canary_audits (classification, audit_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_redundant_state_shard ON quant.clob_l2_redundant_feed_state (shard_id, last_compared_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_redundant_mismatch_ts ON quant.clob_l2_redundant_mismatch_audits (audit_ts DESC, classification)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_redundant_summary_ts ON quant.clob_l2_redundant_audit_summary (audit_ts DESC, shard_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_tokens_state ON quant.paper_market_registry_tokens (market_state, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_tokens_subscription ON quant.paper_market_registry_tokens (subscription_eligible, desired_subscribed, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_tokens_execution ON quant.paper_market_registry_tokens (execution_eligible, book_quality, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_tokens_condition ON quant.paper_market_registry_tokens (condition_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_markets_state ON quant.paper_market_registry_markets (market_state, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_markets_condition ON quant.paper_market_registry_markets (condition_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_markets_gamma ON quant.paper_market_registry_markets (gamma_market_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_markets_hash ON quant.paper_market_registry_markets (metadata_hash)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_lifecycle_asset_time ON quant.paper_market_lifecycle_events (asset_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_lifecycle_condition_time ON quant.paper_market_lifecycle_events (condition_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_universe_generation ON quant.paper_token_universe_snapshots (generation DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_lob_targets_status ON quant.paper_lob_subscription_targets (target_status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_lob_targets_desired_updated ON quant.paper_lob_subscription_targets (updated_at DESC, asset_id) WHERE desired_subscribed = TRUE",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_lob_targets_undesired_asset ON quant.paper_lob_subscription_targets (asset_id) WHERE desired_subscribed = FALSE",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_lob_targets_execution_l2 ON quant.paper_lob_subscription_targets (last_l2_event_at DESC, last_book_update_at DESC, asset_id) WHERE desired_subscribed = TRUE AND actual_subscribed = TRUE AND last_book_quality IN ('READY_HIGH', 'READY_MEDIUM') AND best_bid > 0 AND best_ask > 0",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_sync_runs_type_started ON quant.paper_registry_sync_runs (sync_type, started_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_sync_runs_status ON quant.paper_registry_sync_runs (status, started_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_health_time ON quant.paper_registry_health_snapshots (recorded_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_registry_health_label ON quant.paper_registry_health_snapshots (label, recorded_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_outbox_status ON quant.paper_registry_outbox (status, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_outbox_asset ON quant.paper_registry_outbox (asset_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_paper_outbox_pending_id ON quant.paper_registry_outbox (outbox_id) WHERE status = 'pending'",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_quant_paper_outbox_pending_event_asset ON quant.paper_registry_outbox (event_type, asset_id) WHERE status = 'pending'",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_runs_status ON quant.quant_backtest_runs (status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_runs_market ON quant.quant_backtest_runs (market_slug, token_side, price_source, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_runs_engine ON quant.quant_backtest_runs (backtest_engine, status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_run_progress_status ON quant.quant_backtest_run_progress (status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_run_progress_worker ON quant.quant_backtest_run_progress (worker_id, status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_trades_run_pnl ON quant.quant_backtest_trades (run_id, pnl)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_orders_run_status ON quant.quant_backtest_orders (run_id, status, submit_x)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_orders_trade ON quant.quant_backtest_orders (run_id, trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_order_state_run_order ON quant.real_order_state_events (run_id, order_id, event_time DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_order_state_external ON quant.real_order_state_events (external_order_id, event_time DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_order_state_market_time ON quant.real_order_state_events (market_slug, token_side, event_time DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_order_state_collection_source ON quant.real_order_state_collection_state (source, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_order_state_collection_success ON quant.real_order_state_collection_state (last_success_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_external_source_import_type ON quant.external_source_import_state (source_type, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_external_source_import_success ON quant.external_source_import_state (last_success_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_external_signal_events_run_time ON quant.external_signal_events (run_id, observed_at DESC, observed_block DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_external_signal_events_market_time ON quant.external_signal_events (market_slug, token_side, observed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_calibration_run_verdict ON quant.quant_backtest_calibration_orders (run_id, verdict, observed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_calibration_market_time ON quant.quant_backtest_calibration_orders (market_slug, token_side, observed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_calibration_role_time ON quant.quant_backtest_calibration_orders (role, observed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_cost_events_run_type ON quant.real_backtest_cost_events (run_id, event_type, observed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_cost_events_order ON quant.real_backtest_cost_events (run_id, order_id, event_type)",
    "CREATE INDEX IF NOT EXISTS idx_quant_real_cost_events_trade ON quant.real_backtest_cost_events (run_id, trade_id, event_type)",
    "CREATE INDEX IF NOT EXISTS idx_quant_cost_calibration_run_verdict ON quant.quant_backtest_cost_calibration (run_id, verdict, event_type)",
    "CREATE INDEX IF NOT EXISTS idx_quant_cost_calibration_market_type ON quant.quant_backtest_cost_calibration (market_slug, token_side, event_type)",
    "CREATE INDEX IF NOT EXISTS idx_quant_platform_incidents_block ON quant.platform_incidents (start_block, end_block, severity)",
    "CREATE INDEX IF NOT EXISTS idx_quant_platform_incidents_time ON quant.platform_incidents (start_ts, end_ts, severity)",
    "CREATE INDEX IF NOT EXISTS idx_quant_platform_incidents_market ON quant.platform_incidents (market_slug, token_side, component)",
    "CREATE INDEX IF NOT EXISTS idx_quant_execution_profile_overrides_status ON quant.execution_profile_overrides (status, scope, updated_at DESC, override_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_execution_profile_overrides_bucket ON quant.execution_profile_overrides (status, bucket_field, bucket_value, updated_at DESC, override_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_parameter_search_batches_status ON quant.parameter_search_batches (status, updated_at DESC, batch_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_parameter_search_batches_universe ON quant.parameter_search_batches (universe_name, status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_parameter_search_items_status ON quant.parameter_search_batch_items (status, batch_id, item_index)",
    "CREATE INDEX IF NOT EXISTS idx_quant_parameter_search_items_retry ON quant.parameter_search_batch_items (batch_id, status, attempt_count, item_index)",
    "CREATE INDEX IF NOT EXISTS idx_quant_parameter_search_items_fingerprint ON quant.parameter_search_batch_items (parameter_fingerprint, evidence_mode, batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_production_parameter_staging_status ON quant.production_parameter_staging (status, updated_at DESC, staging_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_production_parameter_staging_strategy ON quant.production_parameter_staging (strategy_name, strategy_version, status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_production_parameter_staging_benchmark ON quant.production_parameter_staging (benchmark_id, status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_strategy_activation_run ON quant.strategy_activation_decisions (run_id, created_at DESC, decision_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_strategy_activation_mode ON quant.strategy_activation_decisions (target_mode, activation_allowed, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_strategy_activation_strategy ON quant.strategy_activation_decisions (strategy_name, strategy_version, target_mode, created_at DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_quant_strategy_enable_state_unique ON quant.strategy_enable_state (strategy_name, strategy_version, target_mode, market_slug, token_side)",
    "CREATE INDEX IF NOT EXISTS idx_quant_strategy_enable_state_enabled ON quant.strategy_enable_state (target_mode, enabled, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_strategy_enable_state_run ON quant.strategy_enable_state (run_id, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_ledger_run_event ON quant.quant_backtest_ledger (run_id, event_type, x_value)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_ledger_trade ON quant.quant_backtest_ledger (run_id, trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_equity_run_x ON quant.quant_backtest_equity (run_id, point_index)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_benchmark_runs_created ON quant.quant_backtest_benchmark_runs (created_at DESC, benchmark_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_benchmark_runs_universe ON quant.quant_backtest_benchmark_runs (universe_name, status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_benchmark_rows_quality ON quant.quant_backtest_benchmark_rows (benchmark_id, data_quality, row_index)",
    "CREATE INDEX IF NOT EXISTS idx_quant_backtest_benchmark_rows_market ON quant.quant_backtest_benchmark_rows (market_slug, benchmark_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_metadata_search ON quant.market_event_metadata (event_slug, event_title)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_metadata_status ON quant.market_event_metadata (status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_members_market ON quant.market_event_members (market_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_members_coverage ON quant.market_event_members (event_slug, coverage_status, outcome_order)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_members_active ON quant.market_event_members (active, closed, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_price_series_tiles_lookup ON quant.quant_price_series_tiles (scope, entity_slug, price_source, range_name, resolution, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_price_series_tiles_entity ON quant.quant_price_series_tiles (entity_type, entity_slug, price_source, tile_kind, range_name, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_price_series_tiles_window ON quant.quant_price_series_tiles (entity_type, entity_slug, price_source, tile_kind, window_from_x, window_to_x, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_quant_price_series_tiles_expiry ON quant.quant_price_series_tiles (expires_at)",
)


OPTIONAL_SEARCH_INDEX_SQL: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_slug_trgm ON quant.market_token_metadata USING gin (lower(market_slug) gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_quant_metadata_title_trgm ON quant.market_token_metadata USING gin (lower(market_title) gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_quant_market_progress_slug_trgm ON quant.market_price_build_market_progress USING gin (lower(market_slug) gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_metadata_slug_trgm ON quant.market_event_metadata USING gin (lower(event_slug) gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_metadata_title_trgm ON quant.market_event_metadata USING gin (lower(event_title) gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_members_slug_trgm ON quant.market_event_members USING gin (lower(market_slug) gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_members_question_trgm ON quant.market_event_members USING gin (lower(question) gin_trgm_ops)",
    "CREATE INDEX IF NOT EXISTS idx_quant_event_members_outcome_trgm ON quant.market_event_members USING gin (lower(outcome_label) gin_trgm_ops)",
)


ADD_COLUMN_RE = re.compile(r"^ALTER TABLE (?P<table>[a-z_]+\.[a-z_]+) ADD COLUMN IF NOT EXISTS (?P<column>[a-z_]+)\b", re.IGNORECASE)
INDEX_RE = re.compile(
    r"^CREATE (?:UNIQUE )?INDEX IF NOT EXISTS "
    r"(?P<index>[a-z_][a-z0-9_]*) ON "
    r"(?P<table>[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)\b",
    re.IGNORECASE,
)


def _column_exists(conn: Any, table: str, column: str) -> bool:
    schema_name, table_name = table.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (schema_name, table_name, column),
        )
        return cur.fetchone() is not None


def _relation_exists(conn: Any, relation_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (relation_name,))
        row = cur.fetchone()
        return bool(row and row["exists"])


def _should_skip_statement(conn: Any, statement: str) -> bool:
    text = " ".join(statement.strip().split())
    add_match = ADD_COLUMN_RE.match(text)
    if add_match and _column_exists(conn, add_match.group("table"), add_match.group("column")):
        return True
    index_match = INDEX_RE.match(text)
    if index_match:
        table_name = index_match.group("table")
        if not _relation_exists(conn, table_name):
            return True
        index_schema = table_name.split(".", 1)[0]
        if _relation_exists(conn, f"{index_schema}.{index_match.group('index')}"):
            return True
    if text.upper().startswith("ALTER TABLE QUANT.MARKET_TOKEN_BLOCK_CLOSE ALTER COLUMN SOURCE SET DEFAULT"):
        return True
    return False


def _execute_optional_ddl(conn: Any, statement: str, *, lock_timeout_ms: int = 1500) -> bool:
    """Run optional search DDL without blocking core API/backtest schema setup."""

    with conn.cursor() as cur:
        cur.execute("SAVEPOINT optional_search_ddl")
        try:
            cur.execute(f"SET LOCAL lock_timeout = '{int(lock_timeout_ms)}ms'")
            cur.execute(statement)
            cur.execute("RELEASE SAVEPOINT optional_search_ddl")
            return True
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT optional_search_ddl")
            cur.execute("RELEASE SAVEPOINT optional_search_ddl")
            sqlstate = str(getattr(exc, "sqlstate", "") or "")
            if sqlstate in {"55P03", "42501", "42704"}:
                return False
            raise


def create_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(914020250607)")
        cur.execute(CREATE_SCHEMA_SQL)
        for statement in CREATE_TABLE_SQL:
            cur.execute(statement)
    for statement in ALTER_TABLE_SQL:
        if _should_skip_statement(conn, statement):
            continue
        with conn.cursor() as cur:
            cur.execute(statement)
    for statement in DATA_MIGRATION_SQL:
        with conn.cursor() as cur:
            cur.execute(statement)
    for statement in CREATE_INDEX_SQL:
        if _should_skip_statement(conn, statement):
            continue
        with conn.cursor() as cur:
            cur.execute(statement)
    for statement in OPTIONAL_EXTENSION_SQL:
        _execute_optional_ddl(conn, statement)
    for statement in OPTIONAL_SEARCH_INDEX_SQL:
        if _should_skip_statement(conn, statement):
            continue
        _execute_optional_ddl(conn, statement)
