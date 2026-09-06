"""Readiness checks for the fill-first backtest stack."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Iterable
from .repository_scope import OUT_OF_SCOPE, external_owner


READY = "ready"
REVIEW = "review"
MISSING = "missing"


@dataclass(frozen=True)
class FileRequirement:
    name: str
    path: str
    tokens: tuple[str, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class ReadinessCheck:
    name: str
    status: str
    detail: str
    evidence: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "evidence": self.evidence,
        }


FILE_REQUIREMENTS: tuple[FileRequirement, ...] = (
    FileRequirement(
        "raw orderfilled replay",
        "quant/backtest/backtest_engine.py",
        ("raw_event_count", "candidate_event_count", "consumed_event_count", "loaded_block_window", "raw_evidence_summary"),
        "raw成交证据、候选成交、已消费成交、加载窗口和 canonical 去重口径必须进入 Fill Quality",
    ),
    FileRequirement(
        "no fill and markout quality",
        "quant/backtest/backtest_engine.py",
        ("no_fill_reasons", "missed_opportunity_count", "avg_markout_after_1_bars", "adverse_selection_buckets"),
        "No Fill、错失机会和成交后 markout 是 fill-first 的核心诊断",
    ),
    FileRequirement(
        "focused fill report export",
        "quant/backtest/fill_report.py",
        ("build_backtest_fill_report", "missed_orders", "no_fill", "raw_event_count", "calibration_samples"),
        "每个 run 要能独立导出成交质量、no_fill_reason、missed orders、raw evidence 和校准缺口，方便脱离前端排查",
    ),
    FileRequirement(
        "focused fill report script",
        "scripts/export_backtest_fill_report.py",
        ("--run-id", "--max-orders", "load_backtest_run_artifact_inputs", "backtest_fill_report_to_markdown"),
        "成交质量报告要有 CLI 入口，可直接导出 JSON/Markdown",
    ),
    FileRequirement(
        "real order state evidence",
        "quant/backtest/order_state.py",
        ("real_order_state_events", "attach_real_order_state_events", "submit_status", "cancel_status"),
        "真实 submit/accepted/cancel/API/chain 状态要能附加到模拟订单",
    ),
    FileRequirement(
        "real order state collector",
        "scripts/collect_real_order_state_events.py",
        ("--state-key", "--since-param", "--cursor-param", "upsert_real_order_state_events"),
        "真实订单状态采集要支持增量水位",
    ),
    FileRequirement(
        "real order state file importer",
        "scripts/import_real_order_state_events.py",
        ("--state-key", "upsert_external_source_import_state", "last_rows_written"),
        "本地/manual 订单状态文件导入也必须写统一 external source freshness 水位",
    ),
    FileRequirement(
        "real order state health",
        "scripts/check_real_order_state_collection_health.py",
        ("--max-stale-seconds", "evaluate_collection_state_health"),
        "采集链路要能检查 freshness 和错误状态",
    ),
    FileRequirement(
        "live-vs-sim fill calibration",
        "quant/backtest/calibration.py",
        ("build_calibration_report", "status_error", "price_error", "latency_error_seconds", "simulated_pnl", "live_pnl", "pnl_error", "pnl_mismatch"),
        "真实成交和模拟成交要能做状态、价格、滑点、延迟和 PnL 误差对比",
    ),
    FileRequirement(
        "auto calibration sample builder",
        "scripts/build_backtest_calibration_samples.py",
        ("quant.real_order_state_events", "quant.quant_backtest_orders", "upsert_calibration_orders"),
        "真实订单状态和模拟订单要能自动构造 calibration samples",
    ),
    FileRequirement(
        "calibration sample context",
        "quant/backtest/calibration_samples.py",
        ("market_category", "liquidity_bucket", "volatility_bucket", "time_to_expiry_bucket", "\"context\""),
        "calibration samples 必须保留 market/liquidity/volatility/time-to-expiry 分桶上下文，避免落库后分桶信息丢失",
    ),
    FileRequirement(
        "periodic calibration profile suggestions",
        "quant/backtest/calibration_report.py",
        ("build_periodic_calibration_report", "execution_profile_suggestions", "execution_profile_suggestions_from_report", "calibration_recommendations", "Execution Profile Suggestions"),
        "周期校准报告必须直接输出可 review/落库的 execution profile suggestions，不能只给自然语言 review",
    ),
    FileRequirement(
        "execution profile suggestion apply bridge",
        "scripts/apply_execution_profile_calibration.py",
        ("load_suggestions", "execution_profile_suggestions", "items", "suggestions", "upsert_execution_profile_overrides", "--approve", "--dry-run"),
        "校准报告里的结构化 suggestions 必须能直接进入 pending/approved override 写入脚本，避免报告和落库工具格式断开",
    ),
    FileRequirement(
        "shadow/live collection plan",
        "scripts/export_shadow_live_order_plan.py",
        ("build_shadow_live_order_plan", "event-jsonl", "live-shadow"),
        "回测订单要能导出成 shadow/live 采集模板，用于后续真实成交对齐",
    ),
    FileRequirement(
        "shadow/live evidence validation",
        "scripts/validate_shadow_live_order_events.py",
        ("validate_shadow_live_order_events", "require-cost-fields", "strict-review"),
        "真实订单状态导入前必须能校验 live 字段完整性，避免空模板污染校准样本",
    ),
    FileRequirement(
        "shadow/live calibration fixture",
        "quant/backtest/calibration_fixture.py",
        ("run_shadow_live_calibration_fixture", "build_shadow_live_order_plan", "calibration_trust_status"),
        "每阶段测试要覆盖 shadow/live 的导出、校验、样本构造和 calibration report 小闭环",
    ),
    FileRequirement(
        "shadow/live calibration pipeline",
        "scripts/run_shadow_live_calibration_pipeline.py",
        ("run_shadow_live_calibration_pipeline", "no-require-cost-fields", "calibration-source"),
        "真实 shadow/live 事件要能一键完成校验、导入、样本构造和 calibration report",
    ),
    FileRequirement(
        "execution profile override",
        "quant/backtest/execution_profile_overrides.py",
        ("status", "approved", "pending", "upsert_execution_profile_overrides", "select_approved_execution_profile_override", "bucket_field", "bucket_value"),
        "校准参数必须通过 pending/approved override，不直接改历史 run；approved bucket override 要能在 calibrated run 中优先于 overall 生效",
    ),
    FileRequirement(
        "real cost calibration",
        "quant/backtest/cost_calibration.py",
        ("real_backtest_cost_events", "quant_backtest_cost_calibration", "build_cost_calibration_report"),
        "真实 fee/rebate/gas/settlement/redeem/capital cost 要能和模拟 ledger 对齐",
    ),
    FileRequirement(
        "shadow/live triangulation report",
        "quant/backtest/shadow_live_triangulation.py",
        ("build_shadow_live_triangulation_report", "triangulation_verdict", "fill_model_suspect", "drift_summary", "status_error_rate", "avg_price_error", "avg_latency_error_seconds", "avg_pnl_error", "total_cost_amount_error"),
        "backtest、shadow/live 和真实成本校准必须合成一个明确 verdict，drift 大时直接标记 fill model suspect",
    ),
    FileRequirement(
        "fill-first promotion gate",
        "quant/backtest/promotion_gate.py",
        ("build_fill_first_promotion_gate_report", "production_promotion_allowed", "paper_promotion_allowed", "fill_model_suspect", "do_not_promote_when_fill_model_suspect", "require_shadow_live_triangulation", "external_source_run_coverage_report", "external_source_missing_evidence_plan", "require_run_level_external_evidence_coverage", "require_no_missing_external_evidence"),
        "策略进入 paper/live 前必须经过 fill-first gate；fill_model_suspect、缺 shadow/live triangulation、run-level external evidence 不完整或仍有 missing evidence 时不能推进",
    ),
    FileRequirement(
        "strategy activation gate",
        "quant/backtest/strategy_activation.py",
        ("build_strategy_activation_decision", "build_strategy_enable_state", "upsert_strategy_enable_state", "load_strategy_enable_state", "target_mode", "paper", "live", "activation_allowed", "promotion_gate_report", "paper_live_evidence_gate_report", "paper_live_paper_allowed", "paper_live_live_allowed", "insert_strategy_activation_decision"),
        "paper/live 策略启用入口必须显式检查 promotion gate 和 paper/live evidence gate，并把允许/阻塞原因落成可审计决策；真实启用状态只能来自 allowed activation decision",
    ),
    FileRequirement(
        "strategy runner guard",
        "quant/backtest/strategy_runner_guard.py",
        ("build_strategy_runner_plan", "quant.strategy_enable_state", "enabled=true", "requires_paper_live_evidence_gate", "paper_live_evidence_gate_report", "quant.real_order_state_events", "quant.quant_backtest_calibration_orders", "lob_required", "False"),
        "paper/live runner 必须只从 enabled strategy state 生成执行计划，并再次检查 paper/live evidence gate，把真实订单状态回写到 fill-first 校准链路",
    ),
    FileRequirement(
        "strategy runner plan script",
        "scripts/build_strategy_runner_plan.py",
        ("load_strategy_enable_state", "build_strategy_runner_plan", "--target-mode", "--include-blocked", "--strict"),
        "真实 executor 接入前必须有只读 runner plan 脚本，用于验证当前启用策略是否可运行",
    ),
    FileRequirement(
        "guarded paper/live executor adapter",
        "quant/backtest/guarded_executor.py",
        ("build_guarded_executor_report", "record_guarded_execution_intents", "quant.real_order_state_events", "record_intent", "dry_run", "requires_paper_live_evidence_gate", "requires_external_order_adapter", "paper_live_evidence_gate_report", "lob_required", "False"),
        "paper/live executor 必须先生成可审计 intent，默认 dry-run，只有显式 record_intent 才写真实订单状态证据表",
    ),
    FileRequirement(
        "guarded strategy executor script",
        "scripts/run_guarded_strategy_executor.py",
        ("build_guarded_executor_report", "record_guarded_execution_intents", "--record-intent", "--target-mode", "--strict"),
        "真实 order adapter 接入前必须能通过 CLI 检查或记录 guarded paper/live intent",
    ),
    FileRequirement(
        "external order execution adapter",
        "quant/backtest/order_execution_adapter.py",
        ("build_order_execution_adapter_report", "build_submit_request_template", "record_order_execution_adapter_events", "_intent_gate_issues", "requires_paper_live_evidence_gate", "paper_live_evidence_gate_report", "quant.real_order_state_events", "dry_run", "submit_url", "lob_required", "False"),
        "guarded intent 必须能进入外部 paper/live 订单 adapter contract，但 adapter 必须再次检查 paper/live evidence gate，并把 submit/cancel/fill 回执标准化回真实订单状态证据表",
    ),
    FileRequirement(
        "external order execution adapter script",
        "scripts/run_order_execution_adapter.py",
        ("build_order_execution_adapter_report", "record_order_execution_adapter_events", "build_order_execution_run_safety_report", "--execute", "--record-events", "--live-confirm", "ORDER_EXECUTION_SUBMIT_URL"),
        "外部订单 adapter 要有默认 dry-run 的 CLI，只有显式 --execute 才调用外部 submit URL",
    ),
    FileRequirement(
        "external order execution safety",
        "quant/backtest/order_execution_safety.py",
        ("build_order_execution_env_audit", "build_order_execution_run_safety_report", "LIVE_CONFIRM_TOKEN", "ORDER_EXECUTION_SUBMIT_URL", "ORDER_EXECUTION_LIVE_CONFIRM", "--execute requires --record-events"),
        "真实 paper/live 执行必须有配置审计和运行时安全门禁，live 执行需要显式确认且执行回执必须写入证据表",
    ),
    FileRequirement(
        "external order execution env audit script",
        "scripts/audit_order_execution_env.py",
        ("build_order_execution_env_audit", "--env-file", "--strict-review", "ORDER_EXECUTION"),
        "ORDER_EXECUTION_* 配置要能独立审计，避免 endpoint/auth/live confirm 缺失时误启真实执行",
    ),
    FileRequirement(
        "external order execution env bootstrap",
        "quant/backtest/order_execution_env_bootstrap.py",
        ("bootstrap_order_execution_env", "contains_live_execution", "contains_secret", "ORDER_EXECUTION_TARGET_MODE", "ORDER_EXECUTION_SUBMIT_URL", "run_order_execution_adapter.py", "audit_order_execution_env.py"),
        "真实 paper/live 执行未配置前，要能生成本机 paper-only dry-run ORDER_EXECUTION env，并明确它不会启用 live 下单或携带密钥",
    ),
    FileRequirement(
        "external order execution calibration bridge",
        "quant/backtest/order_execution_calibration.py",
        ("build_order_execution_calibration_report", "record_order_execution_calibration_samples", "quant.quant_backtest_calibration_orders", "adapter response events", "include_open_events"),
        "外部订单 adapter 回执必须能自动转成 fill calibration samples，而不是只停留在 real_order_state_events",
    ),
    FileRequirement(
        "external order execution calibration CLI bridge",
        "scripts/run_order_execution_adapter.py",
        ("--build-calibration", "--calibration-source", "--include-open-events", "record_order_execution_calibration_samples"),
        "order adapter 写入回执后要能可选立即构造 calibration samples 并写入校准表",
    ),
    FileRequirement(
        "external order execution fixture",
        "quant/backtest/order_execution_fixture.py",
        ("run_order_execution_fixture_pipeline", "build_strategy_runner_plan", "build_guarded_executor_report", "build_order_execution_adapter_report", "build_order_execution_calibration_report"),
        "项目级测试必须覆盖 enabled strategy -> guarded intent -> adapter response -> calibration 的完整本地闭环",
    ),
    FileRequirement(
        "external order execution fixture script",
        "scripts/run_order_execution_fixture_pipeline.py",
        ("run_order_execution_fixture_pipeline", "--strict-review", "--format"),
        "执行适配器和校准桥要能被独立脚本自检，供 quality gate 和人工排查使用",
    ),
    FileRequirement(
        "fill-first production readiness",
        "quant/backtest/fill_first_production_readiness.py",
        ("build_fill_first_production_readiness_report", "launch_allowed", "blocked_reasons", "review_reasons", "external_source_import_health", "external_source_run_coverage", "paper_live_evidence_gate", "paper_live_evidence_gate_report", "load_external_source_run_coverage_inputs", "load_backtest_run_artifact_inputs", "ORDER_EXECUTION", "real_cost_events", "platform_incidents"),
        "paper/live 启动前必须有统一 preflight，明确真实执行、外部证据源配置、DB 导入 freshness、指定 run 的外部证据覆盖和 paper/live evidence gate 是否已满足 fill-first 要求",
    ),
    FileRequirement(
        "fill-first production readiness script",
        "scripts/check_fill_first_production_readiness.py",
        ("build_fill_first_production_readiness_report", "--target-mode", "--env-file", "--check-db", "--run-id", "--run-artifact-json", "load_run_artifact_report", "--max-stale-seconds", "--strict-review"),
        "生产启动前检查要能一条命令输出 paper/live 是否 launch_allowed、导入状态是否新鲜、指定 run 覆盖率、离线 artifact gate 以及阻断原因",
    ),
    FileRequirement(
        "fill-first production launch checklist",
        "quant/backtest/production_launch_checklist.py",
        ("build_fill_first_production_launch_checklist", "command_plan", "external_source_onboarding", "production_readiness", "run external evidence", "order_execution_env_files", "audit_order_execution_env.py", "LOB/DEPTH intentionally excluded"),
        "生产启用前必须有总控 checklist，把 external source onboarding、订单执行 env 审计、导入 dry-run/write、health、run evidence 和 production readiness 串成一份可执行顺序",
    ),
    FileRequirement(
        "fill-first production launch checklist script",
        "scripts/plan_fill_first_production_launch.py",
        ("--target-mode", "--env-file", "--external-env-file", "--order-execution-env-file", "--check-db", "--run-id", "--run-artifact-json", "production_launch_checklist_to_markdown"),
        "生产启用总控 checklist 要有 CLI，默认只读，不写库不下单，可在 strict-review 下阻断未 ready 状态",
    ),
    FileRequirement(
        "fill-first production preflight fixture",
        "quant/backtest/production_preflight_fixture.py",
        ("run_fill_first_production_preflight_fixture", "build_fill_first_external_source_fixture", "build_fill_first_production_readiness_report", "paper_live_evidence_gate_report", "paper_live_evidence_gate", "build_order_execution_run_safety_report", "dry_run_safe"),
        "没有真实私有源时，也要能用本地 fixture 验证外部源 env 审计、paper production preflight、paper/live evidence gate 和订单执行 dry-run safety 链路",
    ),
    FileRequirement(
        "fill-first production preflight fixture script",
        "scripts/run_fill_first_production_preflight_fixture.py",
        ("run_fill_first_production_preflight_fixture", "--output-dir", "--overwrite", "--format"),
        "生产预检 fixture 要有 CLI，可在每阶段验证配置检查链路完整且不会调用外部 API",
    ),
    FileRequirement(
        "external source run coverage",
        "quant/backtest/external_source_run_coverage.py",
        ("build_external_source_run_coverage_report", "order_state_coverage_pct", "calibration_coverage_pct", "missing_order_state_order_ids"),
        "真实外部源不能只看全局 freshness；必须能按 run 检查模拟订单是否有 order-state evidence 和 calibration samples 覆盖",
    ),
    FileRequirement(
        "external source run coverage script",
        "scripts/check_external_source_run_coverage.py",
        ("load_latest_fill_first_backtest_run_id", "load_external_source_run_coverage_inputs", "--run-id", "--strict-review"),
        "每个回测 run 要有 CLI 可直接检查外部证据覆盖和 calibration 覆盖，供阶段验收与人工排查使用",
    ),
    FileRequirement(
        "external source missing evidence plan",
        "quant/backtest/external_source_missing_evidence.py",
        ("build_external_source_missing_evidence_plan", "orders_requiring_order_state", "orders_requiring_calibration", "event_templates", "write_external_source_missing_evidence_task_pack", "event_templates_jsonl"),
        "run 级 coverage 为 review 时，必须能导出具体缺哪些订单状态证据和 calibration 样本，并落成 JSON/Markdown/JSONL 任务包，不能只输出覆盖率为 0",
    ),
    FileRequirement(
        "external source missing evidence script",
        "scripts/export_external_source_missing_evidence_plan.py",
        ("--format", "event-jsonl", "load_backtest_run_artifact_inputs", "build_external_source_missing_evidence_plan"),
        "缺失外部证据要能通过 CLI 导出 Markdown/JSON/JSONL 工作单，供真实订单状态 exporter 或人工补充后再校验导入",
    ),
    FileRequirement(
        "artifact audit missing evidence task pack",
        "scripts/audit_backtest_run_artifacts.py",
        ("--export-missing-evidence-dir", "write_external_source_missing_evidence_task_pack", "missing_evidence_task_pack"),
        "run artifact audit 要能一键导出缺失外部证据任务包，让审计结果直接变成补证据输入",
    ),
    FileRequirement(
        "missing evidence task pack pipeline",
        "quant/backtest/missing_evidence_pipeline.py",
        ("run_missing_external_evidence_task_pack_pipeline", "validate_shadow_live_order_events", "upsert_real_order_state_events", "build_calibration_samples_from_order_events", "build_external_source_run_coverage_report"),
        "缺失证据任务包补齐后必须能一键完成 validate、import、build calibration 和 coverage re-check",
    ),
    FileRequirement(
        "missing evidence task pack pipeline script",
        "scripts/run_missing_external_evidence_task_pack.py",
        ("--task-pack-dir", "--write", "--events", "run_missing_external_evidence_task_pack_pipeline"),
        "任务包回灌要有默认 dry-run 的 CLI；只有显式 --write 才落库并刷新 calibration/coverage",
    ),
    FileRequirement(
        "external source import state",
        "quant/backtest/external_source_state.py",
        ("external_source_import_state", "evaluate_external_source_import_health", "last_rows_written"),
        "真实订单状态、成本、平台异常和 external signal 外部源要有可监控的长期导入状态",
    ),
    FileRequirement(
        "external source health script",
        "scripts/check_external_source_import_health.py",
        ("--max-stale-seconds", "evaluate_external_source_import_health", "--source-type"),
        "外部证据导入链路要能检查 freshness 和错误状态",
    ),
    FileRequirement(
        "external source local discovery",
        "scripts/import_discovered_external_sources.py",
        ("discover_external_source_files", "real_order_state_events", "external_signal_events", "--write", "--strict"),
        "本地订单状态、成本、incident 和 external signal 导出文件要能自动发现、预览并可选择写库",
    ),
    FileRequirement(
        "configured external source runner",
        "scripts/run_configured_external_source_imports.py",
        ("ORDER_STATE_API_URL", "COST_EVENTS_INPUT", "PLATFORM_INCIDENTS_INPUT", "EXTERNAL_SIGNAL_INPUT", "--env-file", "--write"),
        "生产环境变量或 env-file 配置好的订单状态、成本、incident 和 external signal 源要能统一 dry-run 或写库",
    ),
    FileRequirement(
        "external source env audit",
        "scripts/audit_external_source_env.py",
        ("build_external_source_env_audit", "--env-file", "--strict-review"),
        "真实外部源 env-file 要能在导入前检查缺字段、placeholder、缺 auth、缺状态水位和不存在的输入文件",
    ),
    FileRequirement(
        "external source env contract",
        "quant/backtest/external_source_env_audit.py",
        ("mode", "required_keys", "missing_keys", "health_tracked", "next_action"),
        "外部源审计不能只报 review；必须明确 file/url/unconfigured 模式、缺失 key、健康水位和下一步接入动作",
    ),
    FileRequirement(
        "external source onboarding plan",
        "quant/backtest/external_source_onboarding.py",
        ("build_external_source_onboarding_plan", "completion_criteria", "dry_run_command", "write_command", "production_blockers"),
        "外部源 review 要能变成生产接入清单，逐源输出完成标准、dry-run/write 命令和 paper/live 阻塞项",
    ),
    FileRequirement(
        "external source onboarding script",
        "scripts/plan_external_source_onboarding.py",
        ("build_external_source_onboarding_plan", "--env-file", "--strict-review", "--format"),
        "生产外部源接入计划要有 CLI，可直接输出 JSON/Markdown 并在 strict-review 下阻断未配置状态",
    ),
    FileRequirement(
        "external source fixture",
        "scripts/write_fill_first_external_source_fixture.py",
        (
            "build_fill_first_external_source_fixture",
            "build_fill_first_external_source_fixture_from_plan",
            "external_signals",
            "--from-run-id",
            "--use-latest-fill-first-run",
            "--output-dir",
            "--overwrite",
            "--env-file",
        ),
        "没有真实私有 API 时，也要能生成本地订单状态、成本、incident 和 external signal 样例包；需要时还能按真实 run 的订单 ID 对齐自检链路",
    ),
    FileRequirement(
        "external source fixture DB pipeline",
        "scripts/run_fill_first_external_source_fixture_pipeline.py",
        (
            "run_fill_first_external_source_fixture_pipeline",
            "build_external_source_run_coverage_report",
            "create_schema",
            "--write",
            "--check-db",
            "--rollback-smoke",
            "--use-latest-run-id",
            "--from-run-orders",
            "--build-calibration-smoke",
            "--build-calibration",
            "run_coverage",
            "order_state_coverage_pct",
            "calibration_coverage_pct",
        ),
        "外部源 fixture 要能显式执行写库校验，或在可回滚事务里验证真实订单状态、成本、incident 表约束；run-specific 模式还要验证证据能构造 calibration samples，并输出 run 级 order-state/calibration 覆盖率；显式写库时也能把样本落到审计层可读的 calibration 表",
    ),
    FileRequirement(
        "fill-first quality gate",
        "scripts/run_fill_first_quality_gate.py",
        ("build_fill_first_quality_gate_report", "--stage-check", "quality_gate_options", "production readiness CLI artifact gate fixture", "external source onboarding fixture", "--run-artifact-json", "current schema run artifact fixture", "--include-pytest", "--include-frontend-smoke", "--include-run-artifact-audit", "--include-external-fixture-db-smoke", "--external-env-file", "--order-execution-env-file", "external_source_env_files"),
        "每阶段开发后要有一键质量门禁，stage-check 覆盖结构、数据源、env-file 审计和配置、外部源 onboarding 清单、订单执行 env 审计、健康状态、production readiness CLI 离线 artifact gate、current schema artifact fixture、pytest、静态前端 smoke，并在 check-db 时增加 rollback DB smoke",
    ),
    FileRequirement(
        "external source env bootstrap",
        "quant/backtest/external_source_env_bootstrap.py",
        ("bootstrap_external_source_env", "contains_real_evidence", "audit_status", "onboarding_status", "run_configured_external_source_imports.py", "ORDER_STATE_INPUT", "EXTERNAL_SIGNAL_INPUT"),
        "真实外部源未配置前，要能生成本机 file-mode env-file 和四类 JSONL 占位文件，并明确它不是生产真实证据",
    ),
    FileRequirement(
        "current schema sample backtest script",
        "scripts/run_fill_first_sample_backtest.py",
        ("run_fill_first_sample_backtest", "--dry-run", "--seed-run-id", "--order-role", "--buy-limit-price", "--settlement-value", "artifact_schema_version"),
        "每阶段需要能创建或 dry-run 一个真实 DB current-schema ORDERFILLED_CROSS 样本 run，并立刻输出 artifact audit 摘要，避免只靠内置 fixture",
    ),
    FileRequirement(
        "parameter robustness report",
        "quant/backtest/parameter_robustness.py",
        ("build_parameter_robustness_report", "best_run", "median_run", "worst_decile", "parameter_sensitivity", "regime_split_performance", "best_parameter_promotion_allowed", "do_not_promote_best_only"),
        "参数扫描必须输出 best/median/worst-decile、参数敏感性和 regime split，并默认阻止只把最佳参数直接作为生产参数",
    ),
    FileRequirement(
        "parameter search plan",
        "quant/backtest/parameter_search_plan.py",
        ("build_parameter_search_plan", "DEFAULT_GRID", "walk_forward", "required_execution_profiles", "do_not_promote_best_only"),
        "参数扫描必须有默认 dry-run 计划器，固定生成 train/test/walk-forward 和 realistic/conservative 组合，避免手工挑 best-only 参数",
    ),
    FileRequirement(
        "parameter search plan script",
        "scripts/plan_fill_first_parameter_search.py",
        ("build_parameter_search_plan", "--grid-json", "--base-payload-json", "--strict-review"),
        "参数搜索计划要有 CLI，可从 base payload/grid JSON 生成可审计计划，默认不跑重任务不写库",
    ),
    FileRequirement(
        "parameter search results bridge",
        "quant/backtest/parameter_search_results.py",
        ("build_parameter_search_results_report", "missing_items", "coverage_pct", "robustness_report", "regime_coverage_report", "performance_score_validation_report", "score_bias_verdict", "production_parameter_staging"),
        "参数搜索结果必须能和计划逐项对齐，并把 coverage、缺失结果、稳健性 verdict、regime coverage verdict、performance score 小样本偏置检查和 production staging 决策串起来",
    ),
    FileRequirement(
        "parameter search results script",
        "scripts/check_fill_first_parameter_search_results.py",
        ("--plan-json", "--results-json", "--strict-review", "build_parameter_search_results_report"),
        "参数搜索跑完后要有 CLI 可检查结果覆盖率和 robustness/staging，不能只靠手工读取 rows",
    ),
    FileRequirement(
        "parameter search batch runner",
        "quant/backtest/parameter_search_runner.py",
        ("run_parameter_search_plan", "create_and_execute_backtest", "parameter_search_results", "staging_preview", "write_staging"),
        "参数搜索必须有批量执行桥：默认 dry-run，显式 execute 才创建 backtest run，并把结果归一化给 parameter_search_results/staging 使用",
    ),
    FileRequirement(
        "parameter search batch script",
        "scripts/run_fill_first_parameter_search_batch.py",
        ("--plan-json", "--execute", "--stage-parameters", "--write-staging", "--strict-review"),
        "参数搜索批量 runner 要有 CLI，默认不写库；只有显式 execute/write-staging 才跑真实回测和写生产参数 staging",
    ),
    FileRequirement(
        "parameter search scheduler",
        "quant/backtest/parameter_search_scheduler.py",
        ("create_parameter_search_batch", "claim_next_parameter_search_item", "run_parameter_search_batch_worker", "cancel_parameter_search_batch", "requeue_parameter_search_items", "build_parameter_search_progress_report", "retryable_count", "canceled_count"),
        "大 universe 参数搜索必须有可恢复的 DB-backed 调度层，支持 queued/running/succeeded/failed/canceled、失败重试、取消、重新入队和 progress 报告，不能只靠一次性 CLI",
    ),
    FileRequirement(
        "parameter search scheduler script",
        "scripts/run_fill_first_parameter_search_scheduler.py",
        ("--create-batch", "--run-worker", "--worker-loop", "--stream-json", "--progress", "--cancel-batch", "--requeue-items", "--max-attempts", "--max-items"),
        "参数搜索调度要有 CLI，可创建持久化 batch、执行一次 worker 或常驻 worker loop、查看 progress、取消 batch、重新入队失败项，并显式控制重试和每次处理数量",
    ),
    FileRequirement(
        "parameter search scheduler schema",
        "quant/core/schema.py",
        ("quant.parameter_search_batches", "quant.parameter_search_batch_items", "idx_quant_parameter_search_batches_status", "idx_quant_parameter_search_items_retry"),
        "参数搜索 batch/item 队列表必须由 schema 管理，支持按状态、universe 和 retryable item 查询",
    ),
    FileRequirement(
        "parameter search scheduler API",
        "scripts/api/routes/quant.py",
        ("parameter-search-batches", "api_quant_create_parameter_search_batch", "api_quant_run_parameter_search_batch_worker", "api_quant_cancel_parameter_search_batch", "api_quant_requeue_parameter_search_items", "build_parameter_search_progress_report"),
        "前端/API 必须能创建、查询、推进、取消和重新入队 parameter search batch，避免长期参数搜索只能靠本地终端一次性运行",
    ),
    FileRequirement(
        "static frontend strategy tester shell",
        "webpage/quant.html",
        ("Strategy Tester", "Configure backtest", "Execution Replay", "Inspector"),
        "当前前端是静态 HTML 工作台；Strategy Tester shell 必须在根入口可见，复杂参数搜索继续由后端 API/CLI 管理",
    ),
    FileRequirement(
        "static frontend price API wiring",
        "webpage/app.js",
        ("/wm-api/quant/price-window", "event_slug", "price_source", "max_outcomes", "point_format"),
        "静态前端必须直接通过 /wm-api/quant/price-window 读取真实价格窗口，不能再依赖旧 React API client",
    ),
    FileRequirement(
        "unified V2 V3 PML2 execution routing",
        "quant/backtest/backtest_engine.py",
        ("ORDERFILLED_V2_TAPE_MODE", "ORDERFILLED_V3_TRADE_MODE", "PREDICTION_L2_REPLAY_V1_MODE", "simulate_orderfilled_v3_trade_strategy", "fill_only_v3"),
        "统一主回测入口必须保留 Fill-only V2、Fill-only V3 和 PML2 三条可选择执行路径",
    ),
    FileRequirement(
        "static frontend V3 execution option",
        "webpage/app.js",
        ("ORDERFILLED_V3_TRADE", "central_trade_only_l2_reference_expected_fak", "observed_modeled_accounting"),
        "策略测试器必须可选择 Fill-only V3，并明确区分 observed 与 modeled 成交",
    ),
    FileRequirement(
        "parameter search worker systemd template",
        "deploy/systemd/quant-parameter-search-worker.service.example",
        ("parameter-search-worker.env", "--worker-loop", "--stream-json", "--max-items", "--poll-seconds", "--max-polls", "--no-retry-failed"),
        "参数搜索 scheduler 要有不含密钥的常驻 worker systemd 模板，能持续消费 DB 队列而不是依赖人工循环执行 CLI",
    ),
    FileRequirement(
        "parameter search worker env example",
        "deploy/systemd/parameter-search-worker.env.example",
        ("PARAMETER_SEARCH_WORKER_ID", "PARAMETER_SEARCH_MAX_ITEMS", "PARAMETER_SEARCH_POLL_SECONDS", "PARAMETER_SEARCH_MAX_POLLS", "PARAMETER_SEARCH_RETRY_FAILED"),
        "常驻参数搜索 worker 要有独立 env 示例，暴露 batch、poll、每轮处理数量和重试配置，但不能包含真实密钥",
    ),
    FileRequirement(
        "execution profile matrix report",
        "quant/backtest/execution_profile_matrix.py",
        ("build_execution_profile_matrix_report", "required_profiles", "realistic", "conservative", "pnl_degradation_pct_vs_baseline", "do_not_promote_profile_until_conservative_tested"),
        "默认报告必须证明 realistic/conservative 执行假设都跑过，不能只展示 optimistic 或单 profile 结果",
    ),
    FileRequirement(
        "benchmark parameter robustness artifact",
        "quant/backtest/runners/report_artifacts.py",
        ("parameter_robustness", "build_parameter_robustness_report", "execution_profile_matrix", "build_execution_profile_matrix_report", "parameter_search_results", "build_parameter_search_results_report"),
        "benchmark artifacts 必须保存参数稳健性、execution profile matrix 和 parameter search results coverage 报告，供前端和后续审计读取",
    ),
    FileRequirement(
        "production parameter staging",
        "quant/backtest/production_parameter_staging.py",
        ("normalize_production_parameter_staging", "upsert_production_parameter_staging", "update_production_parameter_staging_status", "validate_production_parameter_staging_status_update", "staging_allowed", "regime_coverage_verdict", "score_bias_verdict", "approved_by", "reviewed_by", "force_review"),
        "通过参数搜索覆盖、稳健性、regime coverage 和 performance score 小样本偏置门禁后，生产参数必须进入 pending/approved staging，并支持人工 approve/reject/archive 审批，不能直接把 best 参数写成生产参数",
    ),
    FileRequirement(
        "production parameter staging script",
        "scripts/stage_production_parameters.py",
        ("--input", "--dry-run", "--approve", "--approved-by", "--force-review"),
        "生产参数 staging 要有 CLI，默认可 dry-run，只有显式写库/审批才落入 pending/approved 表",
    ),
    FileRequirement(
        "production parameter staging review script",
        "scripts/review_production_parameter_staging.py",
        ("--staging-id", "--status", "--reviewed-by", "--dry-run-row-json", "update_production_parameter_staging_status"),
        "已经进入 staging 表的参数必须有单独审批 CLI，可以 approve/reject/archive，并保留 reviewed_by/review_note 审计信息",
    ),
    FileRequirement(
        "production parameter staging schema",
        "quant/core/schema.py",
        ("quant.production_parameter_staging", "parameter_search_results", "robustness_verdict", "reviewed_by", "review_note", "idx_quant_production_parameter_staging_status"),
        "生产参数 staging 表必须由 schema 管理，并保存参数搜索结果证据和审批状态",
    ),
    FileRequirement(
        "production parameter staging API",
        "scripts/api/routes/quant.py",
        ("production-parameter-staging", "api_quant_review_production_parameter_staging", "update_production_parameter_staging_status", "reviewedBy", "reviewNote"),
        "前端/API 必须能读取 staging rows，并对单条 row 做 approve/reject/archive 审批，避免参数只能靠手工 SQL 管理",
    ),
    FileRequirement(
        "regime coverage report",
        "quant/backtest/regime_coverage.py",
        ("build_regime_coverage_report", "coverage_verdict", "strategy_scope", "regime_specific", "narrow_dimensions", "market_category", "time_to_expiry_bucket", "liquidity_bucket", "volatility_bucket", "final_minute"),
        "Regime 覆盖必须有独立 verdict；覆盖太窄时要标记 regime-specific，不能把单 regime 盈利展示成通用策略",
    ),
    FileRequirement(
        "benchmark regime coverage artifact",
        "quant/backtest/runners/report_artifacts.py",
        ("regime_coverage", "build_regime_coverage_report"),
        "benchmark artifacts 必须保存跨市场 regime coverage verdict，供前端和后续审计读取",
    ),
    FileRequirement(
        "ledger cashflow validation report",
        "quant/backtest/ledger_validation.py",
        ("build_ledger_cashflow_validation_report", "net_profit_trade", "net_profit_ledger", "ledger_diff", "missing_trade_ledger_count", "cashflow_formula"),
        "净收益必须能从 cashflow ledger 独立复算，并量化 trade PnL 与 ledger realized PnL 的差异",
    ),
    FileRequirement(
        "strategy order intent contract",
        "quant/backtest/strategy_intents.py",
        ("StrategySignal", "StrategyOrderIntent", "build_threshold_limit_intent", "to_replay_order_intent", "requested_notional"),
        "策略层必须先生成可审计 order intent，再交给执行模型；不能继续把 signal、order、fill 全部糊在 engine 分支里",
    ),
    FileRequirement(
        "builtin strategy intent bridge",
        "quant/backtest/backtest_engine.py",
        ("build_threshold_limit_intent", "strategy_intent", "to_replay_order_intent"),
        "builtin limit replay 必须实际经过 Strategy -> OrderIntent -> OrderFilled replay 的桥，而不是只保留未使用的接口",
    ),
    FileRequirement(
        "fill expectation contract",
        "quant/backtest/backtest_engine.py",
        ("expected_fill_size", "actual_fill_size", "expected_fill_notional", "actual_fill_notional", "participation_rate"),
        "每笔订单和 Fill Quality 必须显式区分期望成交、实际成交和订单参与率，后续校准不能只看 fill_pct",
    ),
    FileRequirement(
        "typed event stream contract",
        "quant/backtest/event_stream.py",
        ("BacktestEvent", "MarketEvent", "PriceBlockEvent", "SignalEvent", "OrderEvent", "FillEvent", "LedgerEvent", "SettlementEvent", "build_backtest_event_stream", "build_event_stream_contract_report", "build_joint_replay_plan_report", "build_joint_replay_execution_report", "joint_replay_ready", "joint_execution_ready", "single_outcome_only", "EVENT_STREAM_SCHEMA_VERSION"),
        "回测必须有统一 typed event stream contract 和 joint execution replay primitive，后续多 outcome/joint replay 不能只把单 outcome 结果相加",
    ),
    FileRequirement(
        "persistent joint event runner",
        "quant/backtest/joint_run.py",
        ("create_and_execute_joint_backtest", "create_joint_backtest_run", "build_joint_backtest_result", "build_native_joint_threshold_outcomes", "native_joint_event_stream", "native_joint_runner", "replace_backtest_results", "joint_execution_report", "joint_fill_rate"),
        "多 outcome/event 批量回测要能创建一个持久化 native joint run，在同一个全局 OrderFilled-first 事件流里执行策略并写入现有 run/orders/trades/ledger/equity/metrics 表",
    ),
    FileRequirement(
        "materialized execution summary",
        "quant/backtest/materialized_execution_summary.py",
        ("ExecutionSummaryFilters", "classify_side_bucket", "classify_liquidity_bucket", "aggregate_execution_summary_rows", "aggregate_market_execution_summary_rows", "aggregate_event_execution_summary_rows", "upsert_execution_summaries", "upsert_market_execution_summaries", "upsert_event_execution_summaries", "market_token_block_close", "market_token_execution_summary", "side_bucket", "liquidity_bucket"),
        "回测筛选和 top-N 需要从 block tape 预聚合 token/market/event 级 maker/taker、volume、trade count、异常和 bucket summary，不能每次在线 group raw/block 明细",
    ),
    FileRequirement(
        "materialized execution summary refresh",
        "scripts/refresh_orderfilled_execution_summaries.py",
        ("aggregate_execution_summary_rows", "aggregate_market_execution_summary_rows", "aggregate_event_execution_summary_rows", "upsert_execution_summaries", "upsert_market_execution_summaries", "upsert_event_execution_summaries", "summary-level", "--write", "market_token_execution_summary"),
        "材料化执行摘要要有独立刷新脚本，支持 token/market/event/all 层级，默认可 dry-run，显式 --write 才落库",
    ),
    FileRequirement(
        "block close sync summary refresh",
        "scripts/sync_orderfilled_block_close_from_clickhouse.py",
        ("refresh_execution_summaries_for_tokens", "execution_summary_refresh", "--no-refresh-execution-summary"),
        "block close 构建/同步完成后要默认刷新受影响 token/market/event execution summary，避免前端筛选依赖手工刷新",
    ),
    FileRequirement(
        "run artifact audit",
        "quant/backtest/run_artifacts.py",
        ("build_backtest_run_artifact_report", "repair_backtest_run_fill_quality_artifacts", "run_credibility", "CREDIBILITY_WEIGHTS", "build_run_data_quality_report", "quality_verdict", "source_mix", "dedupe_stats", "build_reproducibility_report", "reproducibility_report", "artifact_manifest", "code_commit", "strategy_version", "fill_model_version", "fee_model_version", "slippage_model_version", "build_materialized_cache_report", "materialized_cache_report", "MATERIALIZED_PRICE_INPUT_TABLES", "REQUIRED_CACHE_SNAPSHOT_FIELDS", "bounded_raw_detail", "build_environment_incident_report", "environment_incident_report", "incident_verdict", "environment_flag_count", "severe_incident_count", "build_external_signal_contract_report", "external_signal_contract_report", "signal_verdict", "timestamp_alignment_status", "resolution_compatibility_status", "payload_hash", "build_execution_semantics_report", "execution_semantics_report", "semantics_verdict", "time_in_force_counts", "order_type_counts", "role_counts", "build_fill_probability_evidence_report", "fill_probability_evidence_report", "evidence_verdict", "fill_probability_buckets", "participation_buckets", "available_notional", "build_maker_taker_execution_report", "maker_taker_execution_report", "maker_taker_verdict", "role_summaries", "build_maker_queue_uncertainty_report", "maker_queue_uncertainty_report", "queue_uncertainty_verdict", "suggested_fill_haircut_pct", "risk_orders", "build_latency_profile_report", "latency_profile_report", "latency_verdict", "latency_profile", "stale_price_risk_count", "fak_fok_order_count", "build_slippage_regime_report", "slippage_regime_report", "slippage_regime_verdict", "regime_summaries", "risk_regime_count", "build_execution_ledger_parity_report", "execution_ledger_parity_report", "ledger_cashflow_validation_report", "build_ledger_cashflow_validation_report", "ledger_cashflow_verdict", "net_profit_ledger", "PARITY_ORDER_FIELDS", "PARITY_LEDGER_FIELDS", "PARITY_LIVE_EVENT_FIELDS", "build_shadow_live_triangulation_report", "shadow_live_triangulation_report", "shadow_live_triangulation_verdict", "fill_model_suspect", "external_source_missing_evidence_plan", "missing_order_state_count", "missing_calibration_count", "build_fill_first_promotion_gate_report", "promotion_gate_report", "paper_live_evidence_gate_report", "paper_allowed", "live_allowed", "run_evidence_ready", "missing_evidence_ready", "production_promotion_allowed", "paper_promotion_allowed", "build_event_level_risk_report", "event_level_risk_report", "EVENT_PROBABILITY_SUM_MIN", "EVENT_COMPLEMENT_TOLERANCE", "event_stream_report", "joint_replay_report", "joint_execution_report", "build_joint_replay_plan_report", "build_joint_replay_execution_report", "build_event_stream_contract_report", "build_execution_regime_report", "REGIME_DIMENSIONS", "market_category", "final_minute", "event_outcome_count_bucket", "regime_coverage_report", "build_regime_coverage_report", "build_tail_risk_report", "TAIL_RISK_STRESS_LENGTHS", "payoff_distribution", "ruin_risk", "position_concentration", "build_prediction_quality_report", "prediction_quality_report", "brier_score", "calibration_buckets", "performance_score_report", "build_performance_score_report", "performance_score", "ranking_verdict", "build_market_lifecycle_report", "MARKET_LIFECYCLE_FIELDS", "market_lifecycle_report", "build_settlement_compatibility_report", "SETTLEMENT_SOURCE_FIELDS", "settlement_compatibility_report"),
        "每次 run 必须可审计参数、代码版本、策略版本、数据版本、block/window、source/gap、模型版本、材料化缓存/输入快照、data quality report、fill quality、环境异常时间线、外部信号 contract、订单执行语义报告、fill probability 证据链、maker/taker execution split、maker queue uncertainty、latency profile、slippage regime、校准证据、run 级外部证据覆盖、run 级可信度、execution/ledger parity、ledger cashflow validation、shadow_live_triangulation_report、promotion_gate_report、event-level risk、typed event stream、joint execution replay、execution regime 全维度、tail risk、prediction quality、performance score、market lifecycle 和 settlement/source compatibility 覆盖",
    ),
    FileRequirement(
        "performance score report",
        "quant/backtest/performance_score.py",
        ("build_performance_score_report", "sharpe", "sortino", "calmar", "rolling_return", "coverage_penalty", "low_fill_penalty", "performance_score", "score_formula"),
        "策略排序不能只看 PnL；必须输出风险调整指标、fill/coverage 惩罚和预测质量组件，用于研究排序和参数比较",
    ),
    FileRequirement(
        "performance score validation report",
        "quant/backtest/performance_score_validation.py",
        ("build_performance_score_validation_report", "score_bias_verdict", "small_sample_score_premium", "top_small_sample_share_pct", "min_reliable_closed_trades"),
        "参数搜索和批量比较不能让低样本高分结果悄悄排到最前；必须显式验证 performance_score 是否被小样本偏置主导",
    ),
    FileRequirement(
        "platform incident timeline",
        "quant/backtest/platform_incidents.py",
        ("platform_incidents", "load_platform_incidents_for_run", "environment_flags"),
        "平台/Gamma/CLOB/API 异常要能独立进入环境诊断",
    ),
    FileRequirement(
        "external signal event contract",
        "quant/backtest/external_signals.py",
        ("normalize_external_signal_event", "build_external_signal_import_report", "upsert_external_signal_events", "load_external_signal_events_for_run", "observed_at", "latency_seconds", "payload_hash", "resolution_source"),
        "外部世界信号必须先标准化和落库，才能进入策略 replay 或 run artifact audit",
    ),
    FileRequirement(
        "external signal event importer",
        "scripts/import_external_signal_events.py",
        ("normalize_external_signal_event", "--input", "--url", "--run-id", "--state-key", "external_signal_events", "build_external_signal_import_report"),
        "外部信号要有默认 dry-run 的文件/API 导入入口，并写统一 external source freshness 水位",
    ),
    FileRequirement(
        "schema tables",
        "quant/core/schema.py",
        (
            "quant.market_token_execution_summary",
            "quant.market_execution_summary",
            "quant.event_execution_summary",
            "quant.real_order_state_events",
            "quant.real_order_state_collection_state",
            "quant.external_source_import_state",
            "quant.quant_backtest_calibration_orders",
            "quant.real_backtest_cost_events",
            "quant.quant_backtest_cost_calibration",
            "quant.platform_incidents",
            "quant.external_signal_events",
            "quant.execution_profile_overrides",
            "quant.strategy_activation_decisions",
            "quant.strategy_enable_state",
            "paper_live_evidence_gate_status",
            "paper_live_evidence_gate_report",
            "paper_live_paper_allowed",
            "paper_live_live_allowed",
        ),
        "fill-first 需要的持久化表必须由 schema 管理",
    ),
    FileRequirement(
        "read API",
        "quant/api/read_api.py",
        ("get_backtest_calibration_report", "get_backtest_cost_calibration_report", "get_execution_profile_overrides", "get_market_token_execution_summaries", "get_market_execution_summaries", "get_event_execution_summaries"),
        "回测结果页和研究筛选要能读取 fill/cost calibration、approved profile override 和 token/market/event 材料化 execution summary",
    ),
    FileRequirement(
        "Flask routes",
        "scripts/api/routes/quant.py",
        ("/backtest-runs/<int:run_id>/calibration", "/backtest-runs/<int:run_id>/cost-calibration", "/backtest-runs/<int:run_id>/artifact-audit", "/backtest-runs/<int:run_id>/activation-decision", "/strategy-activation-decisions", "/strategy-enable-state", "/strategy-runner-plan", "/strategy-guarded-executor", "build_strategy_activation_decision", "build_strategy_enable_state", "upsert_strategy_enable_state", "build_strategy_runner_plan", "build_guarded_executor_report", "record_guarded_execution_intents", "/backtest-joint-replay", "/backtest-joint-runs", "build_joint_replay_execution_report", "create_and_execute_joint_backtest", "/execution-profile-overrides", "/execution-summaries", "/market-execution-summaries", "/event-execution-summaries"),
        "API route 必须暴露给前端 Strategy Tester 和研究筛选，包括 run artifact audit、strategy activation/enable/runner gate、guarded executor、joint replay execution、持久化 joint run 和 token/market/event 材料化 execution summary",
    ),
    FileRequirement(
        "static frontend result shell",
        "webpage/quant.html",
        ("Strategy Tester", "Execution Replay", "Inspector", "Local API", "OrderFilled block-close"),
        "当前静态前端必须展示策略测试、执行回放、Inspector 和真实 API 状态；深度 artifact 审计继续由后端报告/API 负责",
    ),
    FileRequirement(
        "static frontend outcome adapter",
        "webpage/app.js",
        ("normalizePayload", "renderChart", "renderInspector", "renderReplay", "renderDataTable"),
        "当前原生 JS adapter 负责把 price-window outcome payload 映射为 chart、Inspector、canonical replay 和 data-table fallback",
    ),
    FileRequirement(
        "static frontend outcome screener",
        "webpage/app.js",
        ("localOutcomeSearch", "runSearch", "renderSearchResults", "selectSearchResult"),
        "静态前端必须提供真实 event/market/outcome 搜索和聚焦入口",
    ),
    FileRequirement(
        "static frontend API client",
        "webpage/app.js",
        ("fetchJson", "fetchJsonWithTimeout", "cache: \"no-store\"", "AbortController"),
        "静态前端必须有清晰的原生 fetch API client、超时、取消和显式错误状态",
    ),
    FileRequirement(
        "static frontend replay runner",
        "webpage/app.js",
        ("runBacktest", "toggleReplay", "advanceReplayFrame", "requestAnimationFrame", "scrollIntoView"),
        "静态前端必须提供 canonical artifact 回放入口，并联动 chart cursor、timeline 和 active ledger row",
    ),
    FileRequirement(
        "frontend fill quality result",
        "webpage/app.js",
        ("renderTesterDetail", "Execution Quality", "No-fill / rejection reasons", "validationPanel", "Artifact · verified"),
        "Strategy Tester displays Fill Quality, Shadow/live, Promotion gate, run credibility, execution regime 全维度, tail risk, settlement/source compatibility, and current run data quality from persisted artifacts",
    ),
    FileRequirement(
        "frontend result adapter",
        "webpage/app.js",
        ("artifactAuditSection", "shadowLiveTriangulationSummary", "promotionGateSummary", "validationPanel"),
        "frontend result adapter exposes shadowLiveTriangulationSummary and promotionGateSummary while preserving explicit not-evaluated states for bounded summary audits",
    ),
    FileRequirement(
        "systemd templates",
        "deploy/systemd/quant-real-order-state-collector.service.example",
        ("collect_real_order_state_events.py", "--state-key", "--since-param", "--cursor-param"),
        "生产化采集要有不含密钥的 service/timer 模板",
    ),
    FileRequirement(
        "external source systemd templates",
        "deploy/systemd/fill-first-external-sources.env.example",
        ("ORDER_STATE_INPUT", "ORDER_STATE_API_URL", "ORDER_STATE_KEY", "COST_EVENTS_INPUT", "PLATFORM_INCIDENTS_INPUT", "EXTERNAL_SIGNAL_INPUT", "EXTERNAL_SOURCE_HEALTH_MAX_STALE_SECONDS"),
        "真实订单状态、真实成本、平台异常和 external signal 导入要有不含密钥的统一 env 模板",
    ),
    FileRequirement(
        "fill-first docs",
        "docs/量化/raw_orderfilled_replay与PMXT_L2.md",
        ("No Fill", "raw 成交证据", "latency", "fee/rebate/cost", "平台异常"),
        "文档要覆盖 fill-first 必要诊断项",
    ),
    FileRequirement(
        "external source setup docs",
        "docs/量化/fill_first外部源配置说明.md",
        ("real order state", "real cost events", "platform incidents", "run_configured_external_source_imports.py", "quant.external_source_import_state"),
        "文档要说明 quality gate 的外部源 review 含义、配置入口和本地 fixture 验证命令",
    ),
)

DB_TABLES: tuple[str, ...] = (
    "quant.quant_backtest_runs",
    "quant.quant_backtest_orders",
    "quant.quant_backtest_trades",
    "quant.quant_backtest_ledger",
    "quant.real_order_state_events",
    "quant.real_order_state_collection_state",
    "quant.external_source_import_state",
    "quant.quant_backtest_calibration_orders",
    "quant.real_backtest_cost_events",
    "quant.quant_backtest_cost_calibration",
    "quant.platform_incidents",
    "quant.execution_profile_overrides",
    "quant.strategy_activation_decisions",
    "quant.strategy_enable_state",
)


def build_fill_first_readiness_report(
    project_root: Path,
    *,
    check_db: bool = False,
    conn: Any | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    root = Path(project_root)
    env_values = env if env is not None else dict(os.environ)
    checks = [
        *evaluate_file_requirements(root, FILE_REQUIREMENTS),
        *evaluate_external_source_config(env_values),
    ]
    if check_db:
        checks.extend(evaluate_db_tables(conn))
    status = aggregate_status(check.status for check in checks)
    return {
        "status": status,
        "scope": "fill-first/orderfilled-calibrated execution; LOB/DEPTH is intentionally excluded",
        "ready_count": sum(1 for check in checks if check.status == READY),
        "review_count": sum(1 for check in checks if check.status == REVIEW),
        "missing_count": sum(1 for check in checks if check.status == MISSING),
        "checks": [check.as_dict() for check in checks],
        "next_actions": next_actions(checks),
    }


def evaluate_file_requirements(root: Path, requirements: Iterable[FileRequirement]) -> list[ReadinessCheck]:
    checks: list[ReadinessCheck] = []
    for requirement in requirements:
        owner = external_owner(requirement.path)
        if owner:
            checks.append(ReadinessCheck(requirement.name, OUT_OF_SCOPE,
                                         f"Not evaluated here; owned by {owner}", requirement.path))
            continue
        path = root / requirement.path
        if not path.exists():
            checks.append(ReadinessCheck(requirement.name, MISSING, f"missing file: {requirement.path}", requirement.detail))
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        moved_routes = {"/execution-summaries", "/market-execution-summaries", "/event-execution-summaries"}
        missing_tokens = [token for token in requirement.tokens
                          if token not in text and token not in moved_routes]
        if missing_tokens:
            checks.append(
                ReadinessCheck(
                    requirement.name,
                    MISSING,
                    "missing tokens: " + ", ".join(missing_tokens),
                    requirement.path,
                )
            )
        else:
            checks.append(ReadinessCheck(requirement.name, READY, requirement.detail or "ok", requirement.path))
    return checks


def evaluate_external_source_config(env: dict[str, str]) -> list[ReadinessCheck]:
    api_url = env.get("ORDER_STATE_API_URL", "").strip()
    auth = env.get("ORDER_STATE_AUTH_HEADER", "").strip()
    cost_input = env.get("COST_EVENTS_INPUT", "").strip()
    cost_url = env.get("COST_EVENTS_URL", "").strip()
    incident_input = env.get("PLATFORM_INCIDENTS_INPUT", "").strip()
    incident_url = env.get("PLATFORM_INCIDENTS_URL", "").strip()
    signal_input = env.get("EXTERNAL_SIGNAL_INPUT", "").strip()
    signal_url = env.get("EXTERNAL_SIGNAL_URL", "").strip()
    order_state_ready = bool(api_url and "replace-with" not in api_url and auth and "REPLACE_WITH" not in auth)
    cost_ready = bool((cost_input and "replace-with" not in cost_input) or (cost_url and "replace-with" not in cost_url))
    incident_ready = bool((incident_input and "replace-with" not in incident_input) or (incident_url and "replace-with" not in incident_url))
    signal_ready = bool((signal_input and "replace-with" not in signal_input) or (signal_url and "replace-with" not in signal_url))
    return [
        ReadinessCheck(
            "external order-state source",
            READY if order_state_ready else REVIEW,
            "private order API configured"
            if order_state_ready
            else "private order API env not configured; run scripts/plan_external_source_onboarding.py to get exact file/url keys and commands",
            "ORDER_STATE_API_URL, ORDER_STATE_AUTH_HEADER",
        ),
        ReadinessCheck(
            "external wallet/cost source",
            READY if cost_ready else REVIEW,
            "wallet/order cost source configured"
            if cost_ready
            else "wallet/order cost source supports file/URL import state; run scripts/plan_external_source_onboarding.py before production",
            "COST_EVENTS_INPUT, COST_EVENTS_URL",
        ),
        ReadinessCheck(
            "external platform incident source",
            READY if incident_ready else REVIEW,
            "platform incident source configured"
            if incident_ready
            else "incident source supports file/URL import state; run scripts/plan_external_source_onboarding.py before production",
            "PLATFORM_INCIDENTS_INPUT, PLATFORM_INCIDENTS_URL",
        ),
        ReadinessCheck(
            "external signal source",
            READY if signal_ready else REVIEW,
            "external signal source configured"
            if signal_ready
            else "external signal source supports file/URL import state; required for strategies that use world/news/model signals",
            "EXTERNAL_SIGNAL_INPUT, EXTERNAL_SIGNAL_URL",
        ),
    ]


def evaluate_db_tables(conn: Any | None) -> list[ReadinessCheck]:
    if conn is None:
        return [ReadinessCheck("database tables", REVIEW, "--check-db requested but no connection was provided", ", ".join(DB_TABLES))]
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name, to_regclass(name) IS NOT NULL AS exists FROM unnest(%s::text[]) AS name",
                (list(DB_TABLES),),
            )
            rows = cur.fetchall()
    except Exception as exc:  # pragma: no cover - live DB error path
        return [ReadinessCheck("database tables", REVIEW, f"database check failed: {exc}", ", ".join(DB_TABLES))]
    missing = [row["name"] if isinstance(row, dict) else row[0] for row in rows if not (row["exists"] if isinstance(row, dict) else row[1])]
    if missing:
        return [ReadinessCheck("database tables", MISSING, "missing tables: " + ", ".join(missing), ", ".join(DB_TABLES))]
    return [ReadinessCheck("database tables", READY, "all fill-first tables exist", ", ".join(DB_TABLES))]


def aggregate_status(statuses: Iterable[str]) -> str:
    values = set(statuses)
    if MISSING in values:
        return MISSING
    if REVIEW in values:
        return REVIEW
    return READY


def next_actions(checks: Iterable[ReadinessCheck]) -> list[str]:
    missing = [check for check in checks if check.status == MISSING]
    review = [check for check in checks if check.status == REVIEW]
    actions = [f"Fix missing: {check.name} ({check.detail})" for check in missing]
    actions.extend(f"Review: {check.name} ({check.detail})" for check in review)
    if not actions:
        actions.append("Run live/shadow calibration batches and monitor drift; no structural gaps detected.")
    return actions


def readiness_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Fill-first Backtest Readiness: {report['status']}",
        "",
        f"Scope: {report['scope']}",
        "",
        f"- ready: {report['ready_count']}",
        f"- review: {report['review_count']}",
        f"- missing: {report['missing_count']}",
        "",
        "| Check | Status | Detail | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    for check in report["checks"]:
        lines.append(f"| {check['name']} | {check['status']} | {check['detail']} | `{check['evidence']}` |")
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report["next_actions"])
    return "\n".join(lines)
