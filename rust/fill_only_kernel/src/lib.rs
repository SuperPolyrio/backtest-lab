use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use numpy::{IntoPyArray, PyArray1, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub const SCALE: i64 = 10_000_000_000;

type BernoulliArrays<'py> = (Bound<'py, PyArray1<u8>>, Bound<'py, PyArray1<u64>>);
type MergeHeapItem = Reverse<(i64, i64, i64, usize, usize)>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MonteCarloOutput {
    expected_fill: i64,
    fill_probability: i64,
    full_fill_probability: i64,
    q10: i64,
    q50: i64,
    q90: i64,
    next_state: u64,
}

#[derive(Clone)]
pub struct MatchInput {
    pub trade_market: Vec<i64>,
    pub trade_asset: Vec<i64>,
    pub trade_side: Vec<u8>,
    pub trade_block: Vec<i64>,
    pub trade_time_us: Vec<i64>,
    pub trade_price: Vec<i64>,
    pub trade_size: Vec<i64>,
    pub trade_consumed: Vec<i64>,
    pub order_market: Vec<i64>,
    pub order_asset: Vec<i64>,
    pub order_side: Vec<u8>,
    pub order_arrival_block: Vec<i64>,
    pub order_deadline_block: Vec<i64>,
    pub order_arrival_time_us: Vec<i64>,
    pub order_deadline_time_us: Vec<i64>,
    pub order_limit: Vec<i64>,
    pub order_size: Vec<i64>,
    pub order_fill_cap: Vec<i64>,
    pub order_rejected: Vec<u8>,
    pub order_participation: Vec<i64>,
    pub order_price_buffer: Vec<i64>,
    pub order_excluded_trade_index: Vec<i64>,
    pub order_require_full: Vec<u8>,
    pub order_min_future_count: Vec<i64>,
    pub order_min_future_volume: Vec<i64>,
}

#[derive(Clone)]
struct PreparedTradeTapeCore {
    trade_block: Vec<i64>,
    trade_time_us: Vec<i64>,
    trade_price: Vec<i64>,
    trade_size: Vec<i64>,
    groups: HashMap<(i64, i64, u8), Vec<usize>>,
}

impl PreparedTradeTapeCore {
    #[allow(clippy::too_many_arguments)]
    fn new(
        trade_market: Vec<i64>,
        trade_asset: Vec<i64>,
        trade_side: Vec<u8>,
        trade_block: Vec<i64>,
        trade_time_us: Vec<i64>,
        trade_price: Vec<i64>,
        trade_size: Vec<i64>,
    ) -> Result<Self, String> {
        let trade_len = trade_market.len();
        for (name, len) in [
            ("trade_asset", trade_asset.len()),
            ("trade_side", trade_side.len()),
            ("trade_block", trade_block.len()),
            ("trade_time_us", trade_time_us.len()),
            ("trade_price", trade_price.len()),
            ("trade_size", trade_size.len()),
        ] {
            if len != trade_len {
                return Err(format!("{name} length mismatch"));
            }
        }
        let mut groups: HashMap<(i64, i64, u8), Vec<usize>> = HashMap::new();
        for index in 0..trade_len {
            groups
                .entry((trade_market[index], trade_asset[index], trade_side[index]))
                .or_default()
                .push(index);
        }
        Ok(Self {
            trade_block,
            trade_time_us,
            trade_price,
            trade_size,
            groups,
        })
    }
}

#[derive(Clone)]
struct MatchOrders {
    order_market: Vec<i64>,
    order_asset: Vec<i64>,
    order_side: Vec<u8>,
    order_arrival_block: Vec<i64>,
    order_deadline_block: Vec<i64>,
    order_arrival_time_us: Vec<i64>,
    order_deadline_time_us: Vec<i64>,
    order_limit: Vec<i64>,
    order_size: Vec<i64>,
    order_fill_cap: Vec<i64>,
    order_rejected: Vec<u8>,
    order_participation: Vec<i64>,
    order_price_buffer: Vec<i64>,
    order_excluded_trade_index: Vec<i64>,
    order_require_full: Vec<u8>,
    order_min_future_count: Vec<i64>,
    order_min_future_volume: Vec<i64>,
}

#[derive(Default, Clone)]
pub struct MatchOutput {
    pub fill_order_index: Vec<i64>,
    pub fill_trade_index: Vec<i64>,
    pub fill_quantity: Vec<i64>,
    pub fill_price: Vec<i64>,
    pub fill_allocated_capacity: Vec<i64>,
    pub status: Vec<u8>,
    pub remaining: Vec<i64>,
    pub eligible_volume: Vec<i64>,
    pub reason: Vec<u8>,
    pub candidate_count: Vec<i64>,
    pub trade_consumed: Vec<i64>,
}

pub fn match_taker_core(input: MatchInput) -> Result<MatchOutput, String> {
    validate_input(&input)?;
    let tape = PreparedTradeTapeCore::new(
        input.trade_market,
        input.trade_asset,
        input.trade_side,
        input.trade_block,
        input.trade_time_us,
        input.trade_price,
        input.trade_size,
    )?;
    let orders = MatchOrders {
        order_market: input.order_market,
        order_asset: input.order_asset,
        order_side: input.order_side,
        order_arrival_block: input.order_arrival_block,
        order_deadline_block: input.order_deadline_block,
        order_arrival_time_us: input.order_arrival_time_us,
        order_deadline_time_us: input.order_deadline_time_us,
        order_limit: input.order_limit,
        order_size: input.order_size,
        order_fill_cap: input.order_fill_cap,
        order_rejected: input.order_rejected,
        order_participation: input.order_participation,
        order_price_buffer: input.order_price_buffer,
        order_excluded_trade_index: input.order_excluded_trade_index,
        order_require_full: input.order_require_full,
        order_min_future_count: input.order_min_future_count,
        order_min_future_volume: input.order_min_future_volume,
    };
    match_taker_prepared_core(&tape, orders, input.trade_consumed)
}

fn match_taker_prepared_core(
    tape: &PreparedTradeTapeCore,
    orders: MatchOrders,
    trade_consumed: Vec<i64>,
) -> Result<MatchOutput, String> {
    validate_orders(&orders)?;
    if trade_consumed.len() != tape.trade_size.len() {
        return Err("trade_consumed length mismatch".into());
    }
    let mut output = MatchOutput {
        trade_consumed,
        ..MatchOutput::default()
    };
    for order_index in 0..orders.order_market.len() {
        let key = (
            orders.order_market[order_index],
            orders.order_asset[order_index],
            orders.order_side[order_index],
        );
        let mut remaining = orders.order_size[order_index].max(0);
        let mut fill_cap_remaining = orders.order_fill_cap[order_index].max(0).min(remaining);
        if orders.order_rejected[order_index] != 0 {
            output.status.push(0);
            output.remaining.push(remaining);
            output.eligible_volume.push(0);
            output.reason.push(9);
            output.candidate_count.push(0);
            continue;
        }
        let Some(candidates) = tape.groups.get(&key).map(Vec::as_slice) else {
            output.status.push(0);
            output.remaining.push(remaining);
            output.eligible_volume.push(0);
            output.reason.push(1);
            output.candidate_count.push(0);
            continue;
        };
        let mut eligible_volume = 0_i64;
        let mut saw_limit = false;
        let mut saw_buffer_blocked = false;
        let mut saw_capacity_exhausted = false;
        let mut candidate_count = 0_i64;
        let mut pending: Vec<(usize, i64, i64, i64)> = Vec::new();
        let mut pending_by_trade: HashMap<usize, i64> = HashMap::new();
        let (candidate_start, candidate_end) =
            candidate_bounds(candidates, tape, &orders, order_index);
        let (future_count, future_volume) = eligible_future_evidence(
            &candidates[candidate_start..candidate_end],
            tape,
            &orders,
            order_index,
        );
        if future_count < orders.order_min_future_count[order_index].max(0) {
            output.status.push(0);
            output.remaining.push(remaining);
            output.eligible_volume.push(0);
            output.reason.push(7);
            output.candidate_count.push(0);
            continue;
        }
        if future_volume < orders.order_min_future_volume[order_index].max(0) {
            output.status.push(0);
            output.remaining.push(remaining);
            output.eligible_volume.push(0);
            output.reason.push(8);
            output.candidate_count.push(0);
            continue;
        }
        for &trade_index in &candidates[candidate_start..candidate_end] {
            if remaining <= 0 || fill_cap_remaining <= 0 {
                break;
            }
            if tape.trade_block[trade_index] < orders.order_arrival_block[order_index]
                || tape.trade_block[trade_index] > orders.order_deadline_block[order_index]
                || tape.trade_time_us[trade_index] < orders.order_arrival_time_us[order_index]
                || tape.trade_time_us[trade_index] > orders.order_deadline_time_us[order_index]
            {
                continue;
            }
            candidate_count += 1;
            if orders.order_excluded_trade_index[order_index] == trade_index as i64 {
                continue;
            }
            let side = orders.order_side[order_index];
            let historical_price = tape.trade_price[trade_index];
            let limit = orders.order_limit[order_index];
            if !limit_allows(side, historical_price, limit) {
                continue;
            }
            saw_limit = true;
            eligible_volume = eligible_volume.saturating_add(tape.trade_size[trade_index]);
            let exec_price = execution_price(
                side,
                historical_price,
                orders.order_price_buffer[order_index],
            );
            if !limit_allows(side, exec_price, limit) {
                saw_buffer_blocked = true;
                continue;
            }
            let cap = mul_scaled_round_half_up(
                tape.trade_size[trade_index],
                orders.order_participation[order_index],
            )?;
            let already_pending = *pending_by_trade.get(&trade_index).unwrap_or(&0);
            let available = cap
                .saturating_sub(output.trade_consumed[trade_index])
                .saturating_sub(already_pending)
                .max(0);
            if available <= 0 {
                saw_capacity_exhausted = true;
                continue;
            }
            let quantity = remaining.min(available).min(fill_cap_remaining);
            if quantity <= 0 {
                continue;
            }
            pending.push((trade_index, quantity, exec_price, available));
            *pending_by_trade.entry(trade_index).or_insert(0) += quantity;
            remaining -= quantity;
            fill_cap_remaining -= quantity;
        }
        let full_required = orders.order_require_full[order_index] != 0;
        if full_required && remaining > 0 {
            pending.clear();
            remaining = orders.order_size[order_index].max(0);
        } else {
            for (trade_index, quantity, exec_price, allocated_capacity) in pending.iter().copied() {
                output.trade_consumed[trade_index] =
                    output.trade_consumed[trade_index].saturating_add(quantity);
                output.fill_order_index.push(order_index as i64);
                output.fill_trade_index.push(trade_index as i64);
                output.fill_quantity.push(quantity);
                output.fill_price.push(exec_price);
                output.fill_allocated_capacity.push(allocated_capacity);
            }
        }
        let filled = orders.order_size[order_index]
            .max(0)
            .saturating_sub(remaining);
        output.status.push(if filled <= 0 {
            0
        } else if remaining <= 0 {
            2
        } else {
            1
        });
        output.remaining.push(remaining);
        output.eligible_volume.push(eligible_volume);
        output.candidate_count.push(candidate_count);
        output.reason.push(if full_required && remaining > 0 {
            4
        } else if !saw_limit {
            1
        } else if saw_buffer_blocked {
            2
        } else if saw_capacity_exhausted {
            3
        } else if remaining > 0 {
            5
        } else {
            0
        });
    }
    Ok(output)
}

fn eligible_future_evidence(
    candidates: &[usize],
    tape: &PreparedTradeTapeCore,
    orders: &MatchOrders,
    order_index: usize,
) -> (i64, i64) {
    let mut count = 0_i64;
    let mut volume = 0_i64;
    for &trade_index in candidates {
        if orders.order_excluded_trade_index[order_index] == trade_index as i64 {
            continue;
        }
        let side = orders.order_side[order_index];
        let historical_price = tape.trade_price[trade_index];
        let limit = orders.order_limit[order_index];
        if !limit_allows(side, historical_price, limit) {
            continue;
        }
        let exec_price = execution_price(
            side,
            historical_price,
            orders.order_price_buffer[order_index],
        );
        if !limit_allows(side, exec_price, limit) {
            continue;
        }
        count = count.saturating_add(1);
        volume = volume.saturating_add(tape.trade_size[trade_index]);
    }
    (count, volume)
}

fn candidate_bounds(
    candidates: &[usize],
    tape: &PreparedTradeTapeCore,
    orders: &MatchOrders,
    order_index: usize,
) -> (usize, usize) {
    let mut start = 0;
    let mut end = candidates.len();
    let arrival_block = orders.order_arrival_block[order_index];
    let deadline_block = orders.order_deadline_block[order_index];
    let arrival_time = orders.order_arrival_time_us[order_index];
    let deadline_time = orders.order_deadline_time_us[order_index];
    if arrival_block != i64::MIN {
        start =
            start.max(candidates.partition_point(|index| tape.trade_block[*index] < arrival_block));
    }
    if deadline_block != i64::MAX {
        end =
            end.min(candidates.partition_point(|index| tape.trade_block[*index] <= deadline_block));
    }
    if arrival_time != i64::MIN {
        start = start
            .max(candidates.partition_point(|index| tape.trade_time_us[*index] < arrival_time));
    }
    if deadline_time != i64::MAX {
        end = end
            .min(candidates.partition_point(|index| tape.trade_time_us[*index] <= deadline_time));
    }
    (start.min(end), end)
}

fn validate_input(input: &MatchInput) -> Result<(), String> {
    let trade_len = input.trade_market.len();
    for (name, len) in [
        ("trade_asset", input.trade_asset.len()),
        ("trade_side", input.trade_side.len()),
        ("trade_block", input.trade_block.len()),
        ("trade_time_us", input.trade_time_us.len()),
        ("trade_price", input.trade_price.len()),
        ("trade_size", input.trade_size.len()),
        ("trade_consumed", input.trade_consumed.len()),
    ] {
        if len != trade_len {
            return Err(format!("{name} length mismatch"));
        }
    }
    let order_len = input.order_market.len();
    for (name, len) in [
        ("order_asset", input.order_asset.len()),
        ("order_side", input.order_side.len()),
        ("order_arrival_block", input.order_arrival_block.len()),
        ("order_deadline_block", input.order_deadline_block.len()),
        ("order_arrival_time_us", input.order_arrival_time_us.len()),
        ("order_deadline_time_us", input.order_deadline_time_us.len()),
        ("order_limit", input.order_limit.len()),
        ("order_size", input.order_size.len()),
        ("order_fill_cap", input.order_fill_cap.len()),
        ("order_rejected", input.order_rejected.len()),
        ("order_participation", input.order_participation.len()),
        ("order_price_buffer", input.order_price_buffer.len()),
        (
            "order_excluded_trade_index",
            input.order_excluded_trade_index.len(),
        ),
        ("order_require_full", input.order_require_full.len()),
        ("order_min_future_count", input.order_min_future_count.len()),
        (
            "order_min_future_volume",
            input.order_min_future_volume.len(),
        ),
    ] {
        if len != order_len {
            return Err(format!("{name} length mismatch"));
        }
    }
    Ok(())
}

fn validate_orders(orders: &MatchOrders) -> Result<(), String> {
    let order_len = orders.order_market.len();
    for (name, len) in [
        ("order_asset", orders.order_asset.len()),
        ("order_side", orders.order_side.len()),
        ("order_arrival_block", orders.order_arrival_block.len()),
        ("order_deadline_block", orders.order_deadline_block.len()),
        ("order_arrival_time_us", orders.order_arrival_time_us.len()),
        (
            "order_deadline_time_us",
            orders.order_deadline_time_us.len(),
        ),
        ("order_limit", orders.order_limit.len()),
        ("order_size", orders.order_size.len()),
        ("order_fill_cap", orders.order_fill_cap.len()),
        ("order_rejected", orders.order_rejected.len()),
        ("order_participation", orders.order_participation.len()),
        ("order_price_buffer", orders.order_price_buffer.len()),
        (
            "order_excluded_trade_index",
            orders.order_excluded_trade_index.len(),
        ),
        ("order_require_full", orders.order_require_full.len()),
        (
            "order_min_future_count",
            orders.order_min_future_count.len(),
        ),
        (
            "order_min_future_volume",
            orders.order_min_future_volume.len(),
        ),
    ] {
        if len != order_len {
            return Err(format!("{name} length mismatch"));
        }
    }
    Ok(())
}

fn limit_allows(side: u8, price: i64, limit: i64) -> bool {
    if side == 0 {
        price <= limit
    } else {
        price >= limit
    }
}

fn execution_price(side: u8, historical: i64, buffer: i64) -> i64 {
    let value = if side == 0 {
        historical.saturating_add(buffer)
    } else {
        historical.saturating_sub(buffer)
    };
    value.clamp(0, SCALE)
}

fn mul_scaled_round_half_up(left: i64, right: i64) -> Result<i64, String> {
    if left < 0 || right < 0 {
        return Err("fixed-point multiplication requires non-negative values".into());
    }
    let product = (left as i128) * (right as i128);
    let rounded = (product + (SCALE as i128 / 2)) / SCALE as i128;
    i64::try_from(rounded).map_err(|_| "fixed-point multiplication overflow".into())
}

#[pyclass(name = "PreparedTradeTape")]
struct PreparedTradeTapeHandle {
    inner: Arc<PreparedTradeTapeCore>,
}

#[pymethods]
impl PreparedTradeTapeHandle {
    #[new]
    #[allow(clippy::too_many_arguments)]
    fn new(
        trade_market: PyReadonlyArray1<'_, i64>,
        trade_asset: PyReadonlyArray1<'_, i64>,
        trade_side: PyReadonlyArray1<'_, u8>,
        trade_block: PyReadonlyArray1<'_, i64>,
        trade_time_us: PyReadonlyArray1<'_, i64>,
        trade_price: PyReadonlyArray1<'_, i64>,
        trade_size: PyReadonlyArray1<'_, i64>,
    ) -> PyResult<Self> {
        let inner = PreparedTradeTapeCore::new(
            slice_vec(&trade_market)?,
            slice_vec(&trade_asset)?,
            slice_vec(&trade_side)?,
            slice_vec(&trade_block)?,
            slice_vec(&trade_time_us)?,
            slice_vec(&trade_price)?,
            slice_vec(&trade_size)?,
        )
        .map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.inner.trade_size.len()
    }

    #[getter]
    fn group_count(&self) -> usize {
        self.inner.groups.len()
    }

    fn replay_session(
        &self,
        trade_consumed: PyReadonlyArray1<'_, i64>,
    ) -> PyResult<ReplaySessionHandle> {
        let consumed = slice_vec(&trade_consumed)?;
        if consumed.len() != self.inner.trade_size.len() {
            return Err(PyValueError::new_err("trade_consumed length mismatch"));
        }
        Ok(ReplaySessionHandle {
            tape: Arc::clone(&self.inner),
            trade_consumed: consumed,
        })
    }
}

#[pyclass(name = "ReplaySession")]
struct ReplaySessionHandle {
    tape: Arc<PreparedTradeTapeCore>,
    trade_consumed: Vec<i64>,
}

#[pymethods]
impl ReplaySessionHandle {
    #[allow(clippy::too_many_arguments)]
    fn match_taker_batch<'py>(
        &mut self,
        py: Python<'py>,
        order_market: PyReadonlyArray1<'py, i64>,
        order_asset: PyReadonlyArray1<'py, i64>,
        order_side: PyReadonlyArray1<'py, u8>,
        order_arrival_block: PyReadonlyArray1<'py, i64>,
        order_deadline_block: PyReadonlyArray1<'py, i64>,
        order_arrival_time_us: PyReadonlyArray1<'py, i64>,
        order_deadline_time_us: PyReadonlyArray1<'py, i64>,
        order_limit: PyReadonlyArray1<'py, i64>,
        order_size: PyReadonlyArray1<'py, i64>,
        order_fill_cap: PyReadonlyArray1<'py, i64>,
        order_rejected: PyReadonlyArray1<'py, u8>,
        order_participation: PyReadonlyArray1<'py, i64>,
        order_price_buffer: PyReadonlyArray1<'py, i64>,
        order_excluded_trade_index: PyReadonlyArray1<'py, i64>,
        order_require_full: PyReadonlyArray1<'py, u8>,
        order_min_future_count: PyReadonlyArray1<'py, i64>,
        order_min_future_volume: PyReadonlyArray1<'py, i64>,
    ) -> PyResult<Py<PyAny>> {
        let orders = MatchOrders {
            order_market: slice_vec(&order_market)?,
            order_asset: slice_vec(&order_asset)?,
            order_side: slice_vec(&order_side)?,
            order_arrival_block: slice_vec(&order_arrival_block)?,
            order_deadline_block: slice_vec(&order_deadline_block)?,
            order_arrival_time_us: slice_vec(&order_arrival_time_us)?,
            order_deadline_time_us: slice_vec(&order_deadline_time_us)?,
            order_limit: slice_vec(&order_limit)?,
            order_size: slice_vec(&order_size)?,
            order_fill_cap: slice_vec(&order_fill_cap)?,
            order_rejected: slice_vec(&order_rejected)?,
            order_participation: slice_vec(&order_participation)?,
            order_price_buffer: slice_vec(&order_price_buffer)?,
            order_excluded_trade_index: slice_vec(&order_excluded_trade_index)?,
            order_require_full: slice_vec(&order_require_full)?,
            order_min_future_count: slice_vec(&order_min_future_count)?,
            order_min_future_volume: slice_vec(&order_min_future_volume)?,
        };
        let tape = Arc::clone(&self.tape);
        let consumed = std::mem::take(&mut self.trade_consumed);
        let (mut output, consumed) = py
            .allow_threads(move || {
                let mut output = match_taker_prepared_core(&tape, orders, consumed)?;
                let consumed = std::mem::take(&mut output.trade_consumed);
                Ok::<_, String>((output, consumed))
            })
            .map_err(PyValueError::new_err)?;
        self.trade_consumed = consumed;
        match_output_dict(py, &mut output, false)
    }

    fn consumed_snapshot<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i64>> {
        self.trade_consumed.clone().into_pyarray(py)
    }
}

fn match_output_dict(
    py: Python<'_>,
    output: &mut MatchOutput,
    include_consumed: bool,
) -> PyResult<Py<PyAny>> {
    let result = PyDict::new(py);
    result.set_item(
        "fill_order_index",
        std::mem::take(&mut output.fill_order_index).into_pyarray(py),
    )?;
    result.set_item(
        "fill_trade_index",
        std::mem::take(&mut output.fill_trade_index).into_pyarray(py),
    )?;
    result.set_item(
        "fill_quantity",
        std::mem::take(&mut output.fill_quantity).into_pyarray(py),
    )?;
    result.set_item(
        "fill_price",
        std::mem::take(&mut output.fill_price).into_pyarray(py),
    )?;
    result.set_item(
        "fill_allocated_capacity",
        std::mem::take(&mut output.fill_allocated_capacity).into_pyarray(py),
    )?;
    result.set_item(
        "status",
        std::mem::take(&mut output.status).into_pyarray(py),
    )?;
    result.set_item(
        "remaining",
        std::mem::take(&mut output.remaining).into_pyarray(py),
    )?;
    result.set_item(
        "eligible_volume",
        std::mem::take(&mut output.eligible_volume).into_pyarray(py),
    )?;
    result.set_item(
        "reason",
        std::mem::take(&mut output.reason).into_pyarray(py),
    )?;
    result.set_item(
        "candidate_count",
        std::mem::take(&mut output.candidate_count).into_pyarray(py),
    )?;
    if include_consumed {
        result.set_item(
            "trade_consumed",
            std::mem::take(&mut output.trade_consumed).into_pyarray(py),
        )?;
    }
    Ok(result.into_any().unbind())
}

#[derive(Default)]
struct L2MatchOutput {
    fill_order_index: Vec<i64>,
    fill_level_index: Vec<i64>,
    fill_quantity: Vec<i64>,
    fill_price: Vec<i64>,
    residual_before: Vec<i64>,
    residual_after: Vec<i64>,
    status: Vec<u8>,
    remaining: Vec<i64>,
}

#[allow(clippy::too_many_arguments)]
fn match_l2_snapshot_core(
    level_offsets: &[i64],
    level_prices: &[i64],
    level_sizes: &[i64],
    order_side: &[u8],
    order_limit: &[i64],
    order_size: &[i64],
    depth_haircut: &[i64],
    require_full: &[u8],
) -> Result<L2MatchOutput, String> {
    if level_offsets.len() != order_size.len() + 1
        || level_offsets.first() != Some(&0)
        || level_offsets.last() != Some(&(level_prices.len() as i64))
        || level_prices.len() != level_sizes.len()
    {
        return Err("invalid L2 level offsets or lengths".into());
    }
    for (name, len) in [
        ("order_side", order_side.len()),
        ("order_limit", order_limit.len()),
        ("depth_haircut", depth_haircut.len()),
        ("require_full", require_full.len()),
    ] {
        if len != order_size.len() {
            return Err(format!("{name} length mismatch"));
        }
    }
    let mut output = L2MatchOutput::default();
    for order_index in 0..order_size.len() {
        let start =
            usize::try_from(level_offsets[order_index]).map_err(|_| "negative L2 level offset")?;
        let end = usize::try_from(level_offsets[order_index + 1])
            .map_err(|_| "negative L2 level offset")?;
        if start > end || end > level_prices.len() {
            return Err("invalid L2 level offset order".into());
        }
        let requested = order_size[order_index].max(0);
        let mut remaining = requested;
        let mut pending: Vec<(usize, i64, i64, i64, i64)> = Vec::new();
        for level_index in start..end {
            if remaining <= 0 {
                break;
            }
            let price = level_prices[level_index];
            if !limit_allows(order_side[order_index], price, order_limit[order_index]) {
                continue;
            }
            let available = mul_scaled_round_half_up(
                level_sizes[level_index].max(0),
                depth_haircut[order_index].clamp(0, SCALE),
            )?;
            if available <= 0 {
                continue;
            }
            let quantity = remaining.min(available);
            pending.push((
                level_index,
                quantity,
                price,
                available,
                available - quantity,
            ));
            remaining -= quantity;
        }
        if require_full[order_index] != 0 && remaining > 0 {
            pending.clear();
            remaining = requested;
        }
        for (level_index, quantity, price, before, after) in pending {
            output.fill_order_index.push(order_index as i64);
            output.fill_level_index.push(level_index as i64);
            output.fill_quantity.push(quantity);
            output.fill_price.push(price);
            output.residual_before.push(before);
            output.residual_after.push(after);
        }
        let filled = requested - remaining;
        output.status.push(if filled <= 0 {
            0
        } else if remaining <= 0 {
            2
        } else {
            1
        });
        output.remaining.push(remaining);
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn match_l2_snapshot_batch<'py>(
    py: Python<'py>,
    level_offsets: PyReadonlyArray1<'py, i64>,
    level_prices: PyReadonlyArray1<'py, i64>,
    level_sizes: PyReadonlyArray1<'py, i64>,
    order_side: PyReadonlyArray1<'py, u8>,
    order_limit: PyReadonlyArray1<'py, i64>,
    order_size: PyReadonlyArray1<'py, i64>,
    depth_haircut: PyReadonlyArray1<'py, i64>,
    require_full: PyReadonlyArray1<'py, u8>,
) -> PyResult<Py<PyAny>> {
    let level_offsets = slice_vec(&level_offsets)?;
    let level_prices = slice_vec(&level_prices)?;
    let level_sizes = slice_vec(&level_sizes)?;
    let order_side = slice_vec(&order_side)?;
    let order_limit = slice_vec(&order_limit)?;
    let order_size = slice_vec(&order_size)?;
    let depth_haircut = slice_vec(&depth_haircut)?;
    let require_full = slice_vec(&require_full)?;
    let output = py
        .allow_threads(move || {
            match_l2_snapshot_core(
                &level_offsets,
                &level_prices,
                &level_sizes,
                &order_side,
                &order_limit,
                &order_size,
                &depth_haircut,
                &require_full,
            )
        })
        .map_err(PyValueError::new_err)?;
    let result = PyDict::new(py);
    result.set_item("fill_order_index", output.fill_order_index.into_pyarray(py))?;
    result.set_item("fill_level_index", output.fill_level_index.into_pyarray(py))?;
    result.set_item("fill_quantity", output.fill_quantity.into_pyarray(py))?;
    result.set_item("fill_price", output.fill_price.into_pyarray(py))?;
    result.set_item("residual_before", output.residual_before.into_pyarray(py))?;
    result.set_item("residual_after", output.residual_after.into_pyarray(py))?;
    result.set_item("status", output.status.into_pyarray(py))?;
    result.set_item("remaining", output.remaining.into_pyarray(py))?;
    Ok(result.into_any().unbind())
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn match_taker_batch<'py>(
    py: Python<'py>,
    trade_market: PyReadonlyArray1<'py, i64>,
    trade_asset: PyReadonlyArray1<'py, i64>,
    trade_side: PyReadonlyArray1<'py, u8>,
    trade_block: PyReadonlyArray1<'py, i64>,
    trade_time_us: PyReadonlyArray1<'py, i64>,
    trade_price: PyReadonlyArray1<'py, i64>,
    trade_size: PyReadonlyArray1<'py, i64>,
    trade_consumed: PyReadonlyArray1<'py, i64>,
    order_market: PyReadonlyArray1<'py, i64>,
    order_asset: PyReadonlyArray1<'py, i64>,
    order_side: PyReadonlyArray1<'py, u8>,
    order_arrival_block: PyReadonlyArray1<'py, i64>,
    order_deadline_block: PyReadonlyArray1<'py, i64>,
    order_arrival_time_us: PyReadonlyArray1<'py, i64>,
    order_deadline_time_us: PyReadonlyArray1<'py, i64>,
    order_limit: PyReadonlyArray1<'py, i64>,
    order_size: PyReadonlyArray1<'py, i64>,
    order_fill_cap: PyReadonlyArray1<'py, i64>,
    order_rejected: PyReadonlyArray1<'py, u8>,
    order_participation: PyReadonlyArray1<'py, i64>,
    order_price_buffer: PyReadonlyArray1<'py, i64>,
    order_excluded_trade_index: PyReadonlyArray1<'py, i64>,
    order_require_full: PyReadonlyArray1<'py, u8>,
    order_min_future_count: PyReadonlyArray1<'py, i64>,
    order_min_future_volume: PyReadonlyArray1<'py, i64>,
) -> PyResult<Py<PyAny>> {
    let input = MatchInput {
        trade_market: slice_vec(&trade_market)?,
        trade_asset: slice_vec(&trade_asset)?,
        trade_side: slice_vec(&trade_side)?,
        trade_block: slice_vec(&trade_block)?,
        trade_time_us: slice_vec(&trade_time_us)?,
        trade_price: slice_vec(&trade_price)?,
        trade_size: slice_vec(&trade_size)?,
        trade_consumed: slice_vec(&trade_consumed)?,
        order_market: slice_vec(&order_market)?,
        order_asset: slice_vec(&order_asset)?,
        order_side: slice_vec(&order_side)?,
        order_arrival_block: slice_vec(&order_arrival_block)?,
        order_deadline_block: slice_vec(&order_deadline_block)?,
        order_arrival_time_us: slice_vec(&order_arrival_time_us)?,
        order_deadline_time_us: slice_vec(&order_deadline_time_us)?,
        order_limit: slice_vec(&order_limit)?,
        order_size: slice_vec(&order_size)?,
        order_fill_cap: slice_vec(&order_fill_cap)?,
        order_rejected: slice_vec(&order_rejected)?,
        order_participation: slice_vec(&order_participation)?,
        order_price_buffer: slice_vec(&order_price_buffer)?,
        order_excluded_trade_index: slice_vec(&order_excluded_trade_index)?,
        order_require_full: slice_vec(&order_require_full)?,
        order_min_future_count: slice_vec(&order_min_future_count)?,
        order_min_future_volume: slice_vec(&order_min_future_volume)?,
    };
    let mut output = py
        .allow_threads(move || match_taker_core(input))
        .map_err(PyValueError::new_err)?;
    match_output_dict(py, &mut output, true)
}

fn slice_vec<T: numpy::Element + Copy>(array: &PyReadonlyArray1<'_, T>) -> PyResult<Vec<T>> {
    Ok(array.as_slice()?.to_vec())
}

#[pyfunction]
fn bernoulli_stateful<'py>(
    py: Python<'py>,
    probability_scaled: PyReadonlyArray1<'py, i64>,
    states: PyReadonlyArray1<'py, u64>,
) -> PyResult<BernoulliArrays<'py>> {
    let probabilities = probability_scaled.as_slice()?.to_vec();
    let mut next_states = states.as_slice()?.to_vec();
    if probabilities.len() != next_states.len() {
        return Err(PyValueError::new_err("probability/state length mismatch"));
    }
    let draws = py.allow_threads(|| {
        probabilities
            .iter()
            .zip(next_states.iter_mut())
            .map(|(probability, state)| {
                *state = xorshift64(*state);
                let threshold = (*probability).clamp(0, SCALE) as u128;
                let draw = ((*state as u128) * SCALE as u128) >> 64;
                u8::from(draw < threshold)
            })
            .collect::<Vec<_>>()
    });
    Ok((draws.into_pyarray(py), next_states.into_pyarray(py)))
}

fn xorshift64(mut state: u64) -> u64 {
    if state == 0 {
        state = 0x9E37_79B9_7F4A_7C15;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

#[allow(clippy::too_many_arguments)]
fn monte_carlo_fill_core(
    poisson_chunks: i64,
    poisson_threshold: u64,
    sizes: &[i64],
    order_size: i64,
    participation: i64,
    paths: i64,
    seed: u64,
    require_full: bool,
) -> Result<MonteCarloOutput, String> {
    if paths <= 0 {
        return Err("monte carlo paths must be positive".into());
    }
    if order_size < 0 || participation < 0 {
        return Err("monte carlo fixed-point values must be non-negative".into());
    }
    if poisson_chunks > 0 && sizes.is_empty() {
        return Err("monte carlo sizes cannot be empty when intensity is positive".into());
    }
    if sizes.iter().any(|value| *value < 0) {
        return Err("monte carlo sizes must be non-negative".into());
    }
    let mut state = seed;
    let mut fills = Vec::with_capacity(paths as usize);
    for _ in 0..paths {
        let mut arrivals = 0_i64;
        for _ in 0..poisson_chunks.max(0) {
            let mut product = u64::MAX;
            let mut count = 0_i64;
            while product > poisson_threshold {
                count = count.saturating_add(1);
                if count > 1_000_000 {
                    return Err("poisson sampler iteration limit exceeded".into());
                }
                state = xorshift64(state);
                product = (((product as u128) * (state as u128)) >> 64) as u64;
            }
            arrivals = arrivals.saturating_add(count.saturating_sub(1));
        }
        let mut generated = 0_i64;
        for _ in 0..arrivals {
            state = xorshift64(state);
            let index = (((state as u128) * (sizes.len() as u128)) >> 64) as usize;
            generated = generated.saturating_add(sizes[index.min(sizes.len() - 1)]);
        }
        let capacity = mul_scaled_round_half_up(generated, participation)?;
        let fill = if require_full {
            if capacity >= order_size {
                order_size
            } else {
                0
            }
        } else {
            order_size.min(capacity)
        };
        fills.push(fill);
    }
    fills.sort_unstable();
    let path_count = paths as i128;
    let total = fills.iter().map(|value| *value as i128).sum::<i128>();
    let expected_fill = i64::try_from((total + path_count / 2) / path_count)
        .map_err(|_| "monte carlo expected fill overflow")?;
    let positive = fills.iter().filter(|value| **value > 0).count() as i128;
    let full = fills.iter().filter(|value| **value >= order_size).count() as i128;
    let probability = |count: i128| -> Result<i64, String> {
        i64::try_from((count * SCALE as i128 + path_count / 2) / path_count)
            .map_err(|_| "monte carlo probability overflow".into())
    };
    let quantile = |numerator: usize, denominator: usize| -> i64 {
        let index = (fills.len().saturating_sub(1) * numerator) / denominator;
        fills[index]
    };
    Ok(MonteCarloOutput {
        expected_fill,
        fill_probability: probability(positive)?,
        full_fill_probability: probability(full)?,
        q10: quantile(1, 10),
        q50: quantile(1, 2),
        q90: quantile(9, 10),
        next_state: state,
    })
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn monte_carlo_fill_distribution<'py>(
    py: Python<'py>,
    poisson_chunks: i64,
    poisson_threshold: u64,
    sizes: PyReadonlyArray1<'py, i64>,
    order_size: i64,
    participation: i64,
    paths: i64,
    seed: u64,
    require_full: bool,
) -> PyResult<Py<PyAny>> {
    let sizes = sizes.as_slice()?.to_vec();
    let output = py
        .allow_threads(move || {
            monte_carlo_fill_core(
                poisson_chunks,
                poisson_threshold,
                &sizes,
                order_size,
                participation,
                paths,
                seed,
                require_full,
            )
        })
        .map_err(PyValueError::new_err)?;
    let result = PyDict::new(py);
    result.set_item("expected_fill", output.expected_fill)?;
    result.set_item("fill_probability", output.fill_probability)?;
    result.set_item("full_fill_probability", output.full_fill_probability)?;
    result.set_item("q10", output.q10)?;
    result.set_item("q50", output.q50)?;
    result.set_item("q90", output.q90)?;
    result.set_item("next_state", output.next_state)?;
    Ok(result.into_any().unbind())
}

pub fn kway_merge_core(
    offsets: &[i64],
    blocks: &[i64],
    tx_indexes: &[i64],
    log_indexes: &[i64],
) -> Result<Vec<i64>, String> {
    if offsets.len() < 2 || *offsets.first().unwrap_or(&0) != 0 {
        return Err("stream offsets must start at zero and include an end".into());
    }
    if blocks.len() != tx_indexes.len() || blocks.len() != log_indexes.len() {
        return Err("sequence array length mismatch".into());
    }
    if *offsets.last().unwrap() != blocks.len() as i64 {
        return Err("last stream offset must equal row count".into());
    }
    let mut heap: BinaryHeap<MergeHeapItem> = BinaryHeap::new();
    for stream in 0..offsets.len() - 1 {
        let index = offsets[stream] as usize;
        if index < offsets[stream + 1] as usize {
            heap.push(Reverse((
                blocks[index],
                tx_indexes[index],
                log_indexes[index],
                stream,
                index,
            )));
        }
    }
    let mut result = Vec::with_capacity(blocks.len());
    while let Some(Reverse((_, _, _, stream, index))) = heap.pop() {
        result.push(index as i64);
        let next = index + 1;
        if next < offsets[stream + 1] as usize {
            heap.push(Reverse((
                blocks[next],
                tx_indexes[next],
                log_indexes[next],
                stream,
                next,
            )));
        }
    }
    Ok(result)
}

#[pyfunction]
fn kway_merge_indices<'py>(
    py: Python<'py>,
    offsets: PyReadonlyArray1<'py, i64>,
    blocks: PyReadonlyArray1<'py, i64>,
    tx_indexes: PyReadonlyArray1<'py, i64>,
    log_indexes: PyReadonlyArray1<'py, i64>,
) -> PyResult<Bound<'py, PyArray1<i64>>> {
    let offsets = offsets.as_slice()?.to_vec();
    let blocks = blocks.as_slice()?.to_vec();
    let tx_indexes = tx_indexes.as_slice()?.to_vec();
    let log_indexes = log_indexes.as_slice()?.to_vec();
    let result = py
        .allow_threads(move || kway_merge_core(&offsets, &blocks, &tx_indexes, &log_indexes))
        .map_err(PyValueError::new_err)?;
    Ok(result.into_pyarray(py))
}

#[pymodule]
fn _fill_only_rust(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("SCALE", SCALE)?;
    module.add_class::<PreparedTradeTapeHandle>()?;
    module.add_class::<ReplaySessionHandle>()?;
    module.add_function(wrap_pyfunction!(match_taker_batch, module)?)?;
    module.add_function(wrap_pyfunction!(match_l2_snapshot_batch, module)?)?;
    module.add_function(wrap_pyfunction!(bernoulli_stateful, module)?)?;
    module.add_function(wrap_pyfunction!(monte_carlo_fill_distribution, module)?)?;
    module.add_function(wrap_pyfunction!(kway_merge_indices, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_point_rounds_half_up() {
        assert_eq!(
            mul_scaled_round_half_up(100 * SCALE, SCALE / 40).unwrap(),
            25_000_000_000
        );
    }

    #[test]
    fn kway_merge_preserves_sequence() {
        let merged = kway_merge_core(&[0, 2, 4], &[1, 3, 2, 4], &[0; 4], &[0; 4]).unwrap();
        assert_eq!(merged, vec![0, 2, 1, 3]);
    }

    #[test]
    fn monte_carlo_is_seed_deterministic_and_bounded() {
        let first = monte_carlo_fill_core(
            1,
            ((-1.0_f64).exp() * u64::MAX as f64) as u64,
            &[2 * SCALE, 3 * SCALE],
            10 * SCALE,
            SCALE / 40,
            100,
            73,
            false,
        )
        .unwrap();
        let second = monte_carlo_fill_core(
            1,
            ((-1.0_f64).exp() * u64::MAX as f64) as u64,
            &[2 * SCALE, 3 * SCALE],
            10 * SCALE,
            SCALE / 40,
            100,
            73,
            false,
        )
        .unwrap();
        assert_eq!(first, second);
        assert!((0..=10 * SCALE).contains(&first.expected_fill));
        assert!((0..=SCALE).contains(&first.fill_probability));
    }

    #[test]
    fn l2_snapshot_batch_preserves_fak_and_fok_semantics() {
        let fak = match_l2_snapshot_core(
            &[0, 2],
            &[50 * SCALE / 100, 51 * SCALE / 100],
            &[3 * SCALE, 4 * SCALE],
            &[0],
            &[51 * SCALE / 100],
            &[10 * SCALE],
            &[SCALE],
            &[0],
        )
        .unwrap();
        assert_eq!(fak.status, vec![1]);
        assert_eq!(fak.remaining, vec![3 * SCALE]);
        assert_eq!(fak.fill_quantity, vec![3 * SCALE, 4 * SCALE]);

        let fok = match_l2_snapshot_core(
            &[0, 2],
            &[50 * SCALE / 100, 51 * SCALE / 100],
            &[3 * SCALE, 4 * SCALE],
            &[0],
            &[51 * SCALE / 100],
            &[10 * SCALE],
            &[SCALE],
            &[1],
        )
        .unwrap();
        assert_eq!(fok.status, vec![0]);
        assert_eq!(fok.remaining, vec![10 * SCALE]);
        assert!(fok.fill_quantity.is_empty());
    }
}
