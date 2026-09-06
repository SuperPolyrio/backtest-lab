use _fill_only_rust::{match_taker_core, MatchInput, SCALE};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn input() -> MatchInput {
    let trades = 50_000;
    let orders = 100;
    MatchInput {
        trade_market: vec![1; trades],
        trade_asset: vec![1; trades],
        trade_side: (0..trades).map(|index| (index % 2) as u8).collect(),
        trade_block: (0..trades).map(|index| 100_000 + index as i64).collect(),
        trade_time_us: (0..trades).map(|index| index as i64 * 1_000_000).collect(),
        trade_price: vec![5_500_000_000; trades],
        trade_size: vec![100 * SCALE; trades],
        trade_consumed: vec![0; trades],
        order_market: vec![1; orders],
        order_asset: vec![1; orders],
        order_side: vec![0; orders],
        order_arrival_block: (0..orders).map(|index| 124_900 + index as i64).collect(),
        order_deadline_block: (0..orders).map(|index| 125_000 + index as i64).collect(),
        order_arrival_time_us: (0..orders)
            .map(|index| (24_900 + index as i64) * 1_000_000)
            .collect(),
        order_deadline_time_us: (0..orders)
            .map(|index| (24_930 + index as i64) * 1_000_000)
            .collect(),
        order_limit: vec![6_000_000_000; orders],
        order_size: vec![10 * SCALE; orders],
        order_fill_cap: vec![10 * SCALE; orders],
        order_rejected: vec![0; orders],
        order_participation: vec![250_000_000; orders],
        order_price_buffer: vec![50_000_000; orders],
        order_excluded_trade_index: vec![-1; orders],
        order_require_full: vec![0; orders],
        order_min_future_count: vec![0; orders],
        order_min_future_volume: vec![0; orders],
    }
}

fn benchmark(c: &mut Criterion) {
    let source = input();
    c.bench_function("match_taker_50k_100", |bencher| {
        bencher.iter(|| match_taker_core(black_box(source.clone())).unwrap())
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
