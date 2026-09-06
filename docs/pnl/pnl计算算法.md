昨天把 Polymarket PnL 算法拆了个 12 分钟的视频。

起因是研究 Polymarket 排行榜地址时发现，常见几种算法能把盈亏符号直接算反——用 /positions 接口偏浮亏、信 makerPnl 偏差 10 倍、按 txHash 去重删真实成交。

视频里讲 5 个最离谱的坑 + 最后跑通的现金流算法：
Σ(SELL+REDEEM+MERGE+REBATE) − Σ(BUY+SPLIT) + 持仓市值

跟官方 Leaderboard 交叉验证，Top 10 误差 < 1%，MAPE 0.2%。

排除 REWARD / REFERRAL / CONVERSION 这三类——平台外生收入不是交易 PnL。

精算脚本开源在 GitHub (https://github.com/runesleo/polymarket-toolkit)（MIT），polymarket-pnl skill 三步装好：

cp -R skills/polymarket-pnl ~/.claude/skills/ && pip install httpx

不用 API key，公开 Data API。

—
想自己扒头部地址的现金流数据 → Polymarket (https://polymarket.com/?r=xpmpnl&via=runes-leo&utm_source=tg&utm_content=pnl-video)