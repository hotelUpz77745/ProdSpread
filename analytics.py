# ============================================================
# FILE: analytics.py
# ROLE: Trade data collection, analytics and multi-exchange PnL calculation.
# ============================================================
import os
import time
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timezone
import pytz
from c_log import log
import json
import concurrent.futures

_analytics_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

from typing import Dict, Any, List

from consts import TIME_ZONE

class TradeAnalytics:
    def __init__(self, symbol: str, risks_cfg: Dict[str, Any]) -> None:
        self.symbol = symbol
        self.risks_cfg = risks_cfg
        
        self.tz = pytz.timezone(TIME_ZONE)
        self.log_dir = os.path.join("logs", "analytics")
        os.makedirs(self.log_dir, exist_ok=True)
        self.filepath = os.path.join(self.log_dir, f"{self.symbol}.json")
        self.readable_path = os.path.join(self.log_dir, f"{self.symbol}_history.txt")
        
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump([], f)
                
        self.trade_counter = self._get_trade_count()
        
        self.cumulative_pnl_usd = 0.0
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data:
                    self.cumulative_pnl_usd = data[-1].get("Cumulative_PnL_USD", 0.0)
        except Exception:
            pass

        self.active_trade = {}

    def _get_trade_count(self) -> int:
        if not os.path.exists(self.filepath): return 0
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                return len(json.load(f))
        except:
            return 0

    def record_open(self, route: str, direction: str, target_ex: str, oracle_ex: str, target_price_in: float, spread_in: float, slippage_in: float = 0.0) -> None:
        self.active_trade = {
            "route": route,
            "direction": direction,
            "target_ex": target_ex,
            "oracle_ex": oracle_ex,
            "open_time": time.time(),
            "target_price_in": target_price_in,
            "spread_in": spread_in,
            "slippage": slippage_in
        }

    def record_close(self, target_price_close: float, spread_out: float, slippage_out: float = 0.0, target_executed_usd: float = None) -> Dict[str, Any]:
        if not self.active_trade:
            # Fallback for positions closed after bot restart
            self.active_trade = {
                "route": "UNKNOWN",
                "direction": "LONG",
                "target_ex": "BITGET",
                "oracle_ex": "BINANCE",
                "open_time": time.time() - 60,
                "target_price_in": target_price_close,
                "spread_in": 0.0,
                "slippage": 0.0
            }
            
        t_in = self.active_trade
        close_time = time.time()
        
        duration_sec = int(close_time - t_in["open_time"])
        mins, secs = divmod(duration_sec, 60)
        duration_str = f"{mins} min {secs} sec"
        
        dt_open = datetime.fromtimestamp(t_in["open_time"], self.tz).strftime('%Y-%m-%d %H:%M:%S')
        dt_close = datetime.fromtimestamp(close_time, self.tz).strftime('%Y-%m-%d %H:%M:%S')
        
        target_ex = t_in["target_ex"]
        oracle_ex = t_in["oracle_ex"]
        direction = t_in["direction"]
        
        target_price_in = t_in["target_price_in"]
        
        if target_price_in <= 0 or target_price_close <= 0:
            target_pnl = 0.0
        elif direction == "LONG":
            target_pnl = (target_price_close - target_price_in) / target_price_in
        else:
            target_pnl = (target_price_in - target_price_close) / target_price_in
            
        if target_ex.upper() in self.risks_cfg and "trading_risks" in self.risks_cfg[target_ex.upper()]:
            target_cfg = self.risks_cfg[target_ex.upper()]["trading_risks"]
        elif target_ex.lower() in self.risks_cfg:
            target_cfg = self.risks_cfg[target_ex.lower()]
        else:
            target_cfg = self.risks_cfg[target_ex.upper()]
            
        default_trade_size = float(target_cfg["trade_size_usd"])
        taker_fee_rate = float(target_cfg["taker_fee"])
        
        actual_target_usd = target_executed_usd if target_executed_usd is not None and target_executed_usd > 0 else default_trade_size
        
        if target_price_close <= 0:
            t_fee_usd = 0.0
            target_pnl_usd = 0.0
            net_pnl_usd = 0.0
            net_pnl = 0.0
            total_investment = actual_target_usd
        else:
            t_fee_usd = actual_target_usd * (taker_fee_rate * 2.0)
            target_pnl_usd = actual_target_usd * target_pnl - t_fee_usd
            net_pnl_usd = target_pnl_usd
            total_investment = actual_target_usd
            net_pnl = net_pnl_usd / total_investment if total_investment > 0 else 0
        
        self.cumulative_pnl_usd += net_pnl_usd
        self.trade_counter += 1
        
        total_slip = t_in.get("slippage", 0.0) + slippage_out
        
        trade_obj = {
            "Trade_ID": self.trade_counter,
            "Route": t_in['route'],
            "Direction": direction,
            "Target_Ex": target_ex,
            "Oracle_Ex": oracle_ex,
            "Open_Time": dt_open,
            "Close_Time": dt_close,
            "Duration": duration_str,
            "Target_Price_In": round(target_price_in, 6),
            "Target_Price_Out": round(target_price_close, 6),
            "Target_PnL": round(target_pnl, 5),
            "Target_PnL_USD": round(target_pnl_usd, 4),
            "Total_Fee_USD": round(t_fee_usd, 4),
            "Slippage": round(total_slip, 5),
            "Net_PnL": round(net_pnl, 5),
            "Net_PnL_USD": round(net_pnl_usd, 4),
            "Win": 1 if net_pnl_usd >= 0 else -1,
            "Spread_In": round(t_in.get('spread_in', 0.0), 4),
            "Spread_Out": round(spread_out, 4),
            "Cumulative_PnL_USD": round(self.cumulative_pnl_usd, 4)
        }
        
        readable = (
            f"=========================================\n"
            f"Trade #{self.trade_counter} ({direction})\n"
            f"Route: {t_in['route']} (Oracle: {oracle_ex}, Target: {target_ex})\n"
            f"Time: {dt_open} -> {dt_close} ({duration_str})\n"
            f"Entry | {target_ex}: {target_price_in:.6f} | Spread: {trade_obj['Spread_In']:.4f}\n"
            f"Exit  | {target_ex}: {target_price_close:.6f} | Spread: {spread_out:.4f}\n"
            f"PnL USD: {target_ex} {target_pnl_usd:+.4f}$\n"
            f"Fees: {trade_obj['Total_Fee_USD']:.4f} USD\n"
            f"P&L: {net_pnl_usd:+.4f} USD ({net_pnl*100:+.3f}%)\n"
            f"Cumulative PnL: {self.cumulative_pnl_usd:+.4f}$\n"
            f"=========================================\n\n"
        )
        
        try:
            def _io_tasks():
                try:
                    with open(self.filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = []
                    
                data.append(trade_obj)
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                    
                with open(self.readable_path, "a", encoding="utf-8") as f:
                    f.write(readable)
                    
                log(f"[ANALYTICS] [{self.symbol}] Trade #{self.trade_counter} ({t_in['route']}) saved. Net PnL: {net_pnl_usd:+.4f} USD.", level="INFO")

            _analytics_executor.submit(_io_tasks)
        except Exception as e:
            log(f"Error submitting IO tasks in analytics: {e}", level="ERROR")
            
        self.active_trade = {}
        return trade_obj


_cached_base_total = None
_cached_cumulative_pnl = 0.0
_balance_initialized = False

def _recalc_total_pnl_from_disk() -> float:
    log_dir = os.path.join("logs", "analytics")
    history_pnl = 0.0
    if os.path.exists(log_dir):
        for fname in os.listdir(log_dir):
            if fname.endswith(".json") and fname != "global_report.json":
                fpath = os.path.join(log_dir, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        trades = json.load(f)
                        if trades and isinstance(trades, list):
                            history_pnl += sum(float(t.get("Net_PnL_USD", 0.0)) for t in trades)
                except Exception:
                    pass
    return history_pnl

def update_total_balance(cfg: dict, is_startup: bool = False, extra_pnl: float = 0.0) -> float:
    """
    Ultra-fast O(1) balance calculation from in-memory cache.
    Async flush of total_balance.json and reports without blocking trading loop.
    """
    global _cached_base_total, _cached_cumulative_pnl, _balance_initialized
    try:
        if "exchanges" in cfg:
            exchanges_dict = cfg["exchanges"]
            if _cached_base_total is None or is_startup:
                _cached_base_total = sum(
                    float(ex_cfg["trading_risks"]["paper_start_balance"])
                    for ex_cfg in exchanges_dict.values()
                    if "trading_risks" in ex_cfg and "paper_start_balance" in ex_cfg["trading_risks"]
                )
        elif "trading_risks" in cfg:
            risks = cfg["trading_risks"]
            if _cached_base_total is None or is_startup:
                _cached_base_total = sum(float(risk["paper_start_balance"]) for risk in risks.values())
        else:
            _cached_base_total = 0.0

        if not _balance_initialized or is_startup:
            _cached_cumulative_pnl = _recalc_total_pnl_from_disk()
            _balance_initialized = True
        
        if extra_pnl != 0.0:
            _cached_cumulative_pnl += extra_pnl

        base_total = _cached_base_total
        total_pnl = _cached_cumulative_pnl
        total = base_total + total_pnl
        now = time.time()

        prefix = "Initial Total balance" if is_startup else "Total balance updated"
        log(f"{prefix}: {total:.2f} USD (Base: {base_total:.2f}, PnL: {total_pnl:.2f})", level="INFO")

        # Non-blocking disk flush in background threadpool
        def _write_balance_task():
            try:
                payload = {
                    "timestamp": now,
                    "total_balance_usd": round(total, 4),
                    "base_total_usd": round(base_total, 4),
                    "total_pnl_usd": round(total_pnl, 4)
                }
                with open("total_balance.json", "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=4)
                generate_global_report()
            except Exception as io_err:
                log(f"Error in background balance writing: {io_err}", level="WARNING")

        _analytics_executor.submit(_write_balance_task)
        return total
    except Exception as e:
        log(f"Error updating total balance: {e}", level="ERROR")
        return 0.0


def generate_global_report(log_dir: str = "logs/analytics"):
    if not os.path.exists(log_dir):
        print(f"Directory {log_dir} not found.")
        return

    all_trades = []
    
    for filename in os.listdir(log_dir):
        if filename.endswith(".json") and filename != "global_report.json":
            filepath = os.path.join(log_dir, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if data:
                        for trade in data:
                            trade["Symbol"] = filename.replace(".json", "")
                        all_trades.extend(data)
            except Exception:
                pass

    if not all_trades:
        print("No data for global report (files empty or missing).")
        return

    df = pd.DataFrame(all_trades)
    
    df['Close_Time_DT'] = pd.to_datetime(df['Close_Time'])
    df = df.sort_values(by='Close_Time_DT').reset_index(drop=True)
    
    total_trades = len(df)
    wins = len(df[df['Win'] == 1])
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
    
    net_usdt = df['Net_PnL_USD'].sum()
    df['Cumulative_PnL'] = df['Net_PnL_USD'].cumsum()
    
    best_trade = df.loc[df['Net_PnL_USD'].idxmax()]
    worst_trade = df.loc[df['Net_PnL_USD'].idxmin()]
    
    def parse_dur(d_str):
        try:
            parts = d_str.split()
            return int(parts[0]) * 60 + int(parts[2])
        except:
            return 0
            
    avg_sec = df['Duration'].apply(parse_dur).mean()
    avg_m, avg_s = divmod(int(avg_sec), 60)

    # Route Statistics
    route_stats = df.groupby('Route').agg(
        Total_Trades=('Route', 'count'),
        Net_PnL_USD=('Net_PnL_USD', 'sum')
    ).sort_values(by='Net_PnL_USD', ascending=False)
    
    top_routes = route_stats.head(3)
    worst_routes = route_stats.tail(3)

    report = (
        f"=== GLOBAL REPORT (ALL COINS) ===\n"
        f"Total Trades : {total_trades}\n"
        f"Win Rate     : {win_rate:.2f}%\n"
        f"Net Profit   : {net_usdt:.2f} USD\n"
        f"Best Trade   : {best_trade['Symbol']} [{best_trade['Route']}] | NetPnL: {best_trade['Net_PnL_USD']:.2f} USD\n"
        f"Worst Trade  : {worst_trade['Symbol']} [{worst_trade['Route']}] | NetPnL: {worst_trade['Net_PnL_USD']:.2f} USD\n"
        f"Avg Duration : {avg_m} min {avg_s} sec\n\n"
        f"--- TOP 3 ROUTES ---\n"
    )
    for route, row in top_routes.iterrows():
        report += f"{route}: {row['Net_PnL_USD']:.2f} USD ({row['Total_Trades']} trades)\n"
        
    report += f"\n--- WORST 3 ROUTES ---\n"
    for route, row in worst_routes.iterrows():
        report += f"{route}: {row['Net_PnL_USD']:.2f} USD ({row['Total_Trades']} trades)\n"
        
    report += f"=================================\n"
    
    print(report)
    
    report_path = os.path.join(log_dir, "global_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
        
    print(f"Global report saved to {report_path}")
    
    # Chart
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df['Cumulative_PnL'], marker='.', linestyle='-', color='g')
    plt.axhline(0, color='gray', linestyle='--', linewidth=1)
    plt.title("Global Equity Curve (Cumulative Net PnL USD)")
    plt.xlabel("Total Trades (Chronological)")
    plt.ylabel("Cumulative Profit (USD)")
    plt.grid(True)
    
    chart_path = os.path.join(log_dir, "global_chart.png")
    plt.savefig(chart_path)
    plt.close()
    print(f"Global chart saved to {chart_path}")

if __name__ == "__main__":
    import sys
    try:
        print("Generating global report...")
        generate_global_report()
    except Exception as e:
        print(f"Report generation error: {e}")
        sys.exit(1)
