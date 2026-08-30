# ============================================================
# FILE: analytics.py
# ROLE: Сбор и анализ данных о торговле, расчет PnL (мульти-биржа).
# ============================================================
import os
import time
import pandas as pd
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

    def record_open(self, route: str, direction: str, long_ex: str, short_ex: str, long_price: float, short_price: float, spread: float, slippage: float = 0.0) -> None:
        self.active_trade = {
            "route": route,
            "direction": direction,
            "long_ex": long_ex,
            "short_ex": short_ex,
            "open_time": time.time(),
            "long_price_in": long_price,
            "short_price_in": short_price,
            "spread_in": spread,
            "slippage": slippage
        }

    def record_close(self, long_price_close: float, short_price_close: float, spread_out: float, slippage_out: float = 0.0, long_executed_usd: float = None, short_executed_usd: float = None) -> None:
        if not self.active_trade:
            return
            
        t_in = self.active_trade
        close_time = time.time()
        
        duration_sec = int(close_time - t_in["open_time"])
        mins, secs = divmod(duration_sec, 60)
        duration_str = f"{mins} min {secs} sec"
        
        dt_open = datetime.fromtimestamp(t_in["open_time"], self.tz).strftime('%Y-%m-%d %H:%M:%S')
        dt_close = datetime.fromtimestamp(close_time, self.tz).strftime('%Y-%m-%d %H:%M:%S')
        
        long_ex = t_in["long_ex"]
        short_ex = t_in["short_ex"]
        
        long_pnl = (long_price_close - t_in["long_price_in"]) / t_in["long_price_in"] if t_in["long_price_in"] else 0.0
        short_pnl = (t_in["short_price_in"] - short_price_close) / t_in["short_price_in"] if t_in["short_price_in"] else 0.0
        
        gross_pnl = (long_pnl + short_pnl) / 2.0
        
        long_cfg = self.risks_cfg.get(long_ex.lower(), {})
        short_cfg = self.risks_cfg.get(short_ex.lower(), {})
        
        actual_long_usd = long_executed_usd if long_executed_usd is not None else long_cfg.get("trade_size_usd", 100.0)
        actual_short_usd = short_executed_usd if short_executed_usd is not None else short_cfg.get("trade_size_usd", 100.0)
        
        l_fee_usd = actual_long_usd * (long_cfg.get("taker_fee", 0.0006) * 2.0)
        s_fee_usd = actual_short_usd * (short_cfg.get("taker_fee", 0.0006) * 2.0)
        
        long_pnl_usd = actual_long_usd * long_pnl - l_fee_usd
        short_pnl_usd = actual_short_usd * short_pnl - s_fee_usd
        
        net_pnl_usd = long_pnl_usd + short_pnl_usd
        total_investment = actual_long_usd + actual_short_usd
        net_pnl = net_pnl_usd / total_investment if total_investment > 0 else 0
        
        self.cumulative_pnl_usd += net_pnl_usd
        self.trade_counter += 1
        
        total_slip = t_in.get("slippage", 0.0) + slippage_out
        
        trade_obj = {
            "Trade_ID": self.trade_counter,
            "Route": t_in['route'],
            "Direction": t_in['direction'],
            "Long_Ex": long_ex,
            "Short_Ex": short_ex,
            "Open_Time": dt_open,
            "Close_Time": dt_close,
            "Duration": duration_str,
            "Long_Price_In": round(t_in["long_price_in"], 5),
            "Short_Price_In": round(t_in["short_price_in"], 5),
            "Long_Price_Out": round(long_price_close, 5),
            "Short_Price_Out": round(short_price_close, 5),
            "Long_PnL": round(long_pnl, 5),
            "Short_PnL": round(short_pnl, 5),
            "Long_PnL_USD": round(long_pnl_usd, 4),
            "Short_PnL_USD": round(short_pnl_usd, 4),
            "Long_Fee_USD": round(l_fee_usd, 4),
            "Short_Fee_USD": round(s_fee_usd, 4),
            "Slippage": round(total_slip, 5),
            "Gross_PnL": round(gross_pnl, 5),
            "Net_PnL": round(net_pnl, 5),
            "Net_PnL_USD": round(net_pnl_usd, 4),
            "Win": 1 if net_pnl_usd > 0 else -1,
            "Spread_In": round(t_in['spread_in'], 4),
            "Spread_Out": round(spread_out, 4),
            "Cumulative_PnL_USD": round(self.cumulative_pnl_usd, 4)
        }
        
        readable = (
            f"=========================================\n"
            f"Сделка #{self.trade_counter} ({t_in['direction']})\n"
            f"Связка: {t_in['route']}\n"
            f"Время: {dt_open} -> {dt_close} ({duration_str})\n"
            f"Вход | {long_ex}: {t_in['long_price_in']:.5f} | {short_ex}: {t_in['short_price_in']:.5f} | Спред: {t_in['spread_in']:.4f}\n"
            f"Выход| {long_ex}: {long_price_close:.5f} | {short_ex}: {short_price_close:.5f} | Спред: {spread_out:.4f}\n"
            f"PnL USD: {long_ex} {long_pnl_usd:.4f}$ | {short_ex} {short_pnl_usd:.4f}$\n"
            f"P&L: {net_pnl_usd:.4f} USD ({net_pnl:.5f})\n"
            f"Cumulative PnL: {self.cumulative_pnl_usd:.4f}$\n"
            f"=========================================\n\n"
        )
        
        try:
            def _io_tasks():
                try:
                    with open(self.filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except:
                    data = []
                    
                data.append(trade_obj)
                with open(self.filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                    
                with open(self.readable_path, "a", encoding="utf-8") as f:
                    f.write(readable)
                    
                log(f"[ANALYTICS] Сделка #{self.trade_counter} ({t_in['direction']}) закрыта. Net PnL: {net_pnl_usd:.4f} USD.", level="DEBUG", is_signal=True)

            _analytics_executor.submit(_io_tasks)
        except Exception as e:
            log(f"Error submitting IO tasks in analytics: {e}", level="ERROR")
            
        self.active_trade = {} 


def generate_global_report(log_dir: str = "logs/analytics"):
    if not os.path.exists(log_dir):
        print(f"Директория {log_dir} не найдена.")
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
        print("Нет данных для глобального отчета (все файлы пусты или отсутствуют).")
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
        
    print(f"Глобальный отчет сохранен в {report_path}")
    
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
    print(f"Глобальный график сохранен в {chart_path}")

if __name__ == "__main__":
    import sys
    try:
        print("Запуск генерации глобального отчета...")
        generate_global_report()
    except Exception as e:
        print(f"Ошибка генерации отчета: {e}")
        sys.exit(1)
