import sys
import re

def main():
    with open('CORE/position_fsm.py', 'r', encoding='utf-8') as f:
        content = f.read()

    new_run_open = """    async def run_open(self) -> bool:
        \"\"\"
        Запуск пайплайна открытия позиции (PARALLEL_LIMIT_IOC):
        IDLE -> SUBMITTING (Both Legs) -> VERIFYING_FILL -> ACTIVE_HEDGED / SINGLE_LEG_EXPOSURE / ABORTED
        \"\"\"
        self._set_state(PositionState.SUBMITTING)
        
        entry_cfg = self.cfg["trading_rules"]["entry"]
        roles_cfg = entry_cfg["exchange_roles"].get(self.route)
        if not roles_cfg:
            log(f"[{self.sym}] ⛔ Нет ролей для связки {self.route}.", level="WARNING")
            self._set_state(PositionState.IDLE)
            self._notify_pos_failed("NO_ROLES")
            return False
            
        lead_ex = roles_cfg["lead"]
        hedge_ex = roles_cfg["hedge"]
        
        phase1_cfg = entry_cfg.get("phase1_lead_leg", entry_cfg)
        quarantine_cfg = entry_cfg.get("quarantine_durations_sec", {})
        
        is_lead_long = (lead_ex == self.long_ex)
        native_lead = self.native_long if is_lead_long else self.native_short
        native_hedge = self.native_short if is_lead_long else self.native_long
        
        lead_side = "BUY" if is_lead_long else "SELL"
        hedge_side = "SELL" if is_lead_long else "BUY"
        
        lead_pos_side = "LONG" if is_lead_long else "SHORT"
        hedge_pos_side = "SHORT" if is_lead_long else "LONG"
        
        spread_val = self.engine_res.get("net_spread", self.engine_res.get("vwap_spread", 0.0))
        log(f"[{self.sym}] Открываем (PARALLEL_LIMIT_IOC): {self.long_ex} (L) / {self.short_ex} (S) | Net Spread: {spread_val * 100:.2f}%", level="INFO")
        
        size_long_usd = float(self.cfg["trading_risks"][self.long_ex.lower()]["trade_size_usd"])
        size_short_usd = float(self.cfg["trading_risks"][self.short_ex.lower()]["trade_size_usd"])
        
        # Получаем расчетные цены (текущие лучшие цены или VWAP в зависимости от логики движка)
        price_long_calc = self.engine_res.get("long_avg_price", 0.0)
        price_short_calc = self.engine_res.get("short_avg_price", 0.0)
        
        # Используем лимиты проскальзывания из конфига (пока берем те же что были для Lead)
        max_slip = float(phase1_cfg.get("max_slippage_pct", entry_cfg.get("lead_max_slippage_pct", 0.0005)))
        
        price_long_limit = price_long_calc * (1 + max_slip)
        price_short_limit = price_short_calc * (1 - max_slip)
        
        ev_long = None
        ev_short = None
        
        if self.long_ex in self.orders and hasattr(self.orders[self.long_ex], "subscribe_position_update"):
            ev_long = self.orders[self.long_ex].subscribe_position_update(self.native_long, "LONG")
        if self.short_ex in self.orders and hasattr(self.orders[self.short_ex], "subscribe_position_update"):
            ev_short = self.orders[self.short_ex].subscribe_position_update(self.native_short, "SHORT")
            
        req_long_qty = self.engine_res.get("long_qty", 0.0)
        req_short_qty = self.engine_res.get("short_qty", 0.0)
        
        log(f"[{self.sym}] Phase 1: Sending PARALLEL LIMIT_IOC | L: {price_long_limit:.6f}, S: {price_short_limit:.6f}", level="INFO")
        
        tasks = []
        tasks.append(self.orders[self.long_ex].place_order(
            self.native_long, "BUY", size_long_usd, price_long_limit, order_type="LIMIT_IOC", position_side="LONG"
        ))
        tasks.append(self.orders[self.short_ex].place_order(
            self.native_short, "SELL", size_short_usd, price_short_limit, order_type="LIMIT_IOC", position_side="SHORT"
        ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, res in enumerate(results):
            ex_name = self.long_ex if i == 0 else self.short_ex
            if isinstance(res, Exception):
                log(f"[{self.sym}] 🚨 Ошибка входа ({ex_name}): {res}.", level="ERROR")
                self.ban_coin_cb(self.sym, reason=str(res), duration_sec=3600)
        
        self._set_state(PositionState.VERIFYING_FILL)
        
        l_pos, s_pos, l_rate, s_rate = await self._wait_for_fill_confirmation(
            req_long_qty, req_short_qty, ev_long=ev_long, ev_short=ev_short
        )
        
        if hasattr(self.orders[self.long_ex], "unsubscribe_position_update"):
            self.orders[self.long_ex].unsubscribe_position_update(self.native_long, "LONG")
        if hasattr(self.orders[self.short_ex], "unsubscribe_position_update"):
            self.orders[self.short_ex].unsubscribe_position_update(self.native_short, "SHORT")
            
        l_qty = l_pos.get("size", 0.0)
        s_qty = s_pos.get("size", 0.0)
        l_price = l_pos.get("price", 0.0)
        s_price = s_pos.get("price", 0.0)
        
        log(f"[{self.sym}] Phase 2: Результаты налива -> LONG: {l_qty:.4f} ({l_rate*100:.1f}%), SHORT: {s_qty:.4f} ({s_rate*100:.1f}%)", level="INFO")
        
        if l_qty <= 0.0 and s_qty <= 0.0:
            zero_fill_sec = float(phase1_cfg.get("quarantine_zero_fill_sec", quarantine_cfg.get("zero_fill", 300)))
            log(f"[{self.sym}] Zero Fill: Ни одна нога не налилась. Карантин {zero_fill_sec:.0f}с.", level="WARNING")
            self.ban_coin_cb(self.sym, reason="Zero Fill (Both Legs)", duration_sec=zero_fill_sec)
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("ZERO_FILL")
            return False
            
        # Check partial vs full hedge
        # If both are somewhat filled, check if they are balanced enough
        min_hedge_rate = float(entry_cfg.get("phase3_hedge_leg", {}).get("min_hedge_fill_rate", 0.75))
        
        if l_qty > 0 and s_qty > 0:
            l_notional = l_qty * l_price
            s_notional = s_qty * s_price
            ratio = min(l_notional, s_notional) / max(l_notional, s_notional)
            if ratio >= min_hedge_rate:
                log(f"[{self.sym}] Обе ноги успешно налиты. Сбалансированность {ratio*100:.1f}%. Переход в ACTIVE_HEDGED.", level="INFO")
                # TODO: trim excess if needed (for now just finalize)
                self._finalize_open(l_qty, s_qty, l_price, s_price)
                return True
        
        # SINGLE LEG EXPOSURE
        log(f"[{self.sym}] Переход к статической арбитражной обработке одной зависшей ноги (SINGLE_LEG_EXPOSURE).", level="WARNING")
        self._set_state(PositionState.SINGLE_LEG_EXPOSURE)
        await self._run_single_leg_exposure(l_qty, s_qty, l_price, s_price)
        return False

    async def _run_single_leg_exposure(self, l_qty: float, s_qty: float, l_price: float, s_price: float):
        \"\"\"
        Чейзинг стакана для выхода из зависшей ноги.
        \"\"\"
        # Определяем зависшую ногу
        if l_qty > 0 and s_qty <= 0.0:
            open_ex = self.long_ex
            native_sym = self.native_long
            qty_to_close = l_qty
            entry_price = l_price
            close_side = "SELL"
            pos_side = "LONG"
            engine_price_key = "long_avg_price" 
            ev_leg = self.orders[open_ex].subscribe_position_update(native_sym, "LONG") if hasattr(self.orders[open_ex], "subscribe_position_update") else None
        elif s_qty > 0 and l_qty <= 0.0:
            open_ex = self.short_ex
            native_sym = self.native_short
            qty_to_close = s_qty
            entry_price = s_price
            close_side = "BUY"
            pos_side = "SHORT"
            engine_price_key = "short_avg_price"
            ev_leg = self.orders[open_ex].subscribe_position_update(native_sym, "SHORT") if hasattr(self.orders[open_ex], "subscribe_position_update") else None
        else:
            # Аномалия, сбрасываем обе если нужно
            if l_qty > 0:
                await self._emergency_unwind_single(self.long_ex, self.native_long, l_qty, l_price, "BUY", "LONG")
            if s_qty > 0:
                await self._emergency_unwind_single(self.short_ex, self.native_short, s_qty, s_price, "SELL", "SHORT")
            self._set_state(PositionState.ABORTED)
            return
            
        exit_cfg = self.cfg["trading_rules"]["exit"]
        decay_map = exit_cfg.get("single_leg_exit_map", [])
        
        qty_rem = qty_to_close
        
        start_time = time.time()
        for step in decay_map:
            if qty_rem <= 0:
                break
            
            target_val = float(step.get("target_val", 0.0))
            wait_sec = float(step.get("seconds", 0.0))
            
            now = time.time()
            elapsed = now - start_time
            if elapsed < wait_sec:
                await asyncio.sleep(wait_sec - elapsed)
                
            if target_val <= -900.0:
                # Market fallback
                log(f"[{self.sym}] Single Leg Fallback: MARKET выход ({open_ex}).", level="WARNING")
                await self._emergency_unwind_single(open_ex, native_sym, qty_rem, entry_price, "BUY" if close_side=="SELL" else "SELL", pos_side)
                break
                
            # Пробуем лимитку
            # Для чейзинга нужно взять лучшую цену стакана и ухудшить ее на target_val
            # Чтобы не парсить стакан заново, используем последнюю цену из движка (он обновляется в фоне)
            current_calc_price = self.engine_res.get(engine_price_key, entry_price)
            
            if close_side == "SELL":
                # Ухудшаем цену вниз
                limit_price = current_calc_price * (1 + target_val)
            else:
                # Ухудшаем цену вверх
                limit_price = current_calc_price * (1 - target_val)
                
            usd_needed = qty_rem * limit_price
            log(f"[{self.sym}] Single Leg Chasing (Iter {step.get('step')}): {close_side} {qty_rem:.4f} @ {limit_price:.6f} (Target: {target_val})", level="INFO")
            
            try:
                await self.orders[open_ex].place_order(
                    native_sym, close_side, usd_needed, limit_price, order_type="LIMIT_IOC", position_side=pos_side, exact_qty=qty_rem, reduce_only=True
                )
            except Exception as e:
                log(f"[{self.sym}] Ошибка чейзинга {open_ex}: {e}", level="ERROR")
                continue
                
            await asyncio.sleep(0.5) # Ждем налив лимитки
            
            # Проверяем позицию
            pos = await self.orders[open_ex].get_position_rest(native_sym, pos_side)
            qty_rem = pos.get("size", 0.0)
            
        if ev_leg and hasattr(self.orders[open_ex], "unsubscribe_position_update"):
            self.orders[open_ex].unsubscribe_position_update(native_sym, pos_side)
            
        self.ban_coin_cb(self.sym, reason="Single Leg Exposure Exit", duration_sec=3600)
        self._set_state(PositionState.ABORTED)
        self._notify_pos_failed("SINGLE_LEG_EXPOSURE")"""

    import re
    # We will replace run_open method
    start_str = "    async def run_open(self) -> bool:"
    end_str = "    async def _emergency_unwind_single("
    
    start_idx = content.find(start_str)
    end_idx = content.find(end_str)
    
    if start_idx != -1 and end_idx != -1:
        new_content = content[:start_idx] + new_run_open + "\n\n" + content[end_idx:]
        with open('CORE/position_fsm.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Success")
    else:
        print("Could not find boundaries")

if __name__ == '__main__':
    main()
