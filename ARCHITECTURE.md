# 🧠 Архитектура HFT Спредера (Mental Map v7.1 - Asymmetric LIMIT_IOC Execution)

> [!NOTE]
> **СТАТУС ТЕКУЩЕЙ АРХИТЕКТУРЫ: ASYMMETRIC_LIMIT_IOC (v7.1)**
> В версии **v7.1** система использует зрелый асимметричный протокол исполнения `LIMIT_IOC` со следующими рубежами защиты капитала:
> - **Пре-фильтр OBI (Order Book Imbalance):** Оценка дисбаланса топ-5 уровней стакана Binance до выстрела. Отсекает вход, если рынок сильной ноги летит против стороны хеджа.
> - **Фаза 1 (Lead Leg):** Лимитный прострел слабой биржи (Bitget/KuCoin) с контролем жесткого проскальзывания (`0.05%`). Нулевой налив $\to$ карантин 300с (Ветка Б).
> - **Фаза 2 (Validation & Soft Floor):** Проверка `min_notional_usd >= 6.0$` и жизнеспособности спреда по порогу `min_acceptable_net_spread >= 0.0010` (+10 bps суммарного чистого спреда). Если спред улетел в минус $\to$ 1-Shot Market Kill-Switch (Ветка А1) с фикс-карантином на 1 час (3600с).
> - **Фаза 3 (Hedge Leg):** Итеративный дожим сильной биржи (Binance) лимитными ордерами по `hedge_decay_map` (0.60 $\to$ 0.30 $\to$ 0.00) с точным номиналом монет (`exact_qty`) и безопасным квантованием цены (`ROUND_FLOOR` на BUY, `ROUND_CEILING` на SELL). Налив оценивается строго относительно объема первой ноги (`req_hedge_qty = lead_qty_actual`).
> - **Фаза 4 (Resolution):** Выравнивание ног. Если налив хеджа $\ge 75\%$, излишек первой ноги подрезается `MARKET reduce_only` с `exact_qty=delta_qty`. Иначе — полный аварийный развал.
> - **Фаза 5 (Exit & TTL):** Стандартный выход по относительной сетке `relative_profit_decay_map` (30с безубыток, 60с hard TTL). При слабом фактическом входе ($\le 0.0015$) — сжатая 20-секундная аварийная сетка `extreme_profit_decay_map` (0с 0.00, 10с -0.05%, 20с hard TTL).

---

## 🏗 Топология двух процессов (2-Process IPC Architecture)

Для обхода ограничений GIL (Global Interpreter Lock), изоляции вычислительного цикла котировок от сетевых задержек ордеров и предотвращения задержек цикла система разделена на два изолированных процесса, общающихся через неблокирующий локальный TCP-сокет (`CORE/ipc_socket.py`):

```
┌────────────────────────────────────────────────────────┐
│      ПРОЦЕСС 1: Market Data & Decision Engine          │
│                    (main.py)                           │
│  • Public WS Streams (Binance, KuCoin, Bitget, OKX)    │
│  • Numba JIT Orderbook Scan (pre_calculate_orderbook)   │
│  • VWAP & Spread Analysis (trading_engine.py)          │
│  • Order Book Imbalance (OBI) Filter (Hedge Book)      │
│  • Tightened Synthetic Slippage (ratio: 0.35, max: 0.8%)│
│  • Signal Dwell Time Filter (min_signal_dwell_ms)      │
│  • Global Symbol & Exchange Locks (position_manager)   │
│  • Profit Decay Monitoring:                            │
│      - relative_profit_decay_map (60с TTL, 30с 0.00)   │
│      - extreme_profit_decay_map (20с TTL сжатая)       │
└─────────────────────────┬──────────────────────────────┘
                          │ Local Async TCP Socket (IPC)
                          │ [CMD_OPEN, CMD_CLOSE, INIT_TOPOLOGY]
                          │ [POS_OPENED, POS_CLOSED, POS_FAILED]
┌─────────────────────────▼──────────────────────────────┐
│       ПРОЦЕСС 2: Execution & Settlement Worker         │
│               (CORE/executor_process.py)               │
│  • Pre-warmed Persistent TCP/TLS REST Sessions         │
│    (Keepalive Loop каждые 45с с фейк-ордерами warmup)  │
│  • Position FSM (Reactive Finite State Machine v7.1)   │
│  • Reactive Event Bus (asyncio.Event per symbol/side)  │
│    [Zero OS Timer Sleep Jitter: FSM wakeup < 0.1 ms]   │
│  • Real-Time Private WS Position Streams (KuCoin,      │
│    Binance, Bitget) с контрактными множителями         │
│  • Directional Safe Quantization:                      │
│      - BUY: ROUND_FLOOR (никогда не платить выше)      │
│      - SELL: ROUND_CEILING (не продавать ниже)         │
│      - QTY: ROUND_FLOOR + Epsilon 1e-8 (нет остатка)   │
│  • Exact Nominal Dispatch (exact_qty)                  │
│  • Phase 2 Soft Floor Check (min_acceptable_net_spread)│
│  • 1-Shot HFT Market Kill-Switch (emergency unwind)    │
│  • Controlled Trim (trim_excess_lead: MARKET reduce)   │
│  • Instant 0ms PnL Calculation (analytics.py)          │
│  • Automated Margin & Leverage Setup (leverage_setter) │
└────────────────────────────────────────────────────────┘
```

---

## 🎛 Компоненты системы

### 1. Market Data & Decision Engine (`main.py`)
- **Топология инструментов (`discovery.py`):** Запрашивает 24ч объемы торгов (фильтр от $2M–$5M), отсекает неликвид, строит нормализованный кросс-биржевой маппинг `coin_to_native` для активных связок (`BINANCE_KUCOIN`, `BINANCE_BITGET`).
- **Сбор стаканов:** Поддерживает пулы WebSocket-стримов стаканов глубины (L2 Depth).
- **Микросекундный пре-фильтр (`CORE/math_core.py`):** Каждую итерацию без задержки обновляет 4-колоночную матрицу цен и объемов `prices_array` (`[ask_p, ask_usd, bid_p, bid_usd]`) и прогоняет ее через `pre_calculate_orderbook` (`Numba @njit`), мгновенно отсекая тонкие уровни (`min_top_depth_usd: 200.0`) и сортируя связки по величине спреда без аллокаций памяти Python.
- **Оценка входа (`CORE/trading_engine.py`):** 
  - Рассчитывает взвешенные VWAP-цены входа с дисконтом глубины (`volatility_discount_entry: 0.40`), вычитает суммарные комиссии обеих бирж.
  - **Фильтр давления стакана (OBI):** Рассчитывает $\text{Imbalance} = (\sum \text{Bids}_{1..5} - \sum \text{Asks}_{1..5}) / (\sum \text{Bids}_{1..5} + \sum \text{Asks}_{1..5})$ стакана Hedge-биржи. Если при покупке на хедже стакан забит бидами ($> +0.45$), или при продаже забит асками ($< -0.45$), сигнал бракуется до выстрела.
  - **Ужесточенное синтетическое проскальзывание:** `max_slippage_ratio = 0.35`, `hard_max_slippage = 0.008` (0.8%).
- **Signal Dwell Time (Выдержка сигнала):** Опциональный фильтр устойчивости сигнала (`min_signal_dwell_ms`, 0 — выстрел на 1-м тике).
- **Мониторинг позиций и деградация профита:** 
  - На каждом тике рассчитывает суммарный арбитражный PnL связки (`net_yield`) по реальным бидам/аскам стаканов с учетом объемов и 4 комиссий (вход + выход обеих ног).
  - Поддерживает две карты деградации: `relative_profit_decay_map` (основная 60с) и `extreme_profit_decay_map` (аварийная 20с сжатая).

### 2. Execution & FSM Engine (`CORE/executor_process.py` + `CORE/position_fsm.py`)
- **Реактивная шина событий (Reactive Event Bus):**
  - Во всех приватных сокетах (`API/BINANCE/ws_private_binance.py`, `API/KUCOIN/ws_private_kucoin.py`, `API/BITGET/ws_private_bitget.py`) внедрены реестры `_update_events[(symbol, side)] = asyncio.Event()`.
  - При получении пуша сокет мгновенно дергает `_notify(symbol, side)`, пробуждая ожидающие корутины за $< 0.1$ мс без джиттера таймеров OS.
- **Безопасное квантование цен и объемов (`API/orders.py`):**
  - `BUY` $\to$ `ROUND_FLOOR` (гарантирует, что цена покупки лимитки хеджа никогда не превысит допустимый потолок спреда `limit_ceiling`).
  - `SELL` $\to$ `ROUND_CEILING` (гарантирует, что цена продажи лимитки хеджа никогда не опустится ниже расчетного пола `limit_floor`).
  - `QTY` $\to$ `ROUND_FLOOR` с защитой эпсилона `1e-8` (устраняет дельту от усечения `99.99999999999999`).
  - Поддержка точного номинала `exact_qty` во всех ордерах.
- **Фаза 2: Soft Floor & Min Notional:**
  - Проверка `min_notional_usd >= 6.0$` (гарантирует запас над лимитом Binance $5$).
  - Оценка жизнеспособности спреда через `evaluate_hedge_entry` с порогом `min_acceptable_net_spread = 0.0010` (+10 bps суммарного чистого спреда). Если спред ниже +10 bps $\to$ Ветка А1 (сброс ноги, 1 час карантина).
- **Фаза 3: Дожим Hedge Leg:**
  - Карта `decay_map`: iter 0 (`0.60`, уступка 40% спреда, 50мс), iter 1 (`0.30`, уступка 70%, 150мс), iter 2 (`0.00`, дожим по минимальному порогу спреда, 300мс).
  - Процент налива хеджа `min_hedge_fill_rate = 0.75` измеряется **строго относительно фактически налитого объема Lead-ноги** (`lead_qty_actual`).
- **Фаза 4: Управляемая подрезка (`trim_excess_lead`):**
  - При частичном наливе хеджа ($\ge 75\%$) излишек Lead-ноги аккуратно срезается MARKET-ордером `reduce_only` с точным номиналом `exact_qty=delta_qty`.
- **Экстремальный выход (Сжатый 20с Hard TTL):**
  - При факте входа $\le \text{min\_spread\_entry}$ ($0.0015$) активируется `extreme_profit_decay_map`: 0с — безубыток, 10с — уступка -0.05%, 20с — безусловный сброс по рынку (`target_val = -999.0`, `TTL_EXPIRED`).

### 3. Менеджер позиций (`CORE/position_manager.py`)
- **Инвариант биржи (`max_positions`):** Число активных и ожидающих (`pending`) позиций по каждой бирже строго ограничено конфигом (по умолчанию 1).
- **Инвариант символа:** Одна и та же монета не может одновременно торговаться более чем на одной связке.
- **Сохранение флагов состояния:** Сохраняет `use_extreme_decay`, `actual_gross_spread` и `actual_net_spread` в `active_positions.json`.

### 4. Аналитика и клиринг PnL (`analytics.py`)
- Фиксация реальных цен исполнения обеих ног (`entry_long_price`, `entry_short_price`, `close_long_price`, `close_short_price`).
- Учет полного цикла комиссий (Round-Trip Taker Fees: вход + выход по обеим ногам).
- Синхронная запись сделок в `total_balance.json` and `active_positions.json`.

---

## 🔄 Жизненный цикл сделки v7.1 (Asymmetric LIMIT_IOC)

```
[Стаканы L2 WS] ──> [pre_calculate_orderbook (Numba JIT)]
                               │ (Топ-кандидат: спред > spread_entry, объем >= 200$)
                               ▼
                    [trading_engine.evaluate_entry]
                               ├── Расчет VWAP с дисконтом волатильности 0.40
                               ├── Проверка OBI стакана Hedge-биржи (|Imbalance| <= 0.45)
                               └── Ужесточенная синтетическая ликвидность (slip <= 0.35*net, hard <= 0.8%)
                               ▼
                    [Signal Dwell Time Filter] (0 - мгновенный выстрел)
                               ▼
                    [PositionManager.can_enter] -> lock_for_entry
                               ▼
                    [main.py шлет CMD_OPEN через IPC]
                               │
                               ▼
                    [PositionFSM: run_open()]
                     ├── Фаза 1: Выстрел в Lead Leg (Слабая нога: Bitget/KuCoin)
                     │     └─ LIMIT_IOC с slippage 0.05%. Если налив 0 -> Карантин 300с (Ветка Б)
                     │
                     ├── Фаза 2: Валидация Lead Leg & Soft Floor
                     │     ├── Проверка minNotional >= 6.0$
                     │     ├── Проверка evaluate_hedge_entry (порог min_acceptable_net_spread >= 0.10%)
                     │     └── Если спред < 0.10% -> 1-Shot Market Kill-Switch (Ветка А1, бан 3600с)
                     │
                     ├── Фаза 3: Дожим сильной ноги (Hedge Leg: Binance)
                     │     ├── 3 итерации decay_map: 0.60 (50мс) -> 0.30 (150мс) -> 0.00 (300мс)
                     │     ├── Точный номинал exact_qty (без float-искажений)
                     │     ├── Безопасное квантование: ROUND_FLOOR на BUY, ROUND_CEILING на SELL
                     │     └── Расчет налива строго от Lead Leg (hedge_actual / lead_actual >= 75%)
                     │
                     ├── Фаза 4: Выравнивание дельты
                     │     ├── Если налив >= 75% и trim_excess_lead=true: MARKET reduce_only подрезка излишка Lead
                     │     └── Если налив < 75%: Полный аварийный развал обеих ног
                     │
                     └── Успешный вход -> Расчет actual_net_spread -> POS_OPENED
                               │
                               ▼
                    [main.py: мониторинг выхода]
                               ├── Нормальный вход: relative_profit_decay_map (30с 0.00, 60с TTL)
                               └── Слабый вход (<= 0.15%): extreme_profit_decay_map (10с -0.05%, 20с TTL)
                               ▼
                    [main.py шлет CMD_CLOSE через IPC]
                               │
                               ▼
                    [PositionFSM: run_close()]
                     ├── Параллельная отправка LIMIT_IOC ордеров закрытия (прогретый REST)
                     ├── Реактивное подтверждение по WS (_wait_for_close_confirmation)
                     ├── Аварийный сброс по рынку остатков при недоливе
                     ├── Мгновенный расчет PnL и Round-Trip комиссий
                     └── Шлет POS_CLOSED -> confirm_exit -> разблокировка биржи
```

---

## ⚙️ Сводка ключевых параметров конфигурации (`cfg.json`)

| Блок / Фаза | Параметр | Значение | Описание |
| :--- | :--- | :--- | :--- |
| **Режим входа** | `order_execution_type` | `"ASYMMETRIC_LIMIT_IOC"` | Асимметричный последовательный выстрел (Слабая нога $\to$ Валидация $\to$ Хедж $\to$ Подрезка) |
| **Сигнальные фильтры** | `spread_entry` | `0.008` (0.80%) | Минимальный требуемый чистый спред входа после вычета комиссий |
| | `min_spread_entry` | `0.0015` (0.15%) | Порог активации экстремальной карты деградации при слабом фактическом входе |
| | `check_obi_filter` | `true` | Фильтр давления стакана Binance перед выстрелом в Lead |
| | `max_adverse_imbalance` | `0.45` | Предел перекоса стакана Binance ($> 0.45$ против стороны хеджа $\to$ отмена) |
| | `obi_levels` | `5` | Глубина анализа стакана для OBI (топ-5 уровней) |
| | `max_slippage_ratio` | `0.35` (35%) | Предельная доля спреда на синтетический выход (ужесточено) |
| | `hard_max_slippage` | `0.008` (0.8%) | Жесткий потолок синтетического проскальзывания (ужесточено) |
| | `min_top_depth_usd` | `$200.0` | Фильтр глубины: минимальный объем (USD) на первом квалифицированном уровне стакана |
| | `max_desync_ms` | `BN_KU: 125, BN_BG: 200` (мс) | Допустимый рассинхрон получения стаканов между биржами |
| | `min_signal_dwell_ms` | `0` (мс) | Выдержка сигнала перед входом (0 — выстрел на первом тике) |
| **Роли связок** | `exchange_roles` | `{"lead": "...", "hedge": "BINANCE"}` | Маршрутизация ролей: Lead = неликвидная биржа, Hedge = ликвидный гигант (Binance) |
| **Фаза 1: Lead Leg** | `phase1_lead_leg.max_slippage_pct` | `0.0005` (0.05%) | Запас цены для одиночного LIMIT_IOC выстрела от лучшего аска/бида |
| | `phase1_lead_leg.fill_confirm_timeout_sec` | `0.600` (600 мс) | Таймаут ожидания налива слабой ноги по WS-событию / кэшу |
| | `phase1_lead_leg.quarantine_zero_fill_sec` | `300` (5 мин) | Карантин монеты при нулевом наливе Lead Leg (Ветка Б) |
| **Фаза 2: Валидация** | `phase2_lead_validation.min_notional_usd`| `$6.0` | Порог minNotional налитого объема Lead (гарантия запаса над лимитом Binance $5) |
| | `phase2_lead_validation.min_acceptable_net_spread` | `0.0010` (0.10%) | Порог жизнеспособности спреда (Soft Floor). Если $< 0.10\% \to$ Ветка А1 |
| | `phase2_lead_validation.max_model_drift_ratio` | `0.80` (80%) | Предел дрейфа расчетной цены (уход > 80% от спреда $\to$ откат ноги, Ветка А1) |
| | `phase2_lead_validation.quarantine_a1_step1_sec`| `3600` (1 час) | Карантин монеты при сбросе по Ветке А1 (фикса 1 час) |
| **Фаза 3: Hedge Leg** | `phase3_hedge_leg.decay_map` | `3 итерации (0.60 / 0.30 / 0.00)` | Адаптивный дожим хеджа: уступка 40% (50мс) $\to$ 70% (150мс) $\to$ порог (300мс) |
| | `phase3_hedge_leg.min_hedge_fill_rate` | `0.75` (75%) | Минимальный процент налива хеджа **относительно фактически налитого объема Lead-ноги** |
| | `phase3_hedge_leg.quarantine_hedge_failed_sec` | `1800` (30 мин) | Карантин связки при сбое хеджирования (< 75% налива) |
| **Фаза 4: Резолюция** | `phase4_resolution.trim_excess_lead` | `true` | Подрезка излишка Lead Leg ордером MARKET reduce_only при частичном наливе |
| **Фаза 5: Выход** | `relative_profit_decay_map` | `60 сек (0.80 -> 0.00 -> TTL)` | Сетка выхода: 30с безубыток, 45с -20%, 60с аварийный Hard TTL сброс (-999.0) |
| | `extreme_profit_decay_map` | `20 сек (0.00 -> -0.05% -> TTL)` | Сжатая аварийная сетка выхода при слабом входе: 0с безубыток, 10с -0.05%, 20с TTL (-999.0) |
| | `close_confirm_timeout_sec` | `1.800` (1.8 с) | Таймаут подтверждения закрытия позиции |
| **Инварианты рисков** | `max_positions` | `1` | Максимум 1 активная позиция на биржу |

---

## 🧪 Инструменты валидации и бенчмарки (`live_tests/`)

Для непрерывного тестирования боевой сетевой инфраструктуры, замера сквозных задержек и валидации приватных стримов созданы автономные тестовые скрипты:

1. **`live_tests/test_pair_hedge_binance_kucoin.py`**:
   - Полный боевой прогон связки Binance <-> Kucoin с реальным входом минимальным лотом и мгновенным закрытием.
   - **Фактические показатели на боевом VPS (Tokyo):**
     - RTT параллельной отправки REST-ордеров входа: **`29.47 мс`**.
     - Подтверждение закрытия Kucoin по топику `/contract/positionAll`: **`5.60 мс`**!
     - Полный цикл сделки (Round-Trip Hedge Open + Close): **`135.15 мс`**.

2. **`live_tests/test_pair_hedge_binance_bitget.py`**:
   - Полный боевой прогон связки Binance <-> Bitget.
   - **Фактические показатели на боевом VPS (Tokyo):**
     - RTT параллельной отправки REST-ордеров входа: **`26.15–28.88 мс`**.
     - Подтверждение закрытия позиции Bitget по сокету: **`62.87 мс`**.
     - Полный цикл сделки: **`191.73 мс`**.

3. **`live_tests/test_orderbook_desync.py`**:
   - Автономный бенчмарк эмпирического распределения рассинхрона стаканов (`diff_ms` / `max_desync_ms`) между биржами на боевых L2 WS-стримах.
   - Замеряет Mean, P50, P90, P95, P99 и процент сигналов, проходящих фильтр при разных порогах (от 50 до 200 мс).
