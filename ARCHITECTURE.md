# 🧠 Архитектура HFT Спредера (Mental Map v8.0 - Parallel LIMIT_IOC Execution)

> [!NOTE]
> **СТАТУС ТЕКУЩЕЙ АРХИТЕКТУРЫ: PARALLEL_LIMIT_IOC (v8.0)**
> В версии **v8.0** система использует параллельный симметричный протокол исполнения `LIMIT_IOC` со следующими рубежами защиты капитала:
> - **Пре-фильтр OBI (Order Book Imbalance):** Оценка дисбаланса топ-уровней стакана на обеих биржах до выстрела (Long-аски, Short-биды). Отсекает вход при давлении против входа. По умолчанию отключен (`enabled: false`) как избыточный для жесткого LIMIT_IOC.
> - **Синтетический выход (Synthetic Exit):** Симуляция немедленного закрытия по встречным стаканам. Блокирует спред, если внутренний bid-ask съедает >50% прибыли (защита от фантомных спредов).
> - **Параллельный вход (Parallel Entry):** Лимитный прострел обеих бирж (LIMIT_IOC) с независимым контролем дальности заброса (`limit_slip_ratio` из `trading_risks` для каждой биржи).
> - **Валидация налива (Fill Validation):** Оценка соотношения налитых объемов `min_hedge_fill_rate` (>= 60%). Если достигнуто — переход в `ACTIVE_HEDGED`. Если нет — `SINGLE_LEG_EXPOSURE`. При нулевом наливе обеих ног — карантин `10s`.
> - **Выход Hedged (Hedged Exit):** Обычный выход по карте `normal_decay`. Спреды рассчитываются строго относительно цены Short-ноги (Bid) для 100% математического совпадения с `PapperSpread`.
> - **Выход Single Leg (Single Leg Exit):** Попытка дозакрыть зависшую ногу лимитками (чейзинг стакана) по карте `chase_map`, либо мгновенный сброс маркетом (если `immediate_market: true` или частичный дисбалансный налив обеих ног).

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
│  • Synthetic Reverse Liquidity Check (ratio: 0.50)     │
│  • Signal Dwell Time Filter (min_signal_dwell_ms)      │
│  • Global Symbol & Exchange Locks (position_manager)   │
│  • Profit Decay Monitoring:                            │
│      - normal_decay (60с TTL, 30с 0.00)                │
└─────────────────────────┬──────────────────────────────┘
                          │ Local Async TCP Socket (IPC)
                          │ [CMD_OPEN, CMD_CLOSE, INIT_TOPOLOGY]
                          │ [POS_OPENED, POS_CLOSED, POS_FAILED]
┌─────────────────────────▼──────────────────────────────┐
│       ПРОЦЕСС 2: Execution & Settlement Worker         │
│               (CORE/executor_process.py)               │
│  • Pre-warmed Persistent TCP/TLS REST Sessions         │
│    (Keepalive Loop каждые 45с с фейк-ордерами warmup)  │
│  • Position FSM (Reactive Finite State Machine v8.0)   │
│  • Reactive Event Bus (asyncio.Event per symbol/side)  │
│    [Zero OS Timer Sleep Jitter: FSM wakeup < 0.1 ms]   │
│  • Real-Time Private WS Position Streams (KuCoin,      │
│    Binance, Bitget) с контрактными множителями         │
│  • Directional Safe Quantization:                      │
│      - BUY: ROUND_FLOOR (никогда не платить выше)      │
│      - SELL: ROUND_CEILING (не продавать ниже)         │
│      - QTY: ROUND_FLOOR + Epsilon 1e-8 (нет остатка)   │
│  • Exact Nominal Dispatch (exact_qty)                  │
│  • Parallel LIMIT_IOC Shot (slippage from risks)       │
│  • Fill Confirmation via WS (timeout 0.6s)             │
│  • Single Leg Book Chasing (chase_map LIMIT_IOC)       │
│  • Emergency Unwind (MARKET reduce_only)               │
│  • Instant 0ms PnL Calculation (analytics.py)          │
│  • Automated Margin & Leverage Setup (leverage_setter) │
└────────────────────────────────────────────────────────┘
```

---

## 🎛 Компоненты системы

### 1. Market Data & Decision Engine (`main.py`)
- **Топология инструментов (`discovery.py`):** Запрашивает 24ч объемы торгов (фильтр от $2M–$5M), отсекает неликвид, строит нормализованный кросс-биржевой маппинг `coin_to_native` для активных связок (`BINANCE_KUCOIN`, `BINANCE_BITGET`).
- **Сбор стаканов:** Поддерживает пулы WebSocket-стримов стаканов глубины (L2 Depth).
- **Микросекундный пре-фильтр (`CORE/math_core.py`):** Каждую итерацию без задержки обновляет 4-колоночную матрицу цен и объемов `prices_array` (`[ask_p, ask_usd, bid_p, bid_usd]`) и прогоняет ее через `pre_calculate_orderbook` (`Numba @njit`), мгновенно отсекая тонкие уровни (`min_top_depth_usd: 50.0`) и сортируя связки по величине спреда без аллокаций памяти Python.
- **Оценка входа (`CORE/trading_engine.py`):** 
  - Рассчитывает взвешенные VWAP-цены входа с дисконтом глубины (`volatility_discount_entry: 0.40`), вычитает суммарные комиссии обеих бирж.
  - Базис спреда рассчитывается относительно цены Short-ноги: `(short_vwap_bid - long_vwap_ask) / short_vwap_bid`.
  - **Синтетический выход (Synthetic Exit):** Симуляция немедленного закрытия встречными стаканами (`max_slippage_ratio = 0.50`, `hard_max_slippage = 0.008`).
  - **Фильтр давления стакана (OBI):** Рассчитывает дисбаланс топ-5 уровней стакана для обеих ног. Опционален, по умолчанию отключен.
- **Signal Dwell Time (Выдержка сигнала):** Фильтр устойчивости сигнала (`min_signal_dwell_ms`, 0 — мгновенный выстрел на первом тике).
- **Мониторинг позиций и деградация профита:** 
  - На каждом тике рассчитывает суммарный арбитражный PnL связки (`net_yield`) по реальным бидам/аскам стаканов с учетом объемов и комиссий (вход + выход обеих ног).
  - Сетка выхода `normal_decay`: 0с — 80% от спреда входа, 15с — 50%, 30с — безубыток (0.0%), 45с — -20%, 60с — аварийный Hard TTL сброс (-999.0).

### 2. Execution & FSM Engine (`CORE/executor_process.py` + `CORE/position_fsm.py`)
- **Реактивная шина событий (Reactive Event Bus):**
  - В приватных сокетах (`BinancePositionStream`, `KucoinPositionStream`, `BitgetPositionStream`) внедрены реестры `_update_events[(symbol, side)] = asyncio.Event()`.
  - При получении пуша сокет мгновенно дергает `_notify(symbol, side)`, пробуждая ожидающие корутины за $< 0.1$ мс без джиттера таймеров OS.
- **Безопасное квантование цен и объемов (`API/orders.py`):**
  - `BUY` $\to$ `ROUND_FLOOR` (гарантирует, что цена покупки лимитки никогда не превысит допустимый предел).
  - `SELL` $\to$ `ROUND_CEILING` (гарантирует, что цена продажи лимитки никогда не опустится ниже расчетного пола).
  - `QTY` $\to$ `ROUND_FLOOR` с защитой эпсилона `1e-8` (устраняет дельту от усечения).
  - Поддержка точного номинала `exact_qty` во всех ордерах.
- **Параллельный вход (Parallel Entry):**
  - Одновременная отправка двух `LIMIT_IOC` ордеров (Long и Short) через `asyncio.gather`.
  - Предельные цены рассчитываются с индивидуальным проскальзыванием `limit_slip_ratio` из `trading_risks` для каждой биржи.
- **Валидация налива и балансировка:**
  - Ожидание подтверждения налива по WS до `fill_confirm_timeout_sec` (0.6 с).
  - При нулевом наливе обеих ног $\to$ Ветка Zero Fill (карантин `10с`).
  - При балансе налива `min(notional) / max(notional) >= min_hedge_fill_rate` (0.60) $\to$ `ACTIVE_HEDGED`.
  - При исполнении только одной ноги $\to$ `SINGLE_LEG_EXPOSURE` (чейзинг стакана).
  - При частичном дисбалансном наливе обеих ног (`< 0.60`) $\to$ экстренный сброс обеих ног по рынку (`IMBALANCED_FILL_UNWOUND`, карантин 1800с).

### 3. Менеджер позиций (`CORE/position_manager.py`)
- **Инвариант биржи (`max_positions`):** Число активных и ожидающих (`pending`) позиций по каждой бирже строго ограничено конфигом (по умолчанию 1).
- **Инвариант символа:** Одна и та же монета не может одновременно торговаться более чем на одной связке.
- **Сохранение флагов состояния:** Сохраняет `actual_gross_spread` и `actual_net_spread` в `active_positions.json`.

### 4. Аналитика и клиринг PnL (`analytics.py`)
- Фиксация реальных цен исполнения обеих ног (`entry_long_price`, `entry_short_price`, `close_long_price`, `close_short_price`).
- Учет полного цикла комиссий (Round-Trip Taker Fees: вход + выход по обеим ногам).
- Синхронная запись сделок в `total_balance.json` и `active_positions.json`.

---

## 🔄 Жизненный цикл сделки v8.0 (Parallel LIMIT_IOC)

```
[Стаканы L2 WS] ──> [pre_calculate_orderbook (Numba JIT)]
                               │ (Топ-кандидат: спред > spread_entry, объем >= 50$)
                               ▼
                    [trading_engine.evaluate_entry]
                               ├── Расчет VWAP с дисконтом волатильности 0.40
                               ├── Синтетический выход (slip <= 0.50*net, hard <= 0.8%)
                               └── Базис спреда: (Short_Bid - Long_Ask) / Short_Bid
                               ▼
                    [Signal Dwell Time Filter] (0 - мгновенный выстрел)
                               ▼
                    [PositionManager.can_enter] -> lock_for_entry
                               ▼
                    [main.py шлет CMD_OPEN через IPC]
                               │
                               ▼
                    [PositionFSM: run_open()]
                     ├── Фаза 1: Одновременный выстрел (PARALLEL LIMIT_IOC)
                     │     ├── Long Leg: BUY LIMIT_IOC (slip из trading_risks)
                     │     └── Short Leg: SELL LIMIT_IOC (slip из trading_risks)
                     │
                     ├── Фаза 2: Реактивное ожидание налива по WS (до 600 мс)
                     │     ├── l_qty == 0 и s_qty == 0: ZERO_FILL -> Карантин 10с
                     │     ├── Налив обеих ног с ratio >= 60%: ACTIVE_HEDGED
                     │     ├── Налита только одна нога: SINGLE_LEG_EXPOSURE
                     │     └── Частичный перекос обеих ног (ratio < 60%):
                     │           Аварийный сброс обеих ног маркетом (IMBALANCED_FILL_UNWOUND)
                     │
                     └── При переходе в SINGLE_LEG_EXPOSURE:
                           ├── Если immediate_market: true -> мгновенный сброс маркетом
                           └── Если immediate_market: false -> Чейзинг стакана (chase_map):
                                 ├── Запрос живого стакана через get_book_ticker (Best Bid / Best Ask)
                                 ├── 0с  -> LIMIT_IOC точно по рынку (price_slip: -0.0)
                                 ├── 15с -> LIMIT_IOC с уступкой вглубь стакана (price_slip: -0.0005 / -0.05%)
                                 ├── 30с -> LIMIT_IOC с уступкой вглубь стакана (price_slip: -0.001 / -0.10%)
                                 ├── 45с -> Принудительный сброс остатка по MARKET (-999.0)
                                 └── Bitget: Автоматическая ориентация side под hedge_mode (устранение ошибки 22002)
                               │
                               ▼
                    [main.py: мониторинг выхода (ACTIVE_HEDGED)]
                               ├── Расчет net_yield по реальным стаканам выхода
                               └── Карта normal_decay:
                                     0с -> 80% профита
                                     15с -> 50% профита
                                     30с -> 0.0% (безубыток)
                                     45с -> -20%
                                     60с -> TTL_EXPIRED (-999.0)
                               ▼
                    [main.py шлет CMD_CLOSE через IPC]
                               │
                               ▼
                    [PositionFSM: run_close()]
                     ├── Параллельная отправка LIMIT_IOC ордеров закрытия
                     ├── Реактивное подтверждение по WS (_wait_for_close_confirmation)
                     ├── При недоливе: аварийная зачистка остатков (до unwind_max_attempts)
                     ├── Мгновенный расчет PnL и Round-Trip комиссий
                     └── Шлет POS_CLOSED -> confirm_exit -> разблокировка биржи
```

---

## ⚙️ Сводка ключевых параметров конфигурации (`cfg.json`)

| Блок / Фаза | Параметр | Значение | Описание |
| :--- | :--- | :--- | :--- |
| **Режим входа** | `order_execution_type` | `"PARALLEL_LIMIT_IOC"` | Параллельный синхронный выстрел в обе ноги |
| **Сигнальные фильтры** | `spread_entry` | `0.008` (0.80%) | Минимальный требуемый чистый спред входа после вычета комиссий |
| | `min_top_depth_usd` | `$50.0` | Минимальный объем (USD) на первом квалифицированном уровне стакана |
| | `min_signal_dwell_ms` | `0` (мс) | Выдержка сигнала перед входом (0 — выстрел на первом тике) |
| | `top_n_candidates` | `4` | Количество лучших связок-кандидатов из `pre_calculate_orderbook` |
| | `max_desync_ms` | `BN_KU: 125, BN_BG: 200` (мс) | Допустимый рассинхрон получения стаканов между биржами |
| | `orderbook_imbalance.enabled` | `false` | Фильтр дисбаланса OBI (отключен как избыточный для LIMIT_IOC) |
| | `synthetic_exit.enabled` | `true` | Симуляция немедленного обратного закрытия |
| | `synthetic_exit.max_slippage_ratio` | `0.50` (50%) | Предельная доля спреда, съедаемая обратным стаканом |
| | `synthetic_exit.hard_max_slippage` | `0.008` (0.80%) | Жесткий потолок обратного проскальзывания |
| **Параллельный вход** | `trading_risks.<ex>.limit_slip_ratio` | `0.0015` (0.15%) | Индивидуальная дальность заброса LIMIT_IOC ордера для каждой биржи |
| | `min_hedge_fill_rate` | `0.60` (60%) | Минимальный баланс налива ног для признания позиции хеджированной |
| | `fill_confirm_timeout_sec` | `0.6` (600 мс) | Таймаут ожидания подтверждения налива по WS |
| | `fill_confirm_poll_interval_sec` | `0.0` | Интервал опроса WS-кэша (0.0 = чистый Event-driven) |
| | `entry_api_timeout_sec` | `5.0` (сек) | Таймаут параллельной отправки пары ордеров LIMIT_IOC в asyncio.wait_for |
| **Выход из хеджа** | `normal_decay` | `60 сек` | Сетка деградации: 0с: 80%, 15с: 50%, 30с: 0%, 45с: -20%, 60с: TTL |
| | `close_confirm_timeout_sec` | `1.8` (с) | Таймаут реактивного подтверждения закрытия позиции |
| **Выход Single Leg** | `single_leg_exit.immediate_market` | `false` | Использовать ли чейзинг лимитками перед сбросом по рынку |
| | `single_leg_exit.chase_map` | `4 шага (до 45с)` | Чейзинг: 0с: 0.0%, 15с: -0.05%, 30с: -0.10%, 45с: MARKET (-999.0) |
| | `single_leg_exit.limit_fill_wait_sec` | `0.5` (500 мс) | Пауза ожидания исполнения лимитного ордера чейзинга перед проверкой |
| **Аварийный сброс** | `emergency_unwind.max_attempts` | `2` | Число повторных попыток очистки остатков позиции |
| | `emergency_unwind.retry_pause_sec` | `0.05` (50 мс) | Пауза между попытками очистки остатков |
| | `emergency_unwind.ws_verify_timeout_sec` | `0.3` (300 мс) | Таймаут реактивного подтверждения обнуления позиции по WS |
| **Сетевой прогрев** | `network_settings.rest_keepalive_interval_sec` | `45` (сек) | Фоновый опрос/прогрев постоянных TCP/TLS сессий к биржам |
| | `network_settings.idle_warmup_threshold_sec` | `30` (сек) | Порог бездействия перед отправкой прогревочного фейк-ордера |
| **Карантины** | `zero_fill` | `10` (сек) | Карантин при нулевом наливе обеих ног |
| | `single_leg_exposure` | `1800` (30 мин) | Карантин при зависании одной ноги / перекосе налива |
| | `entry_error` | `3600` (1 час) | Карантин при сетевой ошибке отправки ордера |
| | `loss_trade` | `3600` (1 час) | Карантин при закрытии сделки с убытком |
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

4. **`live_tests/test_all_exchanges_single_leg.py`**:
   - Сквозной валидатор ветки выхода из зависшей ноги (`single_leg_exit`) с лимитным чейзингом стакана на всех трех биржах (**Bitget**, **Binance**, **Kucoin**).
   - Эмулирует зависание реальной позиции (~6–13 USD) и передает ее в боевой `PositionFSM._run_single_leg_exposure`.
   - **Фактические показатели на боевом счете:**
     - **Bitget (XRPUSDT):** Успешное сведение `LIMIT_IOC` за **`1.41 с`** с нулевым рассинхроном и полным исключением ошибки `22002`.
     - **Binance (XRPUSDT):** Успешное сведение `LIMIT_IOC` за **`1.35 с`**.
     - **Kucoin (XRPUSDTM):** Успешное сведение `LIMIT_IOC` за **`1.33 с`**.
     - Все позиции сведены до остатка `0.0`.
