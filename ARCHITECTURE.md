# 🧠 Архитектура HFT Спредера (Mental Map v7.0 - Asymmetric LIMIT_IOC Execution)

> [!NOTE]
> **СТАТУС ТЕКУЩЕЙ АРХИТЕКТУРЫ: ASYMMETRIC_LIMIT_IOC (v7.0)**
> В версии **v7.0** система переведена с убыточного симметричного `MARKET`-входа на интеллектуальный асимметричный протокол `LIMIT_IOC`.
> - **Фаза 1 (Lead Leg):** Лимитный прострел слабой биржи (Bitget/Kucoin) с контролем жесткого проскальзывания.
> - **Фаза 2 (Validation):** Контроль `Model Drift` (защита от сильного ухода синтетической цены). При превышении (напр. > 80% спреда съедено) — 1-Shot Kill Switch.
> - **Фаза 3 (Hedge Leg):** Итеративный дожим сильной биржи (Binance) лимитными ордерами по `hedge_decay_map` (динамическая уступка цены).
> - **Фаза 4 (Resolution):** Выравнивание ног. Если недолив сильной ноги < 25%, излишек слабой ноги подрезается `MARKET reduce_only`. Иначе (или при отказе API подрезать ногу, как на Bitget V2) — аварийный полный сброс.

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
│  • Signal Dwell Time Filter (min_signal_dwell_ms)      │
│  • Global Symbol & Exchange Locks (position_manager)   │
│  • Profit Decay Monitoring:                            │
│      - profit_decay_map (стандартная 7-ступенчатая)    │
│      - extreme_profit_decay_map (аварийная 2-шаговая)  │
└─────────────────────────┬──────────────────────────────┘
                          │ Local Async TCP Socket (IPC)
                          │ [CMD_OPEN, CMD_CLOSE, INIT_TOPOLOGY]
                          │ [POS_OPENED, POS_CLOSED, POS_FAILED]
┌─────────────────────────▼──────────────────────────────┐
│       ПРОЦЕСС 2: Execution & Settlement Worker         │
│               (CORE/executor_process.py)               │
│  • Pre-warmed Persistent TCP/TLS REST Sessions         │
│    (Keepalive Loop каждые 45с с фейк-ордерами warmup)  │
│  • Position FSM (Reactive Finite State Machine)        │
│  • Reactive Event Bus (asyncio.Event per symbol/side)  │
│    [Zero OS Timer Sleep Jitter: FSM wakeup < 0.1 ms]   │
│  • Granular Timing Telemetry (REST RTT + WS Push Lag)  │
│  • Direct Fast Order Dispatch (asyncio.gather)         │
│  • Real-Time Private WS Position Streams:              │
│      - Binance: ACCOUNT_UPDATE (мгновенный кэш)        │
│      - Kucoin: /contract/positionAll & tradeOrders     │
│      - Bitget: v2 Private Positions Channel            │
│  • Actual Entry Spread Validation (min_spread_entry)   │
│  • 1-Shot HFT Market Kill-Switch (emergency unwind)    │
│  • Instant 0ms PnL Calculation (analytics.py)          │
│  • Automated Margin & Leverage Setup (leverage_setter) │
└────────────────────────────────────────────────────────┘
```

---

## 🎛 Компоненты системы

### 1. Market Data & Decision Engine (`main.py`)
- **Топология инструментов (`discovery.py`):** Запрашивает 24ч объемы торгов (фильтр от $2M–$5M), отсекает неликвид, строит нормализованный кросс-биржевой маппинг `coin_to_native` для активных связок (`BINANCE_KUCOIN`, `BINANCE_BITGET`).
- **Сбор стаканов:** Поддерживает пулы WebSocket-стримов стаканов глубины (L2 Depth).
- **Микросекундный пре-фильтр (`CORE/math_core.py`):** Каждую итерацию без задержки обновляет 4-колоночную матрицу цен и объемов `prices_array` (`[ask_p, ask_usd, bid_p, bid_usd]`) и прогоняет ее через `pre_calculate_orderbook` (`Numba @njit`), мгновенно отсекая тонкие уровни (`min_top_depth_usd: 20.0`) и сортируя связки по величине спреда без аллокаций памяти Python.
- **Оценка входа (`CORE/trading_engine.py`):** Для топ-кандидатов рассчитывает взвешенные VWAP-цены входа с дисконтом глубины (`volatility_discount_entry: 0.40`), вычитает суммарные комиссии обеих бирж и проверяет синтетическое проскальзывание.
- **Signal Dwell Time (Выдержка сигнала):** Опциональный фильтр устойчивости сигнала (`min_signal_dwell_ms`, 0 — выстрел на 1-м тике). Отсекает микросекундные фантомы при необходимости.
- **Мониторинг позиций и деградация профита:** 
  - На каждом тике рассчитывает суммарный арбитражный PnL связки (`net_yield`) по реальным бидам/аскам стаканов с учетом объемов и 4 комиссий (вход + выход обеих ног).
  - Поддерживает две карты деградации: `profit_decay_map` (основная) и `extreme_profit_decay_map` (аварийная).

### 2. Execution & FSM Engine (`CORE/executor_process.py` + `CORE/position_fsm.py`)
- **Реактивная шина событий (Reactive Event Bus v6.0):**
  - Во всех приватных сокетах (`API/BINANCE/ws_private_binance.py`, `API/KUCOIN/ws_private_kucoin.py`, `API/BITGET/ws_private_bitget.py`) внедрены реестры `_update_events[(symbol, side)] = asyncio.Event()`.
  - При получении пуша об изменении позиции или исполнении сделки сокет мгновенно дергает `_notify(symbol, side)`, пробуждая ожидающие корутины в микросекунды ($< 0.1$ мс).
  - В `PositionFSM` методы `_wait_for_fill_confirmation` и `_wait_for_close_confirmation` переведены на предикатные циклы с пробуждением по первому завершенному событию (`asyncio.wait(..., return_when=FIRST_COMPLETED)`). Полностью ликвидирован системный джиттер Windows-таймеров (15.6 мс) от вызовов `asyncio.sleep()`.
  - Подписка на события регистрируется **до** выстрела ордеров (защита от гонок), а отписка гарантируется блоком `finally`.
- **Гранулярная телеметрия латентности:**
  - В моменты входа (`run_open`) и выхода (`run_close`) замеряется и логируется пошаговый профиль времени:
    - Чистый REST RTT по каждой ноге отдельно (`orders[ex].place_order`);
    - Время параллельного сбора HTTP ответов (`asyncio.gather`);
    - Точный тайминг прихода WebSocket-пуша подтверждения налива/обнуления по каждой бирже;
    - Чистый лаг сокета относительно HTTP ответа.
- **Прогретые REST-сессии (`API/orders.py`):** 
  - Высокоскоростная прямая отправка ордеров через нативный HTTP REST по прогретым соединениям.
  - Фоновый цикл `_keepalive_loop()` раз в 45 сек шлет фейковые невалидные ордера (`warmup`), удерживая открытыми постоянные TCP/TLS сокеты к серверам бирж в Токио.
  - Латентность параллельной отправки обеих ног в бою: **`25–30 мс`**!
- **Приватные стримы позиций (`ws_private_*.py`):**
  - **Kucoin**: подписан на топик `/contract/positionAll` + кэш `last_close_prices` из `tradeOrders`. Скорость закрытия: **`5–15 мс`**.
  - **Binance**: мгновенно ловит `ACCOUNT_UPDATE` и обнуляет кэш. Скорость: **`5–25 мс`**.
  - **Bitget**: слушает канал позиций и `fill` ордеров. Внутренний диспатч биржи занимает **`50–70 мс`**.
- **Контроль фактического спреда входа (`min_spread_entry`):**
  - Сразу после налива ордеров FSM берет фактические цены исполнения $P_{\text{long}}$ и $P_{\text{short}}$ из стримов и рассчитывает чистый факт спреда с учетом комиссий:
    $$\text{Actual Net Spread} = \frac{P_{\text{short}} - P_{\text{long}}}{P_{\text{long}}} - (\text{Fee}_{\text{long}} + \text{Fee}_{\text{short}})$$
  - Если факт $\le \text{min\_spread\_entry}$ (например, $\le 0.15\%$), активируется флаг `use_extreme_decay = True`.
- **Экстремальная карта деградации (`extreme_profit_decay_map`):**
  - Шаг 0 (первые 60 сек): попытка закрыться в безубыток (`target_val = 0.0000`).
  - Шаг 1 (на 60-й секунде): безусловный аварийный выход по рынку (`target_val = -999.0`, `TTL_EXPIRED`).
- **1-Shot HFT Market Kill-Switch (`_emergency_unwind`):**
  - При асимметрии налива или сбое одной из ног FSM моментально ликвидирует вторую ногу встречным рыночным ордером, гарантируя нулевую позицию (flat).

### 3. Менеджер позиций (`CORE/position_manager.py`)
- **Инвариант биржи (`max_positions`):** Число активных и ожидающих (`pending`) позиций по каждой бирже строго ограничено конфигом. При занятости биржи блокируются все использующие ее маршруты.
- **Инвариант символа:** Одна и та же монета не может одновременно торговаться более чем на одной связке.
- **Сохранение флагов состояния:** Сохраняет `use_extreme_decay`, `actual_gross_spread` и `actual_net_spread` в `active_positions.json`, сохраняя устойчивость к перезапускам.

### 4. Аналитика и клиринг PnL (`analytics.py`)
- **Мгновенный 0ms расчет PnL:** Фиксация реальных цен исполнения обеих ног на входе и на выходе (`entry_long_price`, `entry_short_price`, `close_long_price`, `close_short_price`).
- Учет полного цикла комиссий (Round-Trip Taker Fees: вход + выход по обеим ногам).
- Синхронная запись сделок в `total_balance.json` и `active_positions.json`.

---

## 🔄 Жизненный цикл сделки v7.0 (Asymmetric LIMIT_IOC)

```
[Стаканы L2 WS] ──> [pre_calculate_orderbook (Numba JIT)]
                               │ (Топ-кандидат: спред > spread_entry)
                               ▼
                    [trading_engine.evaluate_entry]
                               │ (VWAP OK, Synthetic Slippage OK)
                               ▼
                    [Signal Dwell Time Filter]
                               │ min_signal_dwell_ms (0 - выстрел сразу)
                               ▼
                    [PositionManager.can_enter]
                               │ Проверка лимитов бирж и монет
                               ▼ lock_for_entry (pending += 1)
                    [main.py шлет CMD_OPEN через IPC]
                               │
                               ▼
                    [PositionFSM: run_open()]
                     ├── Фаза 1: Выстрел в Lead Leg (Слабая нога)
                     │     └─ Отправка LIMIT_IOC ордера с допустимым проскальзыванием
                     │     └─ Ожидание ответа. Если налив 0 -> Карантин zero_fill
                     │
                     ├── Фаза 2: Валидация налива Lead Leg
                     │     └─ Проверка minNotional (напр. > 5$)
                     │     └─ Проверка Model Drift Ratio (уход цены от синтетической модели)
                     │     └─ Если уход > 80% -> 1-Shot Market Kill-Switch (Сброс ноги)
                     │
                     ├── Фаза 3: Дожим сильной ноги (Hedge Leg)
                     │     └─ Итеративный заброс LIMIT_IOC ордеров с уступкой спреда
                     │     └─ До 3 попыток по карте hedge_decay_map
                     │
                     ├── Фаза 4: Выравнивание дельты
                     │     └─ Если недолив < 25%: MARKET reduce_only подрезка излишка Lead Leg
                     │     └─ Если недолив >= 25% (или подрезка запрещена биржей): Полный аварийный развал обеих ног
                     │
                     └── Успешный вход -> Расчет target_net_spread -> POS_OPENED
                               │
                               ▼
                    [main.py: мониторинг спреда по exit_decay_map]
                               ├── Relative Profit Decay Map (относительно фактического спреда входа)
                               │ Достигнут таргет спреда / TTL
                               ▼
                    [main.py шлет CMD_CLOSE через IPC]
                               │
                               ▼
                    [PositionFSM: run_close()]
                     ├── Параллельная отправка LIMIT_IOC ордеров на закрытие (прогретый REST)
                     ├── Реактивное подтверждение по WS (_wait_for_close_confirmation)
                     ├── Если недолив при выходе -> Аварийный сброс по рынку остатков
                     ├── Мгновенный расчет PnL и Round-Trip комиссий (record_trade)
                     └── Шлет POS_CLOSED -> confirm_exit -> разблокировка биржи
```

---

## ⚙️ Сводка ключевых параметров конфигурации (`cfg.json`)

| Параметр | Значение | Описание |
| :--- | :--- | :--- |
| `order_execution_type` | `"MARKET"` | Строго рыночное высокоскоростное исполнение через параллельные прогретые REST-сессии |
| `network_settings.rest_keepalive_interval_sec` | `45` (сек) | Интервал цикла фонового прогрева TCP/TLS соединений (отправка невалидных IOC-ордеров) |
| `network_settings.idle_warmup_threshold_sec` | `30` (сек) | Порог бездействия для прогрева (не слать лишних warmup, если бот недавно стрелял) |
| `min_signal_dwell_ms` | `0` (мс) | Минимальная выдержка сигнала перед входом (0 — моментальный выстрел на первом же тике) |
| `max_desync_ms` | `BN_KU: 125, BN_BG: 200` (мс) | Максимальный допустимый рассинхрон получения стаканов между биржами (настраивается индивидуально по связкам) |
| `min_top_depth_usd` | `$20.0` | Фильтр глубины: минимальный объем (USD) на первом уровне стакана в `pre_calculate_orderbook` |
| `spread_entry` | `0.008` (0.80%) | Минимальный требуемый чистый спред входа после вычета комиссий |
| `min_spread_entry` | `0.0015` (0.15%) | Порог проверки фактического спреда налива. Если `actual_net_spread <= min_spread_entry`, включается `extreme_profit_decay_map` |
| `extreme_profit_decay_map` | `[0с: 0.0%, 60с: -999.0]` | Аварийная 2-ступенчатая карта: 60 сек попытка выйти в 0, затем мгновенный рыночный сброс |
| `profit_decay_map` | `7 ступеней` | Основная динамическая лестница взятия прибыли (от 0.70% до 0.10% за 1200 сек) |
| `volatility_discount_entry` | `0.40` (40%) | Дисконт ликвидности стакана при расчете цен входа |
| `volatility_discount_exit` | `0.85` (85%) | Дисконт ликвидности стакана при расчете цен выхода |
| `min_fill_rate` | `0.985` (98.5%) | Минимальный процент налива для удержания позиции |
| `fill_confirm_timeout_sec` | `0.600` (600 мс) | Таймаут ожидания налива по сокету до срабатывания Kill-Switch |
| `close_confirm_timeout_sec` | `1.800` (1.8 с) | Таймаут подтверждения закрытия позиции |
| `volume_filters` | `$5M` (все биржи) | Минимальный суточный объем торгов монеты для допуска в торговлю |
| `max_positions` | `1` | Максимум 1 одновременная позиция на биржу |
| `active_routes` | Настраиваемые связки | Например, `BINANCE_KUCOIN`, `BINANCE_BITGET` |

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
