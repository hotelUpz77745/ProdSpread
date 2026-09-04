# 🧠 Архитектура HFT Спредера (Mental Map v5.1)

Проект представляет собой промышленный высокочастотный арбитражный комплекс (Hedge/Spread), торгующий синтетический межбиржевой спред между криптобиржами в режиме **Hedge Mode**. Бот написан на асинхронном Python (`asyncio`), использует `@njit` (`Numba`) для микросекундной фильтрации стаканов и архитектуру изолированных процессов с приватными WebSocket-стримами данных и горячими REST-сессиями исполнения.

В версии **v5.1** архитектура сфокусирована на **ультранизкой задержке (Ultra-Low Latency HFT, < 30 мс)**: прямой параллельный рыночный вход (`MARKET`) через постоянные прогретые TCP/TLS keepalive REST-сессии, нативные приватные WebSocket-стримы позиций (`/contract/positionAll` на Kucoin, `ACCOUNT_UPDATE` на Binance, приватный WS на Bitget), адаптивный контроль качества входа (`min_spread_entry` + `extreme_profit_decay_map`) и мгновенный 1-Shot Kill-Switch при рассинхроне.

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
│  • Position FSM (Finite State Machine per Position)    │
│  • Direct Fast MARKET Order Dispatch (asyncio.gather)  │
│  • Real-Time Private WS Position Streams:              │
│      - Binance: ACCOUNT_UPDATE (мгновенный кэш)        │
│      - Kucoin: /contract/positionAll & tradeOrders     │
│      - Bitget: v2 Private Positions Channel            │
│  • Actual Entry Spread Validation (min_spread_entry)   │
│  • Reactive Fill/Close Verification via WS (5–20 ms)   │
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
- **Микросекундный пре-фильтр (`CORE/math_core.py`):** Каждую итерацию без задержки обновляет матрицу цен `prices_array` и прогоняет ее через `pre_calculate_orderbook` (`Numba @njit`), мгновенно сортируя связки по величине спреда без аллокаций памяти Python.
- **Оценка входа (`CORE/trading_engine.py`):** Для топ-кандидатов рассчитывает взвешенные VWAP-цены входа с дисконтом глубины (`volatility_discount_entry: 0.50`), вычитает суммарные комиссии обеих бирж и проверяет синтетическое проскальзывание.
- **Signal Dwell Time (Выдержка сигнала):** Опциональный фильтр устойчивости сигнала (`min_signal_dwell_ms`, 0 — выстрел на 1-м тике). Отсекает микросекундные фантомы при необходимости.
- **Мониторинг позиций и деградация профита:** 
  - На каждом тике рассчитывает суммарный арбитражный PnL связки (`net_yield`) по реальным бидам/аскам стаканов с учетом объемов и 4 комиссий (вход + выход обеих ног).
  - Поддерживает две карты деградации: `profit_decay_map` (основная) и `extreme_profit_decay_map` (аварийная).

### 2. Execution & FSM Engine (`CORE/executor_process.py` + `CORE/position_fsm.py`)
- **Прогретые REST-сессии (`API/orders.py`):** 
  - Высокоскоростная прямая отправка ордеров через нативный HTTP REST по прогретым соединениям.
  - Фоновый цикл `_keepalive_loop()` раз в 45 сек (настраивается в `network_settings.rest_keepalive_interval_sec`) шлет фейковые невалидные ордера (`warmup`), удерживая открытыми постоянные TCP/TLS сокеты к серверам бирж в Токио.
  - Защита `idle_warmup_threshold_sec`: если бот недавно торговал, фейковые запросы не отправляются, сокет уже горячий.
  - Латентность параллельной отправки обеих ног в бою: **`26–29 мс`**!
- **Приватные стримы позиций (`ws_private_*.py`):**
  - **Kucoin**: подписан на официальный топик `/contract/positionAll` + сохраняет цены исполнения сделок из `tradeOrders` в `last_close_prices`. При `currentQty == 0` позиция мгновенно сбрасывается в 0.0. Скорость подтверждения закрытия: **`5.6 мс`**!
  - **Binance**: мгновенно ловит `ACCOUNT_UPDATE` и обнуляет кэш. Скорость: **`5–10 мс`**.
  - **Bitget**: слушает канал позиций и `fill` ордеров. Скорость: **`50–60 мс`**.
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

## 🔄 Жизненный цикл сделки v5.1

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
                     ├── Pre-flight check_order_size
                     │     └─ Ошибка: шлет POS_FAILED -> rollback_entry
                     │
                     ├── Параллельная отправка MARKET-ордеров в прогретые REST-сессии
                     │     (asyncio.gather: place_order Long & Short, RTT ~26–29 мс)
                     │
                     ├── Реактивный опрос WS-кэша (_wait_for_fill_confirmation)
                     │     └─ Время подтверждения: 5–25 мс
                     │
                     ├── Проверка Fill Rate (min_fill_rate: 0.985):
                     │     ├── Рассинхрон/сбой ноги? 
                     │     │     └─> 1-Shot Market Kill-Switch (_emergency_unwind)
                     │     │         └── Сброс ноги в 0 -> POS_FAILED -> Quarantined
                     │     └── Обе ноги налиты (>=98.5%)
                     │           ├── Расчет фактического чистого спреда:
                     │           │     actual_net_spread = (P_short - P_long)/P_long - (Fee_long + Fee_short)
                     │           ├── if actual_net_spread <= min_spread_entry:
                     │           │     └─> use_extreme_decay = True (аварийный режим выхода)
                     │           └─> Фиксация цен -> confirm_entry -> POS_OPENED
                               │
                               ▼
                    [main.py: мониторинг спреда по decay_map]
                               ├── Если use_extreme_decay: extreme_profit_decay_map (0с: 0.00%, 60с: -999.0)
                               └── Иначе: profit_decay_map (стандартная 7-ступенчатая)
                               │ Достигнут таргет спреда / TTL
                               ▼
                    [main.py шлет CMD_CLOSE через IPC]
                               │
                               ▼
                    [PositionFSM: run_close()]
                     ├── Параллельная отправка MARKET-ордеров на закрытие (прогретый REST)
                     ├── Реактивное подтверждение по WS (_wait_for_close_confirmation, 5–60 мс)
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
| `spread_entry` | `0.008` (0.80%) | Минимальный требуемый чистый спред входа после вычета комиссий |
| `min_spread_entry` | `0.0015` (0.15%) | Порог проверки фактического спреда налива. Если `actual_net_spread <= min_spread_entry`, включается `extreme_profit_decay_map` |
| `extreme_profit_decay_map` | `[0с: 0.0%, 60с: -999.0]` | Аварийная 2-ступенчатая карта: 60 сек попытка выйти в 0, затем мгновенный рыночный сброс |
| `profit_decay_map` | `7 ступеней` | Основная динамическая лестница взятия прибыли (от 0.70% до 0.10% за 1200 сек) |
| `volatility_discount_entry` | `0.50` (50%) | Дисконт ликвидности стакана при расчете цен входа |
| `volatility_discount_exit` | `0.55` (55%) | Дисконт ликвидности стакана при расчете цен выхода |
| `min_fill_rate` | `0.985` (98.5%) | Минимальный процент налива для удержания позиции |
| `fill_confirm_timeout_sec` | `0.600` (600 мс) | Таймаут ожидания налива по сокету до срабатывания Kill-Switch |
| `close_confirm_timeout_sec` | `1.800` (1.8 с) | Таймаут подтверждения закрытия позиции |
| `volume_filters` | `$5M` (BN), `$2M` (Others) | Минимальный суточный объем торгов монеты |
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
