# 🧠 Архитектура HFT Спредера (Mental Map v5.0)

Проект представляет собой промышленный высокочастотный арбитражный комплекс (Hedge/Spread), торгующий синтетический межбиржевой спред между криптобиржами в режиме **Hedge Mode**. Бот написан на асинхронном Python (`asyncio`), использует `@njit` (`Numba`) для микросекундной фильтрации стаканов и архитектуру изолированных процессов с приватными WebSocket-стримами исполнения.

В версии **v5.0** архитектура сфокусирована на **ультранизкой задержке (Ultra-Low Latency HFT)**: прямой рыночный вход (`MARKET`), предварительно прогретые приватные торговые WebSocket-коннекторы, фильтр выдержки сигнала (`Signal Dwell Time`), реактивное отслеживание налива через локальный кэш сокетов и мгновенный 1-Shot Kill-Switch при рассинхроне.

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
│  • Signal Dwell Time Filter (25 ms persistence check)  │
│  • Global Symbol & Exchange Locks (position_manager)   │
│  • Profit Decay Monitoring (profit_decay_map)          │
└─────────────────────────┬──────────────────────────────┘
                          │ Local Async TCP Socket (IPC)
                          │ [CMD_OPEN, CMD_CLOSE, INIT_TOPOLOGY]
                          │ [POS_OPENED, POS_CLOSED, POS_FAILED]
┌─────────────────────────▼──────────────────────────────┐
│       ПРОЦЕСС 2: Execution & Settlement Worker         │
│               (CORE/executor_process.py)               │
│  • Pre-warmed Fast WS Private Traders (Binance, Kucoin,│
│    Bitget) с постоянным heartbeat/keepalive             │
│  • Position FSM (Finite State Machine per Position)    │
│  • Direct MARKET Order Dispatch (asyncio.gather)       │
│  • Reactive Fill/Close Verification via WS Cache (15ms)│
│  • 1-Shot HFT Market Kill-Switch (emergency unwind)    │
│  • Instant 0ms PnL Calculation (analytics.py)          │
│  • Automated Margin & Leverage Setup (leverage_setter) │
└────────────────────────────────────────────────────────┘
```

---

## 🎛 Компоненты системы

### 1. Market Data & Decision Engine (`main.py`)
- **Топология инструментов (`discovery.py`):** Запрашивает 24ч объемы торгов (фильтр от $2M–$5M), отсекает неликвид, строит нормализованный кросс-биржевой маппинг `coin_to_native` для активных связок (например, `BINANCE_KUCOIN`, `BINANCE_BITGET`).
- **Сбор стаканов:** Поддерживает пулы WebSocket-стримов стаканов глубины (L2 Depth).
- **Микросекундный пре-фильтр (`CORE/math_core.py`):** Каждую итерацию без задержки обновляет матрицу цен `prices_array` и прогоняет ее через `pre_calculate_orderbook` (`Numba @njit`), мгновенно сортируя связки по величине спреда без аллокаций памяти Python.
- **Оценка входа (`CORE/trading_engine.py`):** Для топ-кандидатов рассчитывает взвешенные VWAP-цены входа с дисконтом глубины (`volatility_discount_entry: 0.50`), вычитает суммарные комиссии обеих бирж и проверяет синтетическое проскальзывание.
- **Signal Dwell Time (Выдержка сигнала):** Перед отправкой ордера кандидат должен непрерывно удерживать спред $\ge \text{min\_signal\_dwell\_ms}$ (по умолчанию 25 мс). Это отсекает микросекундные фантомные шипы и алгоритмические ловушки отмен.
- **Мониторинг позиций:** Отслеживает время удержания и деградацию спреда по таблице `profit_decay_map`, отправляя команду `CMD_CLOSE` при достижении таргета или сработке TTL.

### 2. Execution & FSM Engine (`CORE/executor_process.py` + `CORE/position_fsm.py`)
- **Прогретые торговые WebSocket-коннекторы (`API/orders.py`):** 
  - `BinanceWsTrader`, `BitgetWsTrader`, `KucoinWsTrader` стартуют в фоновом режиме еще при инициализации исполнителя (`init_runtime`), поддерживая живые сокеты и готовые сессии до прихода первого сигнала.
- **Политика исполнения ордеров:**
  - Исключительно **`MARKET`**: мгновенный удар в стакан без зависания лимиток и без очередей отмен.
  - Обе ноги запускаются одновременно через `asyncio.gather` в естественном параллельном порядке.
- **Реактивная верификация налива (`_wait_for_fill_confirmation`):**
  - Опрашивает локальный WS-кэш позиций с частотой `0 мс` (чистый перебор в цикле событий).
  - Налив фиксируется за 15–30 мс по приходу сокетного события баланса без задержек REST.
- **Реактивное подтверждение выхода (`_wait_for_close_confirmation`):**
  - Подтверждает закрытие позиций (размер = 0.0) по приватному вебсокету за 20–50 мс, исключая задержку между выходом и фиксацией в аналитике.
- **1-Shot HFT Market Kill-Switch (`_emergency_unwind`):**
  - Если на входе одна нога налилась, а по второй произошел сбой сети или таймаут `fill_confirm_timeout_sec`, FSM моментально отправляет 1 встречный MARKET-ордер на ликвидацию открытой ноги.
  - Позиция немедленно сбрасывается в 0 (flat), предотвращая направленный убыток, а символ уходит в карантин (`banned_symbols.json`).

### 3. Менеджер позиций (`CORE/position_manager.py`)
- **Инвариант биржи (`max_positions`):** Число активных и ожидающих (`pending`) позиций по каждой бирже строго ограничено конфигом. При занятости биржи блокируются все использующие ее маршруты.
- **Инвариант символа:** Одна и та же монета не может одновременно торговаться более чем на одной связке.
- **Deadlock-Free State Machine:** Гарантированный откат замков (`rollback_entry`) при ошибках валидации ордеров или частичном сбросе.

### 4. Аналитика и клиринг PnL (`analytics.py`)
- **Мгновенный 0ms расчет PnL:** Фиксация реальных цен исполнения обеих ног на входе и на выходе (`entry_long_price`, `entry_short_price`, `close_long_price`, `close_short_price`).
- Учет полного цикла комиссий (Round-Trip Taker Fees: вход + выход по обеим ногам).
- Синхронная запись сделок в `total_balance.json` и `active_positions.json`.

---

## 🔄 Жизненный цикл сделки v5.0

```
[Стаканы L2 WS] ──> [pre_calculate_orderbook (Numba JIT)]
                               │ (Топ-кандидат: спред > spread_entry)
                               ▼
                    [trading_engine.evaluate_entry]
                               │ (VWAP OK, Synthetic Slippage OK)
                               ▼
                    [Signal Dwell Time Filter]
                               │ Удержание сигнала >= 25 мс?
                               │ ├── Нет ──> ждем подтверждения / сброс таймера при шуме
                               ▼ Да
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
                     ├── Параллельная отправка MARKET-ордеров в прогретые WS
                     │     (asyncio.gather: place_order Long & Short)
                     │
                     ├── Реактивный опрос WS-кэша (_wait_for_fill_confirmation)
                     │     └─ Время подтверждения: 15–30 мс
                     │
                     ├── Проверка Fill Rate (min_fill_rate: 0.985):
                     │     ├── Рассинхрон/сбой ноги? 
                     │     │     └─> 1-Shot Market Kill-Switch (_emergency_unwind)
                     │     │         └── Сброс ноги в 0 -> POS_FAILED -> Quarantined
                     │     └── Обе ноги налиты (>=98.5%)
                     │           └─> Фиксация цен -> confirm_entry -> POS_OPENED
                               │
                               ▼
                    [main.py: мониторинг profit_decay_map]
                               │ Достигнут таргет спреда / TTL
                               ▼
                    [main.py шлет CMD_CLOSE через IPC]
                               │
                               ▼
                    [PositionFSM: run_close()]
                     ├── Параллельная отправка MARKET-ордеров на закрытие
                     ├── Реактивное подтверждение по WS (_wait_for_close_confirmation)
                     ├── Мгновенный расчет PnL и Round-Trip комиссий (record_trade)
                     └── Шлет POS_CLOSED -> confirm_exit -> разблокировка биржи
```

---

## ⚙️ Сводка ключевых параметров конфигурации (`cfg.json`)

| Параметр | Значение | Описание |
| :--- | :--- | :--- |
| `order_execution_type` | `"MARKET"` | Строго рыночное высокоскоростное исполнение через WebSocket |
| `min_signal_dwell_ms` | `25` (мс) | Минимальная выдержка сигнала перед входом (0 — отключено) |
| `spread_entry` | `0.007` (0.70%) | Минимальный чистый спред входа после вычета комиссий |
| `volatility_discount_entry` | `0.50` (50%) | Дисконт ликвидности стакана при расчете цен входа |
| `volatility_discount_exit` | `0.55` (55%) | Дисконт ликвидности стакана при расчете цен выхода |
| `min_fill_rate` | `0.985` (98.5%) | Минимальный процент налива для удержания позиции |
| `fill_confirm_timeout_sec` | `0.600` (600 мс) | Таймаут ожидания налива по сокету до Kill-Switch |
| `close_confirm_timeout_sec` | `1.800` (1.8 с) | Таймаут подтверждения закрытия позиции |
| `volume_filters` | `$5M` (BN), `$2M` (Others) | Минимальный суточный объем торгов монеты |
| `max_positions` | `1` | Максимум 1 одновременная позиция на биржу |
| `active_routes` | Настраиваемые связки | Например, `BINANCE_KUCOIN`, `BINANCE_BITGET` |
