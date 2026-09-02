# 🧠 Архитектура HFT Спредера (Mental Map v3.0)

Проект представляет собой промышленный высокочастотный арбитражный комплекс (Hedge/Spread), торгующий синтетический спред между криптобиржами в режиме **Hedge Mode**. Бот написан на асинхронном Python (`asyncio`) и использует `@njit` (`Numba`) для микросекундной обработки стаканов.

В версии **v3.0** система поддерживает **одновременную работу 3 связок бирж (`BINANCE_KUCOIN`, `BINANCE_BITGET`, `KUCOIN_BITGET`)**, гибкие режимы исполнения ордеров (`IOC` и `LIMIT_GTC`), межпроцессный транспорт по локальному сокету (IPC) и отказоустойчивые замки бирж.

---

## 🏗 Топология двух процессов (2-Process Architecture)

Для полного обхода GIL и предотвращения сетевых лагов система разделена на два изолированных процесса, общающихся через неблокирующий локальный TCP-сокет (`CORE/ipc_socket.py`):

```
┌────────────────────────────────────────────────────────┐
│      ПРОЦЕСС 1: Market Data & Decision Engine          │
│                    (main.py)                           │
│  • Public WS Streams (Binance, KuCoin, Bitget)         │
│  • Numba JIT Orderbook Scan (pre_calculate_orderbook)   │
│  • VWAP & Synthetic Slippage Check (trading_engine.py) │
│  • Global Symbol & Exchange Semaphores (PositionMgr)   │
└─────────────────────────┬──────────────────────────────┘
                          │ Local Async TCP Socket (IPC)
                          │ [CMD_OPEN, CMD_CLOSE, INIT_TOPOLOGY]
                          │ [POS_OPENED, POS_CLOSED, POS_FAILED]
┌─────────────────────────▼──────────────────────────────┐
│       ПРОЦЕСС 2: Execution & Settlement Worker         │
│               (CORE/executor_process.py)               │
│  • Private WS Streams (Position Tracking)              │
│  • Order Adapters (IOC / LIMIT_GTC + Cancel Remnants)  │
│  • Fill Rate Verification & Emergency Dumps            │
│  • Multi-Exchange Settlement & Actual PnL Clearing     │
│  • Automated Leverage & Margin Setup (LeverageSetter)  │
└────────────────────────────────────────────────────────┘
```

---

## 🎛 Компоненты системы

### 1. Market Data & Decision Engine (`main.py`)
- **Топология:** `DiscoveryManager` запрашивает 24ч объемы, фильтрует монеты, строит маппинг `coin_to_native` и логирует число общих пар конкретно по каждой активной связке (`BINANCE_KUCOIN`, `BINANCE_BITGET`, `KUCOIN_BITGET`).
- **Сбор стаканов:** Поднимает 3 независимых пула публичных WebSocket-стримов.
- **Микросекундный пре-фильтр:** В главном цикле (задержка `0.005` с) обновляет `prices_array` и прогоняет его через `pre_calculate_orderbook` (Numba `@njit`), моментально ранжируя пары по величине спреда.
- **Оценка ликвидности:** Для топ-кандидатов вызывает `trading_engine.evaluate_entry`, рассчитывает VWAP с учетом дисконта глубины (`volatility_discount_entry`), проверяет обратное проскальзывание (`check_synthetic_exit`) и отправляет `CMD_OPEN` в экзекьютор.
- **Мониторинг позиций:** Отслеживает деградацию профита по кривой `profit_decay_map` и при достижении таргета шлет `CMD_CLOSE`.

### 2. Execution & Settlement Engine (`CORE/executor_process.py`)
- **Ордерные адаптеры (`API/orders.py`):** Унифицированные классы `BinanceOrder`, `KucoinOrder`, `BitgetOrder` с контролем шагов цены/лота (`round_by_step`), квантованием контрактов и множителей.
- **Политики исполнения ордеров (`order_execution_type`):**
  - **`IOC` (Immediate-Or-Cancel):** Ордер забирает только доступный объем в момент прихода, неналитый остаток аннулируется матчингом биржи.
  - **`LIMIT_GTC`:** Ордер встает агрессивной лимиткой в стакан на время `EXECUTION_PAUSE`, после чего бот вызывает `cancel_all_orders` по обеим ногам для снятия висящего остатка перед замером fill rate.
- **Защита от дедлоков (Deadlock-Free Locks):**
  - При ошибке валидации ордера или полном сбое входа шлет `POS_FAILED`, разблокируя биржу через `rollback_entry()`.
  - При аварийном выходе из-за частичного налива (`min_fill_rate`) автоматически производит откат незафиксированной позиции, исключая зависание замка `is_locked = True`.
- **Клиринг PnL (`API/settlement.py`):** Запрашивает фактические данные исполненных сделок, учитывает комиссии обеих бирж (с конвертацией BNB/KCS) и логирует результат в `total_balance.json`.
- **Настройка маржи и плечей (`CORE/leverage_setter.py`):** Автоматически устанавливает целевые плечи и режим кросс-маржи на всех трех биржах, кэшируя результаты в `CACHE/leverage_cache.json`.

### 3. Менеджер позиций (`CORE/position_manager.py`)
- **Инвариант биржи:** Гарантирует, что число активных и ожидающих сделок биржи (`current + pending`) строго не превышает лимит `max_positions` из конфига. При занятости биржи блокируются все связанные с ней связки.
- **Глобальный инвариант символа:** Одна и та же монета не может одновременно торговаться более чем на одной связке.
- **Стейт-машина:** Хранит статус `OPEN` / `CLOSE` / `None`, синхронизирует состояние в `active_positions.json`.

---

## 🔄 Жизненный цикл сделки v3.0

```
[Стаканы WS] ──> [pre_calculate_orderbook (JIT)]
                        │ (Top candidate spread > spread_entry)
                        ▼
             [trading_engine.evaluate_entry]
                        │ (VWAP OK, Synthetic Slippage OK)
                        ▼
             [PositionManager.can_enter]
              ├── Проверка лимита бирж (max_positions)
              └── Глобальная проверка символа по всем 3 связкам
                        │
                        ▼ (lock_for_entry: pending += 1)
             [main.py шлет CMD_OPEN через IPC]
                        │
                        ▼
             [executor_process.execute_open]
              ├── Pre-flight check_order_size
              │     └─ При ошибке: шлет POS_FAILED -> rollback_entry (снятие замка)
              ├── Параллельная отправка ордеров (IOC или LIMIT_GTC)
              ├── Ожидание EXECUTION_PAUSE
              ├── При LIMIT_GTC: отмена остатков через cancel_all_orders
              ├── Замер фактических позиций через WS/REST
              │
              ├── Перекос fill_rate < min_fill_rate?
              │     ├── 0.0 на обеих ногах -> шлет POS_FAILED -> rollback_entry
              │     └── Частичный налив одной ноги -> аварийный execute_close -> POS_CLOSED
              │
              └── Успешный вход -> confirm_entry -> шлет POS_OPENED
                        │
                        ▼
             [main.py ведет позицию по profit_decay_map]
                        │ (Профит пробил таргет или сработал TTL)
                        ▼
             [main.py шлет CMD_CLOSE через IPC]
                        │
                        ▼
             [executor_process.execute_close]
              ├── Параллельное закрытие обеих ног
              ├── Подтверждение закрытия -> шлет POS_CLOSED -> confirm_exit
              └── Фоновый клиринг PnL через ExchangeSettlement -> total_balance.json
```

---

## ⚙️ Сводка ключевых параметров [cfg.json]

| Параметр | Значение | Описание |
| :--- | :--- | :--- |
| `order_execution_type` | `"LIMIT_GTC"` / `"IOC"` | Политика ордеров: лимитки с авто-снятием остатка или биржевой IOC |
| `spread_entry` | `0.007` (0.70%) | Минимальный чистый спред для входа |
| `volatility_discount_entry` | `0.60` (60%) | Эффективная плотность стакана при расчете VWAP |
| `min_fill_rate` | `0.50` (50%) | Минимальный процент налива ноги до активации аварийного сброса |
| `EXECUTION_PAUSE` | `0.020` (20 мс) | Окно жизни лимитки в стакане и синхронизации позиций |
| `active_routes` | 3 активные | `BINANCE_KUCOIN`, `BINANCE_BITGET`, `KUCOIN_BITGET` |
| `max_positions` | `1` | Максимум 1 одновременная сделка на биржу суммарно |
