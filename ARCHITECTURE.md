# ============================================================
# FILE: ARCHITECTURE.md
# ROLE: Architectural Blueprint and System Mental Map v11.0
# ============================================================

# 🧠 Архитектура HFT Спредера (Mental Map v11.0 - Single-Leg Lead-Lag Arbitrage with Orderbook Hunting & Strict Microstructure Protections)

> [!NOTE]
> **СТАТУС ТЕКУЩЕЙ АРХИТЕКТУРЫ: SINGLE_LEG_LEAD_LAG_ARBITRAGE (v11.0)**
> В версии **v11.0** система устраняет фундаментальные микроструктурные причины слива депозита на основе боевых принципов и референсной архитектуры `Rucheiok Bot 2.0`:
> 1. **Нейтрализация Мины №1 (Запрет Кейса В / Falling Knives):** Полный запрет входа в локальные проливы и ликвидации на ведомой бирже при стоячем Binance. Введен строгий режим `static_leg: "TARGET"` (Кейс Б: Оракул летит, Мишень стоит). Любые проявления Кейса В и Кейса А жестко блокируются (`CASE_C_REJECTED`, `CASE_A_REJECTED`).
> 2. **Нейтрализация Мины №2 (Органическая защита от широких стаканов через Synthetic Exit):** Вместо пложения лишних сущностей и искусственных порогов спреда стакана, защита от «дырявых» стаканов органически возложена на поджатый фильтр симуляции немедленного закрытия `synthetic_exit` (`max_slippage_ratio: 0.30` = не более 30% спреда на обратном выходе, `hard_max_slippage: 0.008` = 0.80%). Монеты с широким BBO и тонкой обратной книгой заявок автоматически отсекаются по проскальзыванию.
> 3. **Нейтрализация Мины №3 (4-стадийный Orderbook Hunting Exit):** Полный отказ от слепого рыночного сброса (15s Decay Map Market Dumping). Внедрен 4-стадийный алгоритмический хантер ликвидности стакана через `LIMIT_IOC` (`Base Virtual TP` $\to$ `Breakeven Stage` $\to$ `Progressive Extrime Step Close`). Выход по `MARKET` строго изолирован и разрешен исключительно для аварийного разворота поводыря (`ORACLE_REVERSAL_STOP`) и Hard SL.
> 4. **Прецизионное проскальзывание входа (`limit_slip_ratio: 0.0015` / 0.15%):** Аккуратный лимитный заброс цены ордера входа, исключающий забирание токсичных дальних уровней стакана.

---

## 🏛 Фундаментальные принципы микроструктуры (Microstructure Core)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          1. ОРАКУЛ-ПОВОДЫРЬ (ORACLE)                            │
│  Binance Futures (сверхбыстрый L2 WS, нулевой пинг в Токио).                    │
│  Торговые ключи отключены — биржа выступает чистым компасом истинной цены.      │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ Импульс цены на Binance (Lead-Lag)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                  2. СТРОГИЙ ДЕТЕКТОР СТОЯЧЕЙ МИШЕНИ (CASE B)                     │
│  • Quiescent Baseline: предсигнальный базис непрерывно следует за рынком.        │
│  • При расширении спреда: Target обязан стоять на месте (delta <= 0.0020).      │
│  • Если Target летит сам при стоячем Binance (Кейс В) -> МГНОВЕННЫЙ ОТБОЙ.       │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ Сигнал подтвержден
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│              3. СИМУЛЯЦИЯ ОБРАТНОГО ЗАКРЫТИЯ (SYNTHETIC EXIT 30% CAP)           │
│  • Симуляция мгновенного выхода встречным стаканом.                             │
│  • Total Slippage <= 30% от чистого спреда входа (max_slippage_ratio: 0.30).    │
│  • Отсекает монеты с дырявыми стаканами без лишних ручных порогов.               │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ Ликвидность подтверждена
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                 4. ОДНОНОГИЙ ВЫСТРЕЛ В МИШЕНЬ (SINGLE-LEG SHOT)                 │
│  • Расчет VWAP по размеру size_usd с дисконтом волатильности.                   │
│  • LIMIT_IOC на Target-биржу (Bitget / KuCoin / OKX) со slip_ratio 0.15%.       │
│  • 0% налив -> мгновенный ABORTED карантин. >0% налив -> ACTIVE позиция.        │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ Позиция в рынке
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                 5. 4-СТАДИЙНЫЙ ORDERBOOK HUNTING ВЫХОД (LIMIT_IOC)              │
│  [Стадия 1: Virtual TP Hunt]    -> Поиск максимальной плотности у/выше TP        │
│  [Стадия 2: Breakeven Hunt]     -> Защита комиссий при откате после профита     │
│  [Стадия 3: Progressive Extrime]-> Ступенчатый сбор стакана (0->1->2->3 тика)    │
│  [Стадия 4: Oracle Reversal SL] -> Мгновенный сброс ТОЛЬКО при развороте Binance │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏗 Топология двух процессов (2-Process IPC Architecture)

Для обхода ограничений GIL (Global Interpreter Lock), изоляции вычислительного цикла котировок от сетевых задержек ордеров и предотвращения задержек цикла система разделена на два изолированных процесса, общающихся через неблокирующий локальный TCP-сокет (`CORE/ipc_socket.py`):

```
┌────────────────────────────────────────────────────────┐
│      ПРОЦЕСС 1: Market Data & Decision Engine          │
│                    (main.py)                           │
│  • Public WS Streams (Binance, KuCoin, Bitget, OKX)    │
│  • Numba JIT Orderbook Scan (pre_calculate_orderbook)   │
│  • Top-3 Mid-Price Tracking (math_core.py)             │
│  • Quiescent Baseline Tracking (StaticDetector v11.0)  │
│  • Strict Case B Lead-Lag Enforcement (Ban Case C & A) │
│  • Deep VWAP & Spread Analysis (evaluate_entry_v9)     │
│  • Tight Synthetic Reverse Liquidity Check (30% net)   │
│  • Global Symbol & Exchange Locks (position_manager)   │
│  • Orderbook Hunting Decision Engine (evaluate_exit_v9)│
└─────────────────────────┬──────────────────────────────┘
                          │ Local Async TCP Socket (IPC)
                          │ [CMD_OPEN, CMD_CLOSE, INIT_TOPOLOGY]
                          │ [POS_OPENED, POS_CLOSED, POS_FAILED]
┌─────────────────────────▼──────────────────────────────┐
│       ПРОЦЕСС 2: Execution & Settlement Worker         │
│               (CORE/executor_process.py)               │
│  • Pre-warmed Persistent TCP/TLS REST Sessions         │
│    (Keepalive Loop каждые 45с с фейк-ордерами warmup)  │
│  • Position FSM (Reactive Single-Leg FSM v11.0)        │
│  • Reactive Event Bus (asyncio.Event per symbol/side)  │
│    [Zero OS Timer Sleep Jitter: FSM wakeup < 0.1 ms]   │
│  • Real-Time Private WS Position Streams (KuCoin,      │
│    Bitget) с контрактными множителями                  │
│  • Directional Safe Quantization:                      │
│      - BUY: ROUND_CEILING (запас для налива асков)     │
│      - SELL: ROUND_FLOOR (запас для налива бидов)      │
│      - QTY: ROUND_FLOOR + Epsilon 1e-8 (нет остатка)   │
│  • Single-Leg Target LIMIT_IOC Shot (slip: 0.15%)      │
│  • Fill Confirmation via WS (timeout 0.3s)             │
│  • Zero-Fill Fast Path (instant ABORTED quarantine)    │
│  • Orderbook Hunter Execution (LIMIT_IOC by target_p)  │
│  • Hard Protection (MARKET strictly on Oracle Reversal)│
│  • Instant 0ms PnL Calculation (analytics.py)          │
│  • Automated Margin & Leverage Setup (leverage_setter) │
└────────────────────────────────────────────────────────┘
```

---

## 🎛 Компоненты системы

### 1. Market Data & Decision Engine (`main.py`)
- **Топология инструментов (`discovery.py`):** Запрашивает 24ч объемы торгов (фильтр от $2M–$5M), отсекает неликвид, строит нормализованный кросс-биржевой маппинг `coin_to_native` для активных связок (`BINANCE_KUCOIN`, `BINANCE_BITGET`, `BINANCE_OKX`).
- **Сбор стаканов:** Поддерживает пулы WebSocket-стримов стаканов глубины (L2 Depth).
- **Микросекундный пре-фильтр (`CORE/math_core.py`):** Каждую итерацию без задержки обновляет 4-колоночную матрицу цен и объемов `prices_array` (`[ask_p, ask_usd, bid_p, bid_usd]`) и прогоняет ее через `pre_calculate_orderbook` (`Numba @njit`), мгновенно отсекая тонкие уровни (`min_top_depth_usd: 50.0`) и сортируя связки по величине спреда без аллокаций памяти Python.
- **Детектор Стояка (`StaticDetector` в `math_core.py`):**
  - Буферизирует средневзвешенную цену топ-3 уровней стакана: $P_{mid} = \frac{\frac{a_1+a_2+a_3}{3} + \frac{b_1+b_2+b_3}{3}}{2}$.
  - **Quiescent Baseline Tracking (Предсигнальный спокойный базис):**
    - Пока спред $< \text{pre\_filter\_spread}$ (движение «гуськом»), базис $(P_{\text{oracle}}^{\text{base}}, P_{\text{target}}^{\text{base}})$ непрерывно следует за рынком.
    - При расширении спреда $\ge \text{pre\_filter\_spread}$ базис фиксируется на время импульса.
  - **Строгая валидация Кейса Б (`static_leg: "TARGET"`):**
    - Замеряются относительные смещения: $\Delta_{\text{oracle}} = \frac{|P_{\text{oracle}} - P_{\text{oracle}}^{\text{base}}|}{P_{\text{oracle}}^{\text{base}}}$ и $\Delta_{\text{target}} = \frac{|P_{\text{target}} - P_{\text{target}}^{\text{base}}|}{P_{\text{target}}^{\text{base}}}$.
    - **Кейс Б (Lead-Lag Momentum):** $\Delta_{\text{target}} \le \text{max\_static\_leg\_ratio}$ и $\Delta_{\text{oracle}} > \text{max\_static\_leg\_ratio}$ $\implies$ `CASE_B_OK` (вход разрешен).
    - **Кейс В (Mean-Reversion / Knife Trap):** $\Delta_{\text{oracle}} \le \text{max\_static\_leg\_ratio}$ и $\Delta_{\text{target}} > \text{max\_static\_leg\_ratio}$ $\implies$ `CASE_C_REJECTED` (строго заблокирован!).
    - **Кейс А (Хаос):** Обе ноги сместились $> \text{max\_static\_leg\_ratio}$ $\implies$ `CASE_A_REJECTED` (строго заблокирован!).
  - **Защита от застрявшего спреда (`STALE_IMPULSE`):** При фиксации спреда дольше $1.5$ с сигнал признается устаревшим, исключая входы в старый зависший спред.
- **Глубокая оценка входа (`CORE/trading_engine.py` -> `evaluate_entry_v9`):** 
  - Находит первый квалифицированный уровень стакана с объемом `>= min_top_depth_usd` (устранение фронтран-мусора).
  - Рассчитывает взвешенные VWAP-цены входа для заданного объема `size_usd` с дисконтом глубины (`volatility_discount_entry`), вычитает суммарные комиссии обеих бирж.
  - Базис спреда рассчитывается относительно цены Short-ноги: `(short_vwap_bid - long_vwap_ask) / short_vwap_bid`.
  - **Синтетический выход (Synthetic Exit):** Симуляция немедленного закрытия встречными стаканами (`max_slippage_ratio = 0.30`, `hard_max_slippage = 0.008`). Органически отсекает неликвидные/широкие стаканы, не пропуская сделки с обратными потерями $>30\%$ от спреда.

---

### 2. Алгоритм выхода: Orderbook Hunting (`CORE/math_core.py` + `CORE/trading_engine.py`)

Выход из позиции построен на архитектуре хантера ликвидности из `Rucheiok Bot 2.0`:

```
                             [АКТИВНАЯ ПОЗИЦИЯ]
                                      │
           ┌──────────────────────────┴──────────────────────────┐
           │                                                     │
[Разворот Oracle к точке входа?]                         [Штатный мониторинг]
   delta_oracle >= 0.0020                                        │
           │                                                     ▼
           ▼                                      [Стадия 1: Base Virtual TP Hunt]
[ORACLE_REVERSAL_STOP]                            Virtual TP = Entry ± min_profit_ratio
   (Выход по MARKET)                              Поиск max_vol уровня у/выше TP в стакане
   Мгновенная защита капитала                     LIMIT_IOC по целевой цене target_price
                                                                 │
                                          ┌──────────────────────┴──────────────────────┐
                                     (Не исполнен)                                  (Откат после пика)
                                          │                                          P_max >= breakeven_act
                                          ▼                                             │
                          [Стадия 3: Progressive Extrime Close]                         ▼
                          Время удержания >= after_sec (60с)             [Стадия 2: Breakeven Stage]
                          Ступенчатый сдвиг по уровням стакана           BE Price = Entry ± roundtrip_fees
                          LIMIT_IOC по целевым уровням шага              LIMIT_IOC по BE цене
```

---

## ⚙️ Сводка ключевых параметров конфигурации (`cfg.json`)

| Блок / Фаза | Параметр | Значение | Описание |
| :--- | :--- | :--- | :--- |
| **Режим исполнения** | `order_execution_type` | `"PARALLEL_LIMIT_IOC"` | Одноногий выстрел в Target-биржу |
| **Сигнальные фильтры (Детектор Стояка)** | `signal_filters.static_detector.enabled` | `true` | Включение детектора стояка |
| | `signal_filters.static_detector.static_leg` | `"TARGET"` | **Строго Кейс Б:** Оракул летит, Target стоит. Кейс В заблокирован! |
| | `signal_filters.static_detector.max_static_leg_ratio` | `0.0020` (0.20%) | Предел смещения стоячей ноги от предсигнального базиса |
| | `signal_filters.static_detector.buffer_window_sec` | `0.35` (350 мс) | Окно предсигнального спокойного базиса |
| **Сигнальные фильтры (Спред и Синтетика)** | `spread_entry_pre` | `[0.006, 0.05] .. [0.008, 0.05]` | Диапазон префильтра спреда |
| | `spread_entry_base` | `0.006 .. 0.008` | Базовый требуемый чистый спред входа |
| | `min_top_depth_usd` | `$40.0 .. $50.0` | Минимальный объем (USD) на первом квалифицированном уровне |
| | `min_signal_dwell_ms` | `0` (мс) | Выдержка сигнала перед входом |
| | `top_n_candidates` | `4` | Количество лучших связок-кандидатов |
| | `max_desync_ms` | `BN_KU: 125, BN_BG: 200` (мс) | Допустимый рассинхрон получения стаканов между биржами |
| | `synthetic_exit.enabled` | `true` | Симуляция немедленного обратного закрытия |
| | `synthetic_exit.max_slippage_ratio` | `0.30` (30%) | **Поджатый лимит:** не отдавать обратно $>30\%$ спреда на стакане |
| | `synthetic_exit.hard_max_slippage` | `0.008` (0.80%) | Жесткий потолок обратного проскальзывания |
| **Вход Target** | `limit_slip_ratio` | `0.0015` (0.15%) | Допустимый заброс цены ордера входа LIMIT_IOC |
| | `fill_confirm_timeout_sec` | `0.3` (300 мс) | Таймаут ожидания подтверждения налива по WS |
| **Выход Target (Orderbook Hunting)** | `exit_order_type` | `"LIMIT_IOC"` | **Строго лимитный выход:** Запрет рыночного сброса на нормальных выходах |
| | `orderbook_hunting.enabled` | `true` | Включение 4-стадийного хантера ликвидности |
| | `orderbook_hunting.min_profit_ratio` | `0.0025` (0.25%) | Базовый целевой профит Virtual TP |
| | `orderbook_hunting.target_volume_usd` | `100.0` ($100) | Минимальный объем искомого пула ликвидности в стакане |
| | `orderbook_hunting.take_profit_slip_ratio` | `0.0005` (0.05%) | Допустимый сдвиг цены ордера LIMIT_IOC при охоте за TP |
| | `orderbook_hunting.breakeven_stage.enabled` | `true` | Включение стадии безубытка |
| | `orderbook_hunting.breakeven_stage.breakeven_activation_ratio` | `0.0015` (0.15%) | Порог набранного профита для активации безубытка при откате |
| | `orderbook_hunting.extrime_close.enabled` | `true` | Включение ступенчатого Extrime Close |
| | `orderbook_hunting.extrime_close.after_sec` | `60.0` (60 сек) | Задержка до включения ступенчатого сбора стакана |
| | `orderbook_hunting.extrime_close.steps` | `[0, 1, 2, 3]` | Уровни заглубления в стакан при сборе ликвидности |
| | `orderbook_hunting.extrime_close.step_spread_ratio` | `0.0002` (0.02%) | Шаг цены между ступенями |
| | `orderbook_hunting.oracle_reversal_stop.enabled` | `true` | Защита от разворота Оракула |
| | `orderbook_hunting.oracle_reversal_stop.oracle_reversal_ratio` | `0.0020` (0.20%) | Порог отката котировок Binance для аварийного MARKET сброса |
| **Карантины и баны** | `zero_fill` | `10` (сек) | Карантин при нулевом наливе ордера Target |
| | `entry_error` | `60` (сек) | Карантин при ошибке API / сети на входе |
| | `loss_trade` | `300` (сек) | Карантин при закрытии сделки с убытком |
| | `perm_ban_loss_ratio` | `0.0075` (0.75%) | Пожизненный перманентный бан монеты при убытке >= 0.75% |
| | `max_consecutive_losses` | `2` | Пожизненный бан при 2 убыточных сделках подряд |
| **Инварианты рисков** | `max_positions` | `1` | Максимум 1 активная позиция на Target-биржу |

---

## 🧪 Инструменты валидации и тесты (`live_tests/`)

1. **`live_tests/test_v11_orderbook_hunting.py`**:
   - Полный модульный и интеграционный валидатор архитектуры v11.
   - Тестирует Virtual TP расчет, поиск пулов ликвидности `find_liquidity_target`, расчет безубытка, шаги Extrime Close, строгое отклонение Кейса В (`CASE_C_REJECTED`) и принятие Кейса Б (`CASE_B_OK`), а также полный 4-стадийный цикл `evaluate_exit_v9`.
2. **`live_tests/test_impulse_detector.py`**:
   - Валидация детектора стояка с предсигнальным спокойным базисом (`Quiescent Baseline Tracking`).
3. **`live_tests/test_trading_engine.py` & `live_tests/test_all_local_pipeline.py`**:
   - Сквозные локальные тесты сквозного пайплайна генерации сигналов, эвалюации и исполнения.
4. **`live_tests/test_position_fsm.py` & `live_tests/test_fsm_transitions.py`**:
   - Валидация переходов состояний конечного автомата позиции (`OPENING` $\to$ `ACTIVE` $\to$ `CLOSING` $\to$ `CLOSED` / `ABORTED`).
