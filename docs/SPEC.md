# Recorder production-grade upgrade — Tardis-parity spec

## Context

Current recorder (`scripts/record_data.py` + `src/ws_client.py` + `src/recorder.py`) пишет 10 стримов в parquet на Contabo Tokyo (16 vCPU / 62 GB / 581 GB SSD). Латентность к Binance 2–5 мс — institutional territory по близости уже достигнута.

Цель: довести recorder до ~99% паритета с Tardis по **качеству real-time потока на одной паре/бирже**. Productization (SDK, download API, auth) и исторический архив 2017–2024 намеренно вне scope.

Пары, которые пишем сейчас: BTCUSDT + ETHUSDT depth L20 @100ms, agg trades; cross-exchange trades (Bybit/OKX/Bitget/Gate.io); funding markPrice; derivatives OI/long-short.

---

## 16 пунктов

### Stream quality

**1. Exchange timestamps `E` + `T` + `local_ts`**
Binance WS messages содержат `E` (event time) и depth/trade содержат `T` (transaction time). Сейчас пишем только local_ts. Хранить три колонки: `exchange_event_ts_us`, `exchange_trans_ts_us` (если есть), `local_ts_us`. Единица — микросекунды с epoch.

**2. Raw WS JSON архив (`.jsonl.gz`) параллельно parquet**
Для каждого стрима писать сырой JSON в append-only `.jsonl.gz` с таким же partitioning как parquet (часовые файлы). Цель: если в parsing logic обнаружен баг, пересчитать всё из raw без потери данных. Параллельная запись в recorder.py, ~20 строк. Компрессия gzip level 6 достаточна.

**3. Sequence-gap detection (`pu → u` для depth, trade id для trades)**
Binance depth diff имеет `pu` (previous update id) и `u` (last update id). Валидация: `msg.pu == prev.u`. При нарушении — писать explicit gap marker в метаданные, триггерить re-snapshot. Сейчас есть частичная реализация в `order_book.py`; нужно расширить на trades (aggregate trade id должен быть strictly increasing).

**4. Clock tuning (chrony + PTP если хостер даёт)**
Проверить активный NTP daemon (`chrony` или `systemd-timesyncd`). Настроить на Stratum-1 пулы (Google / Cloudflare / time.nist.gov). Если Contabo даёт PTP — включить. Цель: `local_ts` drift < 100 µs, а не 10 мс как у default NTP.

**5. Schema version в parquet metadata**
Добавить в каждый parquet файл pyarrow metadata dict: `{recorder_version, schema_version, binance_ws_api_version, recorded_at, host_id}`. Позволяет future-proof reparse когда Binance меняет поля (такое было несколько раз в их истории).

### Depth

**6. L100 snapshot @100ms**
Сейчас пишем L20. `src/order_book.py` уже держит full book state из snapshot + diff stream — меняется только константа `RECORD_LEVELS` в `recorder.py` и схема parquet на `FixedSizeList<f64, 200>` (100 bid + 100 ask × 2 для price/qty). REST snapshot через `/fapi/v1/depth?limit=1000`, WS через обычный `@depth` (incremental diff). Storage: ~15–20 GB/день/пара несжатые.

**7. `book_change` diff stream параллельно**
Каждый incremental WS update пишется как отдельная строка: `{local_ts, exchange_ts, side, price, qty, update_type}` где `update_type ∈ {insert, update, remove}` (remove = qty=0 в Binance WS). Это то, что даёт Tardis в `book_change_*.csv`. Storage ~5 GB/день/пара. Позволяет клиенту пересобрать любую глубину.

### Redundancy / integrity

**8. Второй recorder на отдельном хосте → merge on ingest**
Поднять второй экземпляр в другой локации (другой Contabo VPS или Oracle Free Tier Tokyo). Оба пишут в свой локальный storage. Nightly merge job дедуплицирует по `(exchange_ts, sequence_id)` и заливает в единый архив. Gap в одном recorder'е покрывается вторым → zero-gap guarantee на уровне архива.

**9. Multiple WS endpoints + dedupe**
Binance даёт `wss://fstream.binance.com` и `wss://ws-fapi.binance.com` (и региональные варианты). Подключаться к обоим из одного recorder'а, дедуплицировать по `(stream, exchange_ts, u)`. Защищает от outage одного endpoint'а.

**10. Orderbook checkpoint full state каждые 5 мин**
Сериализовать весь maintained book state (все 1000 levels, не top-20) в parquet каждые 5 минут. Цель: быстрый replay — вместо reconstruct с начала дня, скачать checkpoint + diff с момента checkpoint'а. Plus integrity verification против REST snapshot.

**11. Periodic REST snapshot vs maintained state**
Раз в 15 минут делать `/fapi/v1/depth?limit=1000` и сравнивать с нашим maintained state. Любое расхождение — писать alert + дамп обоих для post-mortem. Детектит silent drift который sequence validation не ловит (race conditions в recorder'е, logic bugs).

**12. Trade → depth correlation cross-check**
Для каждого aggTrade с `T=exchange_ts` должен быть depth update в окне [T-100ms, T+100ms] с соответствующим изменением qty на price level трейда. Несоответствие — flag cross-stream gap. Runs как async post-processor, не блокирует запись.

### Coverage

**13. Liquidations stream (`@forceOrder`)**
Добавить subscription на `btcusdt@forceOrder` и `ethusdt@forceOrder`. Писать в отдельный стрим `liquidations/`. Это то, что у Tardis есть, у нас нет. Схема: `{local_ts, exchange_ts, symbol, side, qty, price, order_type, last_fill_qty, filled_qty, avg_price}`. Binance docs: https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams

**14. Funding settlement events**
Сейчас пишем `markPrice` (каждую секунду). Добавить capture actual funding settlement — это происходит каждые 8 часов (00:00, 08:00, 16:00 UTC). Binance WS `markPrice@1s` содержит `r` (funding rate) и `T` (next funding time); при смене `T` — значит settlement произошёл. Хранить отдельный стрим `funding_settlements/` с realized funding per position.

**15. Per-exchange quirks handling**
Каждая биржа имеет свои нюансы:
- **Bybit**: ping/pong каждые 20 сек иначе разрыв; sequence `seq` поле в отдельном формате
- **OKX**: требует pong на ping каждые 30 сек; одна подписка = один WS
- **Coinbase**: Level 3 full-book vs Level 2 snapshot, JWT auth для авторизованных стримов
- **Deribit**: JSON-RPC вместо plain streaming
- **dYdX**: gRPC/WebSocket, нужна нативная библиотека

Каждая биржа — отдельный handler в `ws_client.py`, ~50–100 строк.

**16. Normalized cross-exchange schema**
Layer поверх per-exchange recorder'ов: каждая строка в output — `{exchange, symbol, instrument_type, side, price, amount, local_ts_us, exchange_ts_us, event_type}`. Единый формат для всех бирж. Реализация: post-processor после основной записи, или embed в `recorder.py` как `normalize(msg)` функция per-exchange.

---

## Вне scope

- **Productization**: download API, SDK (Python/TS/Go), auth, rate limiting, billing — отдельный сервисный слой поверх recorder'а
- **Исторический архив 2017–2024** — физически недостижим
- **Compliance / legal** — разрешения на перераспределение public market data с exchange'ей

---

## Связанные файлы кода

- `scripts/record_data.py` — entrypoint, systemd service
- `src/ws_client.py` — WS подключения к Binance + cross-exchange, watchdog, reconnect
- `src/order_book.py` — maintained state, snapshot/diff apply
- `src/recorder.py` — parquet writers per stream
- `rust_ingest/` — Rust feature builder, не трогается этим upgrade'ом
- `src/config.py` — WS URLs, символы, константы
- `systemd/scalper-recorder.service` — juice unit, `Type=simple`, `Restart=always`

## Текущее состояние

- Коммит `f84e8fb` (2026-04-18) исправил critical watchdog false-positive (18k disconnects/сутки → 0) и listenKey 401 spam. Recorder теперь работает стабильно, uptime с рестарта, все 10 стримов идут.
- Тесты: `tests/test_cache_validation.py`, `tests/test_features_horizon*.py` покрывают downstream feature pipeline — не сам recorder. Тесты на recorder integrity отсутствуют, добавление — часть пунктов 11/12.
