# DB Assistant — ассистент для работы с базами данных

Пример использования [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) совместно с Yandex AI Studio для построения мультиагентного ассистента анализа баз данных. Демонстрирует паттерн делегирования (Handoff): главный агент распределяет задачи между специализированными подагентами.

## Что умеет ассистент

- Преобразовывать вопросы на естественном языке в SQL-запросы, зная структуру схемы
- Анализировать метаданные таблиц: колонки, типы, описания, статистику, индексы
- Давать рекомендации по оптимизации SQL на основе структуры таблиц и плана выполнения
- Строить дерево внешних ключей для анализа связей между таблицами
- Проверять параметры СУБД и выдавать конкретные значения (без «проверьте сами»)
- Рекомендовать правила маскирования/анонимизации данных, опираясь на RAG-индекс в Yandex Vector Store
- Выполнять произвольные SQL-запросы в режиме read-only

## Архитектура

```
Браузер
  │  WebSocket
  ▼
flask_agent (Python / FastAPI + uvicorn, порт 8083)
  │  OpenAI Agents SDK (Yandex AI Studio / Responses API)
  │
  ├─ AssistantAgent         ← главный агент, принимает запросы пользователя
  │     │  handoff
  │     ├─ MetadataAgent    ← анализ схемы и SQL через MCP-сервер
  │     └─ DataMaskingAgent ← рекомендации по маскированию через Vector Store
  │
  ▼  SSE (MCP)
mcp_server (Go, порт 8081)
  │  HTTP REST
  ▼
metadata_server (Go, порт 8080)
  │  SQL
  ▼
СУБД (PostgreSQL / MySQL / Oracle)
```

Три микросервиса запускаются через Docker Compose в изолированной сети `mcp_network`.

## Поддерживаемые СУБД

| СУБД | Значение `MDATA_TYPE` |
|------|-----------------------|
| PostgreSQL / Yandex Managed PostgreSQL | `postgres` |
| MySQL / Yandex Managed MySQL | `mysql` |
| Oracle DB | `oracle` |

Сервис работает только с метаданными (`information_schema`, системные каталоги) — доступ к данным таблиц не требуется, кроме режима `run_wide_sql` (read-only транзакция).

Необходимые привилегии:
- **PostgreSQL** — доступ к `pg_catalog`, `information_schema`
- **MySQL** — доступ к `information_schema`, `performance_schema`
- **Oracle** — `SELECT` на `all_tables`, `all_tab_columns`, `all_indexes`, `all_ind_columns`, `all_constraints`, `all_cons_columns`, `v$parameter`

## Требования

- Docker и Docker Compose
- Сетевой доступ от хоста/VM до СУБД
- Аккаунт Yandex Cloud с доступом к AI Studio

## Быстрый старт

### 1. Склонировать репозиторий

```bash
git clone <repo-url>
cd db-assistant
```

### 2. Создать `.env` — подключение к СУБД

Файл используется сервисами `metadata_server` и `mcp_server`.

```env
# URL metadata_server внутри docker-compose сети (не менять)
META_URL=http://web:8080

# Параметры подключения к СУБД
MDATA_HOST=rc1b-xxx.mdb.yandexcloud.net
MDATA_PORT=5432
MDATA_USER=myuser
MDATA_PASS=mypassword
MDATA_TYPE=postgres        # postgres | mysql | oracle
MDATA_BASE=mydb

# Параметры MCP-сервера
MCP_PORT=8081
MSP_HOST=mcp               # имя контейнера (не менять при docker-compose)
SSE_MODE=true              # true — SSE для agents SDK; false — stdio

# Путь к CA-сертификату (только для TLS-подключения к MySQL)
# MDATA_CAPATH=/app/root.crt
```

**Описание параметров**

| Параметр | Описание | Пример |
|----------|----------|--------|
| `META_URL` | Внутренний URL metadata_server | `http://web:8080` |
| `MDATA_HOST` | Хост или FQDN базы данных | `rc1b-xxx.mdb.yandexcloud.net` |
| `MDATA_PORT` | Порт СУБД | `5432` (PG), `3306` (MySQL), `1521` (Oracle) |
| `MDATA_USER` | Пользователь БД | `myuser` |
| `MDATA_PASS` | Пароль | — |
| `MDATA_TYPE` | Тип СУБД | `postgres` / `mysql` / `oracle` |
| `MDATA_BASE` | Имя базы данных / SID | `mydb` |
| `MCP_PORT` | Порт MCP-сервера | `8081` |
| `MSP_HOST` | Имя хоста MCP-сервера в docker-сети | `mcp` |
| `SSE_MODE` | Режим MCP: SSE (`true`) или stdio (`false`) | `true` |
| `MDATA_CAPATH` | Путь к CA-сертификату внутри контейнера | `/app/root.crt` |

> Для Yandex Managed MySQL с TLS: скачайте `root.crt` и положите его в `/var/opt/root.crt` на хосте — он уже смонтирован в `docker-compose.yaml`.

### 3. Создать `.env.agent` — настройки агента

Файл монтируется в контейнер `flask_agent` как `/app/.env`.

```env
# Yandex Cloud — идентификатор каталога
YANDEX__FOLDER_ID=b1gxxxxxxxxxxxxxxxxx

# API-ключ Yandex AI Studio
YANDEX__AUTH=AQVN...

# Базовый URL Yandex AI Studio (не менять)
YANDEX__URL=https://ai.api.cloud.yandex.net/v1

# Модель — URI формата gpt://<folder_id>/<model>/latest
YANDEX__MODEL=gpt://b1gxxxxxxxxxxxxxxxxx/yandexgpt/latest

# URL MCP-сервера в docker-compose сети (не менять)
YANDEX__GET_INFO_MCP_URL=http://mcp:8081/sse

# Схема БД по умолчанию — агент загрузит её структуру при старте
YANDEX__METADATA_SCHEMA=public

# ID Vector Store в Yandex AI Studio для DataMaskingAgent
YANDEX__MASKING_INDEX_ID=fvt...

# Порт веб-интерфейса
YANDEX__PORT=8083

# Таймаут ожидания ответа от AI API (секунды)
YANDEX__WAIT_TIMEOUT=45

# Максимальное количество шагов агента за один запрос (tool calls + handoffs)
YANDEX__MAX_TURNS=25

# Имя лог-файла внутри контейнера
YANDEX__LOG_FILE_NAME=agent_main.log

# Системные инструкции агентов
YANDEX__ASSISTANT_INSTRUCTION=<промпт для AssistantAgent>
YANDEX__METADATA_INSTRUCTION=<промпт для MetadataAgent>
YANDEX__MASKING_INSTRUCTION=<промпт для DataMaskingAgent>

# Преднастроенные SQL-запросы для SQL-консоли (JSON-массив)
YANDEX__SQL_PRESETS=[{"description":"Список таблиц","sql":"SELECT table_name, table_type\nFROM information_schema.tables\nWHERE table_schema = 'public'\nORDER BY table_name"},{"description":"Размер таблиц","sql":"SELECT relname AS table, pg_size_pretty(pg_total_relation_size(oid)) AS size\nFROM pg_class WHERE relkind = 'r' ORDER BY pg_total_relation_size(oid) DESC LIMIT 20"}]
```

**Описание параметров**

| Параметр | Описание |
|----------|----------|
| `YANDEX__FOLDER_ID` | ID каталога Yandex Cloud (раздел «Обзор» консоли) |
| `YANDEX__AUTH` | API-ключ или IAM-токен Yandex AI Studio |
| `YANDEX__URL` | Endpoint Yandex AI Studio (OpenAI-совместимый) |
| `YANDEX__MODEL` | URI модели в формате `gpt://<folder_id>/<model>/latest` |
| `YANDEX__GET_INFO_MCP_URL` | SSE-эндпоинт MCP-сервера для MetadataAgent |
| `YANDEX__METADATA_SCHEMA` | Схема, которую агент читает при старте и кладёт в контекст |
| `YANDEX__MASKING_INDEX_ID` | ID Vector Store с документами по маскированию данных |
| `YANDEX__PORT` | Порт веб-приложения (8083 по умолчанию) |
| `YANDEX__WAIT_TIMEOUT` | Таймаут (сек.) на ожидание одного ответа от AI API |
| `YANDEX__MAX_TURNS` | Лимит шагов агента (tool calls + handoffs) за один запрос |
| `YANDEX__LOG_FILE_NAME` | Путь к лог-файлу внутри контейнера |
| `YANDEX__ASSISTANT_INSTRUCTION` | Системный промпт главного агента |
| `YANDEX__METADATA_INSTRUCTION` | Системный промпт MetadataAgent |
| `YANDEX__MASKING_INSTRUCTION` | Системный промпт DataMaskingAgent |
| `YANDEX__SQL_PRESETS` | JSON-массив преднастроенных SQL-запросов (см. ниже) |

> Настройки используют Pydantic `BaseSettings` с вложенным разделителем `__` — всё в разделе `YANDEX__*`. Подробнее: `flask_agent/utils/config.py`.

### 4. Собрать и запустить

```bash
# Собрать образы
docker-compose build

# Запустить все сервисы
docker-compose up -d

# Проверить состояние
docker-compose ps

# Следить за логами агента
docker-compose logs -f agent
```

При старте `flask_agent` автоматически считывает список таблиц из схемы `YANDEX__METADATA_SCHEMA` через MCP и сохраняет описание структуры в `AGENT.md`. Этот файл подставляется в контекст MetadataAgent — агент знает схему и пишет SQL без дополнительных вопросов пользователю.

### 5. Открыть веб-интерфейс

```
http://localhost:8083/
```

## Примеры запросов

### Оптимизация SQL

```
Помоги оптимизировать запрос:

SELECT s1.c, COUNT(1)
FROM public.sbtest s1
JOIN public.sbtest7 s7 USING (id)
GROUP BY s1.c
```

Агент проверит индексы на `id` и `c`, посмотрит статистику и предложит конкретные изменения.

### Структура таблицы и индексы

```
Покажи список полей таблицы orders
```

```
Какие индексы есть на таблице account?
```

### SQL по описанию

```
Напиши запрос для получения всех заказов пользователя за последний месяц
```

Агент использует схему из `AGENT.md` и предложит готовый SQL с правильными именами таблиц и квалификатором схемы.

### Связи между таблицами

```
Покажи дерево внешних ключей таблицы orders глубиной 2
```

### Маскирование данных

```
Предложи правила анонимизации таблицы customers для передачи разработчикам
```

DataMaskingAgent найдёт подходящие правила в Vector Store и процитирует источник.

### Параметры СУБД

```
Какое текущее значение параметра work_mem?
```

## REST API metadata_server

Сервис доступен на порту `8080`. MCP-сервер использует его автоматически; прямые запросы полезны для отладки.

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/healthz` | Проверка работоспособности |
| `POST` | `/connection/open` | Открыть соединение (form: `dbtype`, `dbhost`, `dbport`, `username`, `password`, `database`, `capath`) |
| `POST` | `/connection/close` | Закрыть соединение |
| `GET` | `/connection/status` | Статус соединения |
| `GET` | `/metadata/{schema}/{table}` | Метаданные таблицы (колонки, типы, описания) |
| `GET` | `/tables/{schema}/{table}` | Список таблиц по маске (`%`, `_` поддерживаются) |
| `GET` | `/stats/{schema}/{table}` | Статистика таблицы (число строк, дата анализа) |
| `GET` | `/indexes/{schema}/{table}` | Индексы таблицы |
| `GET` | `/parameter/{name}` | Значение параметра СУБД |
| `GET` | `/relationships/{schema}/{table}?depth=N` | Дерево FK-зависимостей (глубина 1–5, по умолчанию 5) |
| `POST` | `/sql` | Выполнить SQL в режиме read-only (form: `sql`) |

## MCP-инструменты

MetadataAgent использует эти инструменты через MCP-сервер (SSE, порт `8081`):

| Инструмент | Параметры | Описание |
|------------|-----------|----------|
| `get_metadata` | `schemaName`, `tableName` | Колонки, типы, комментарии |
| `get_table_list` | `schemaName`, `tableName` (маска) | Список таблиц |
| `get_statistics` | `schemaName`, `tableName` | Статистика таблицы |
| `get_indexes` | `schemaName`, `tableName` | Индексы |
| `get_db_parameters` | `parameterName` | Параметр СУБД |
| `get_relationships` | `schemaName`, `tableName`, `depth` (опц.) | Дерево FK-зависимостей |
| `run_wide_sql` | `sql` | Read-only SQL-запрос |

## Обновление после изменений кода

```bash
docker-compose build
docker-compose up -d
```

## Используемые сервисы Yandex Cloud

- **Yandex AI Studio** — модели YandexGPT, Responses API (OpenAI-совместимый), Vector Store для RAG
- **Yandex Managed Service for PostgreSQL / MySQL** — анализируемые СУБД (опционально)
- **Yandex Cloud Compute** — VM для запуска docker-compose (опционально)
