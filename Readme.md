Пример использования OpenAI Agents SDK для построения ассистента

В данном сценарии решается прикладная задача - создание ассистента-помощника работы с БД. демонстрируется сценарий делегирования (Handoff), который позволяет разделить агентов по решаемым задачам.
Два агента
### MetadataAgent
- анализ метаданных: таблицы, колонки, статистика, индексы. На основании этих данных может выдавать базовые рекомендации по оптимизации SQL запросов;
### DataMaskingAgent
- выдает рекомендации по маскированию данных исходя из описания таблиц, которые ищет в контексте диалога (agents.SQLiteSession). Выдает их на основании файлов, загруженных в Yandex AI Studio vector_store

Для работы нужно 
- склонировать репозиторий
- запустить сборку 
```
docker-compose build
```
- создать .env с описанием подключения к базе c примерным содержимым (пример подлкючения к Managed Service for MySQL)

```
META_URL=http://web:8080
MDATA_HOST=rc1d-06ojkdphd8s0lsld.mdb.yandexcloud.net
MDATA_PORT=3306
MDATA_USER=<user>
MDATA_PASS=<password>
MDATA_TYPE=mysql
MDATA_BASE=pol
MCP_PORT=8081
MSP_HOST=mcp
SSE_MODE=true
MDATA_CAPATH=/app/root.crt