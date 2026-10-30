# Hadoop Result Writer — прототип

Отдельный Python-пакет для сравнения двух способов записи результата расчёта.
Основной `adaptation-ovation` не изменяется.

| Режим | Кто записывает данные | Как фиксируется результат |
| --- | --- | --- |
| `hdfs` | Python создаёт Parquet, Hadoop CLI загружает в HDFS | атомарная публикация commit-файла |
| `trino` | Python передаёт порции SQL INSERT, сервер Trino пишет Parquet в HDFS | публикация имени подготовленной таблицы через Hive Metastore |

Оба режима получают TGT из keytab и поддерживают его действующим. HDFS-режим не подключается
к Trino. SQL-режим не запускает Hadoop CLI. Один `result_id` — один неизменяемый результат;
в SQL-режиме создаётся **отдельная итоговая таблица на каждый результат**. Это выбранная
граница прототипа, не замена строк в общей существующей таблице.

## Что подготовить для работы на кластере

Пакет — клиент существующей инфраструктуры: production KDC, HDFS, Hive Metastore
и Trino он не разворачивает. До подключения нужно получить от администраторов:

1. **Для обоих режимов:** служебный `principal` с realm, соответствующий `keytab`,
   `krb5.conf`, адреса KDC и согласованный срок TGT. Нужны рабочий DNS и синхронизация
   часов узла. Согласовать ротацию keytab и отображение principal в пользователя HDFS/Trino;
   пароль пользователя вместо keytab текущий API не принимает.
2. **Для `mode=hdfs`:** клиентские `core-site.xml`, `hdfs-site.xml`, `log4j.properties`,
   URI `hdfs_root` и заранее выделенный корень записи. Нужны права обхода родительских
   каталогов, list/read, mkdir/create и rename внутри корня, достаточные HDFS-квоты.
   Staging, results и commits должны находиться в одной файловой системе/encryption zone.
3. **Для `mode=trino`:** FQDN и HTTPS-порт coordinator (`trino_host`, `trino_port`),
   имя Kerberos-сервиса (`trino_service`), разрешённый `trino_user`, доверенный CA
   (`ca_bundle`, если CA не публичный), существующие Hive `catalog` и `schema`,
   префикс итоговых таблиц `table` и HDFS-корень для external location (`hdfs_root`).
   Администратор должен разрешить внешние Parquet-таблицы и запись в них, выдать SQL-права
   и настроить доступ **серверов Trino** к HDFS и Hive Metastore, включая их Kerberos.
   Тикет Python-клиента сам по себе эту серверную настройку не заменяет.
4. **Для надёжности:** согласовать репликацию, minimum replication, NameNode HA,
   disk sync при закрытии файлов и условия восстановления после отказа. Отдельно проверить
   no-replace rename в выбранном канале. Commit/receipt — протокол публикации этого пакета,
   а не обещание полной эквивалентности PostgreSQL COMMIT.
5. **Для приложения:** определить стабильный `result_id`, Arrow-схему и способ чтения
   receipt потребителем. Для прямого HDFS согласовать отдельный шаг регистрации/доступа
   в Hive/Trino: пакет его пока не выполняет. Для SQL потребитель читает возвращённую
   итоговую таблицу. Для данных 1–5 ГБ предпочтителен поток порций; готовый DataFrame
   уже занимает память целиком.
6. **Для окружения:** установить зависимости из раздела ниже; предоставить writable
   `cache_dir` пользователю процесса (в контейнере UID/GID 10001) и память под порции/буферы.
   Только в HDFS-режиме дополнительно нужны writable `scratch_dir` и место под текущий
   Parquet-файл с запасом; `target_file_bytes` не является жёстким лимитом сжатого файла.
   Если scratch размещён в tmpfs, учитывать его в лимите памяти контейнера.
   SQL-режим временные Parquet-файлы на стороне Python не создаёт.
   Для CLI `publish --input` входной Parquet тоже должен быть доступен внутри контейнера;
   keytab и конфигурации монтировать read-only.

### Какие сетевые соединения нужны

| Откуда | Куда | Когда |
| --- | --- | --- |
| Узел/контейнер Python | DNS и KDC | Оба режима: имена сервисов, получение и повторный запрос TGT |
| Python с Hadoop CLI | NameNode RPC и DataNode data transfer; KMS при шифровании | Только `hdfs`; одного доступа к NameNode недостаточно |
| Python | HTTPS coordinator Trino | Только `trino`; прямой доступ Python к HDFS/Metastore не нужен |
| Службы Trino | Hive Metastore, HDFS, KDC; KMS при необходимости | Серверная часть `trino`, настраивается владельцем кластера |

Точные адреса, HA nameservice и порты берутся из конфигурации вашего кластера,
не из условных значений `examples/*.json`. SQL-клиент использует `trust_env=False`:
на `HTTPS_PROXY` и `REQUESTS_CA_BUNDLE` полагаться нельзя; нужен прямой маршрут и,
при собственном CA, явный `ca_bundle`.

Порядок запуска: заполнить [HDFS-конфиг](examples/config.json) или
[Trino-конфиг](examples/config-trino.json) → `diagnose` → небольшая `sample` →
`lookup` и повтор с тем же `result_id` → приёмка отказов и объёма 1–5 ГБ.
`diagnose` не доказывает возможность записи: для SQL он выполняет только `SELECT 1`,
не проверяя доступ к catalog/Metastore/HDFS. В [Compose-примере](examples/compose.yaml)
по умолчанию также запускается только `diagnose`; входной файл и команда публикации
добавляются отдельно. Этот общий Compose требует `HRW_HADOOP_CONF` даже в SQL-режиме,
хотя сам SQL-клиент Hadoop-конфигурацию не читает.

## Пакеты и зависимости

Python 3.12+; контейнер использует Python 3.13. Прямые зависимости задаются в
[`pyproject.toml`](pyproject.toml), точные версии и хеши — в [`uv.lock`](uv.lock).
Ниже указаны версии из текущего lock-файла, не рекомендации «последних» версий.

| Python-пакет | Ограничение | Зафиксированная версия | Для чего |
| --- | --- | --- | --- |
| `pyarrow` | `>=21,<24` | `23.0.1` | Arrow-схемы, преобразование входа, запись Parquet |
| `trino[gssapi]` | `>=0.336,<0.340` | `0.339.0` | SQL-клиент Trino с Kerberos/GSSAPI |
| `pandas` | `>=2.2,<4` | `3.1.0` | Необязательный extra `pandas`; также входит в dev-окружение и контейнер |
| `pytest` | `>=8,<10` | `9.2.0` | Только разработка: тесты |
| `ruff` | `>=0.12,<1` | `0.17.3` | Только разработка: линтер и форматирование |

Транзитивно устанавливаются `krb5==0.5.1`, `requests-gssapi==1.4.0`, `gssapi==1.12.0`
для аутентификации; с pandas — `numpy==2.5.4`. Остальные зависимости клиента и инструментов:
`certifi`, `charset-normalizer`, `decorator`, `idna`, `iniconfig`, `lz4`, `orjson`,
`packaging`, `pluggy`, `pygments`, `python-dateutil`, `pytz`, `requests`, `six`,
`tzdata`, `tzlocal`, `urllib3`, `zstandard`. Их версии также фиксирует lock-файл;
устанавливать их по одному не нужно. Полное дерево текущего окружения:

```bash
uv tree --locked --offline
# Runtime без pandas и dev-инструментов:
uv sync --locked --no-dev
# Runtime с DataFrame:
uv sync --locked --no-dev --extra pandas
# Разработка и тесты (включая pandas):
uv sync --locked
```

Сборка wheel использует `hatchling>=1.27` из build-system (отдельное изолированное
окружение сборки, не зафиксирован в `uv.lock`). В Docker builder используется `uv==0.11.23`.

Системные зависимости в Debian Bookworm-образе:

| Где | Пакеты / компоненты | Назначение |
| --- | --- | --- |
| Runtime | `krb5-user`, `libgssapi-krb5-2` | `kinit`, `klist`, нативная библиотека GSSAPI |
| Runtime | `openjdk-17-jre-headless`, Hadoop `3.4.2` | Java и `hdfs` CLI; Hadoop скачивается отдельно с проверкой SHA-512 |
| Общий слой образа | `ca-certificates`, `curl` | Сертификаты TLS и загрузка Hadoop; остаются в итоговом образе |
| Только builder | `gcc`, `libkrb5-dev` | Сборка нативных Python-модулей Kerberos; в runtime не копируются |
| Только integration-стенд | `krb5-kdc`, `krb5-admin-server`, `openssl` | Тестовый KDC и сертификаты; не нужны production-клиенту |

Источник системного списка — [`Dockerfile`](Dockerfile) и
[`integration/Dockerfile`](integration/Dockerfile). APT сам устанавливает их транзитивные
зависимости. Hadoop/JRE нужны прямому HDFS-режиму; SQL-режим ими не пользуется,
но общий образ содержит оба клиента. Hive Metastore и сервер Trino — внешние сервисы,
а не Python-пакеты этого приложения. PySpark для этого прототипа не требуется.
Версии APT-пакетов и digest базового образа пока не закреплены: пересборка может
получить системные обновления, даже при неизменном Python lock-файле.

## Быстро посмотреть без кластера

Из этого каталога, Python 3.12+ и uv:

```bash
uv sync --locked
uv run hadoop-result-writer --help
uv run hadoop-result-writer demo --directory /tmp/hrw-demo --mib 32
# Повтор вернёт тот же опубликованный путь и reused=true:
uv run hadoop-result-writer demo --directory /tmp/hrw-demo --mib 32
make check
```

Демонстрация создаёт настоящие Parquet-файлы и проверяет протокол фиксации на локальной
файловой системе. Она не проверяет HDFS-репликацию, KDC, ACL или SQL Trino.

## Python API

```python
import pyarrow.parquet as pq
from hadoop_result_writer import Config, Connection

config = Config.load("config.json")  # mode: hdfs или trino
with Connection(config) as connection:
    # После перезапуска сначала проверяем фиксацию, не вычисляем гигабайты повторно.
    receipt = connection.writer.lookup("calculation-42")
    if receipt is None:
        batches = pq.ParquetFile("result.parquet").iter_batches(batch_size=8192)
        receipt = connection.writer.publish("calculation-42", batches)
    print(receipt)
```

Вместо локального входного файла можно передать ленивый итератор `pyarrow.RecordBatch`
из расчёта. Пакет не собирает весь результат в памяти. Размер входной порции контролирует
вызывающая сторона; чрезмерно большая порция нарушит желаемое ограничение памяти.
Схема всех порций должна совпадать. Пустые результаты первой версией отклоняются.

### pandas DataFrame и явные типы

Установка с поддержкой pandas: `uv sync --locked --extra pandas`, либо после `uv build`
`pip install './dist/hadoop_result_writer-0.1.0-py3-none-any.whl[pandas]'`.
Для Arrow-only приложения pandas не требуется.

```python
import pandas as pd
import pyarrow as pa
from hadoop_result_writer import Config, Connection

df = pd.DataFrame({"account_id": [101, 102], "amount": [12.5, None]})
schema = pa.schema(
    [
        pa.field("account_id", pa.int64(), nullable=False),
        pa.field("amount", pa.float64()),
    ]
)
with Connection(Config.load("config.json")) as connection:
    receipt = connection.writer.publish(
        "calculation-43",
        df,
        schema=schema,
        batch_size=8192,
    )
    print(receipt)
```

Один API работает для `mode=hdfs` и `mode=trino`. `schema` необязательна для DataFrame:
типы выводит PyArrow из всего входа, преобразование выполняется порциями по `batch_size`
строк. Индекс pandas не сохраняется; если он нужен, сначала вызвать `df.reset_index()`.
Названия и порядок колонок должны совпадать со схемой, лишние колонки не теряются молча.
Небезопасные преобразования (например, переполнение int32) отклоняются.

Для `object`-колонок, полностью заполненных `None`, задавать тип явно: по данным нельзя
узнать, подразумевались строка, число или дата. Nullable integer в pandas удобно задавать
через `dtype="Int64"`. Неявного преобразования всех значений в строки нет.
В SQL-режиме действуют ограничения типов, перечисленные ниже: произвольная Arrow-схема
сама по себе не добавляет поддержку нового SQL-типа.

Готовый DataFrame уже целиком находится в памяти. При явной `schema` пакет преобразует
только текущую порцию, без второй полной Arrow-копии. Без схемы вывод типов PyArrow может
создавать временные массивы целых колонок: для 1–5 ГБ задавать схему явно.
Сами исходные данные и текущая конвертация всё равно занимают память. `batch_size` —
число строк, не лимит байтов: большие строки требуют меньшего значения.

### Python-данные и потоковый вход

Поддерживаются `pyarrow.Table`, один `pyarrow.RecordBatch`, итератор RecordBatch и
итератор словарей Python. Для словарей схема обязательна, ключи должны в точности совпадать
с колонками. Пример не собирает весь расчёт в список:

```python
schema = pa.schema([("account_id", pa.int64()), ("amount", pa.float64())])
rows = ({"account_id": i, "amount": i * 0.5} for i in range(1_000_000))
with Connection(Config.load("config.json")) as connection:
    receipt = connection.writer.publish("calculation-44", rows, schema=schema)
```

Для dict-of-columns использовать `pa.table({"account_id": [1, 2]}, schema=...)`,
для списка dict — тот же `publish` со схемой. Пользовательские объекты/dataclass нужно
явно преобразовать в словари; произвольные объекты автоматически не сериализуются.
Общий конвертер доступен отдельно: `from hadoop_result_writer import as_batches`.
Собственной системы типов нет — используются стандартные `pa.int64()`, `pa.string()`,
`pa.date32()` и другие [типы и преобразования PyArrow](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).

## Kerberos и подключение

Нужны служебный principal, keytab, конфигурация Kerberos и сетевой доступ к KDC.
Для HDFS нужны клиентские `core-site.xml`/`hdfs-site.xml`, доступ к NameNode/DataNode,
созданный writer root и права на создание/rename. DNS, realm и время должны быть согласованы.
В `HADOOP_CONF_DIR` нужен также штатный `log4j.properties` с console target `System.err`:
логи не должны попадать в stdout команд `-stat`/`-cat`, где передаются данные.
Можно использовать файл из `$HADOOP_HOME/etc/hadoop/log4j.properties`.

```bash
export KRB5_CONFIG=/absolute/path/krb5.conf
export HADOOP_OPTS="${HADOOP_OPTS:-} -Djava.security.krb5.conf=$KRB5_CONFIG"
export HADOOP_CONF_DIR=/absolute/path/hadoop-conf
export JAVA_HOME=/absolute/path/java
export HADOOP_HOME=/absolute/path/hadoop
export PATH="$HADOOP_HOME/bin:$PATH"
uv run hadoop-result-writer diagnose --config /absolute/path/config.json
```

Примеры: `examples/config.json` — HDFS; `examples/config-trino.json` — SQL.
Для локального запуска заменить `/run/secrets/writer.keytab` абсолютным путём к keytab.
Keytab не копировать в репозиторий. Созданные кеши имеют права 0600 внутри каталога 0700
и удаляются при нормальном закрытии контекста. `KRB5CCNAME` основного процесса не меняется.
Каждый контекст использует свой кеш; `KRB5_CONFIG` задаётся для процесса до запуска Python.
Для Java/Hadoop нестандартный путь к krb5.conf нужно также указать через
`-Djava.security.krb5.conf` (как выше; пример предполагает путь без пробелов).
В контейнере используется стандартный `/etc/krb5.conf`, дополнительная опция не нужна.
Порядок поиска описан в [документации Java Kerberos](https://docs.oracle.com/en/java/javase/17/security/kerberos-requirements.html).

TGT запрашивается на один час (KDC может выдать меньший срок), повторный `kinit` выполняется
по `refresh_seconds`. Перед операциями выполняется проверка `klist -s`. Интервал обновления
должен быть меньше фактического срока тикета с запасом на долгую запись файла.
При отказе обновления новые операции блокируются; фоновая задача продолжает попытки.
Trino создаёт новые GSSAPI credentials из актуального кеша на каждый SQL-запрос.
Для длительного Hadoop subprocess credentials могут потребовать повторного запуска попытки
после авторизационной ошибки; незавершённая попытка не считается опубликованной.

`diagnose` — read-only проверка связи: она не доказывает наличие прав на запись/rename.
Кластерные команды не возвращают сырой stderr/SQL в CLI-ошибках, чтобы не раскрывать секреты.

## Прямая запись HDFS

```bash
uv run hadoop-result-writer sample --config config.json --result-id calc-1 --mib 32
uv run hadoop-result-writer publish --config config.json --result-id calc-2 --input result.parquet
uv run hadoop-result-writer lookup --config config.json --result-id calc-2
```

Порядок: запись и закрытие очередного Parquet → загрузка и проверка размера → manifest →
rename уникального каталога → rename commit-файла без перезаписи → readback.

```text
<root>/staging/<attempt-uuid>/            незавершённая подготовка
<root>/results/<attempt-uuid>/            закрытые файлы и _manifest.json
<root>/_commits/<result_id>.json          указатель на подтверждённый результат
```

**Потребитель обязан читать commit/receipt**, а не все каталоги под `results/`:
там сохраняются проигравшие и прерванные попытки. Этот commit не регистрирует Hive-таблицу
и не гарантирует автоматическую видимость результата в Trino. Подключение потребителя
описано в разделе «Как сделать данные видимыми в Hive/Trino» ниже; пакет этот шаг
пока не выполняет. Rename должен выполняться в одной HDFS/encryption zone.
Внутри результата читать только Parquet-файлы, перечисленные в manifest.

По умолчанию файлы ротируются примерно каждые 256 МиБ несжатых входных данных.
После сжатия размер другой. На локальном диске хранится текущий файл, в памяти — порция
и буферы Arrow. Manifest хранит SHA-256, размеры, строки и fingerprint схемы.
Lookup проверяет manifest digest и размеры файлов; он не перечитывает все гигабайты для
проверки SHA-256. Контроль передачи выполняет HDFS, полноценная независимая проверка
содержимого может использовать записанные SHA-256.

Фиксация имени и сохранность при отключении питания — разные свойства. Hadoop по умолчанию
не требует немедленного disk sync при закрытии файла. Если нужен аналог подтверждённой
устойчивой записи PostgreSQL, с администраторами нужно согласовать
`dfs.datanode.synconclose=true`, репликацию, minimum replication и отказоустойчивость NameNode.
Python-клиент не может включить серверный disk sync своей настройкой. См.
[гарантии Hadoop OutputStream 3.4.2](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/filesystem/outputstream.html).

Физический manifest должен совпадать при повторном publish. Другой размер порций, версия
Arrow/Parquet или компрессия могут дать конфликт для логически тех же строк. После падения
сначала использовать lookup. До commit может остаться незавершённый каталог; автоматического
удаления таких каталогов в прототипе нет.

## Запись через Trino

Настроить `mode=trino`, HTTPS coordinator, CA при необходимости, GSSAPI service name
(часто `HTTP`, зависит от кластера) и отдельную существующую тестовую Hive-схему.
Права: CREATE внешних Parquet-таблиц, INSERT, SELECT, COMMENT, ALTER TABLE RENAME;
служба Trino должна иметь доступ к HDFS root. SQL-режим не использует register_partition.

Для lookup нужны также видимость `information_schema.tables`, чтение
`system.metadata.table_comments` и `SELECT` с `count(*)`/скрытой колонкой `"$path"`.
В Hive-каталоге администратор должен разрешить `hive.non-managed-table-creates-enabled`
и `hive.non-managed-table-writes-enabled`: в актуальной
[документации Hive connector](https://trino.io/docs/current/connector/hive.html)
запись во внешние таблицы по умолчанию выключена.
Это **серверные** свойства, не поля JSON-конфига Python.
Сверять названия и поддержку с версией вашего Trino.

Команды те же, но с `--config examples/config-trino.json` после замены параметров.
Таблица попытки `_hrw_<uuid>` создаётся в тестовой схеме и наполняется SQL INSERT.
После проверки числа строк метаданные результата сохраняются в comment, таблица
переименовывается в `<table-prefix>_<hash(result_id)>`; полное имя возвращается в receipt.
SQL-потребители читают только итоговые имена. Промежуточные таблицы технически видны
пользователям с правами на схему: отдельная схема/ACL обязательны для изоляции.

**Это функциональный SQL-прототип, не рекомендуемый пока канал загрузки 5 ГБ.**
Значения передаются INSERT-пакетами примерно до 128 КиБ (одна строка больше лимита
отклоняется); это много запросов и потенциально маленьких файлов. Для производительной
загрузки может понадобиться staging + INSERT SELECT/CTAS. Поддерживаются int32/int64,
float32/float64 без NaN/Inf, bool, string, binary, date32. Время, decimal и вложенные типы
не реализованы. Fingerprint зависит от схемы, порядка и значений строк, но не от порций.

Каждый INSERT коммитится отдельно в таблицу попытки; общей многооператорной SQL-транзакции
здесь нет. Неопределённый INSERT не повторяется: вся попытка оставляется для диагностики,
повтор создаёт новую. Финальная гарантия требует атомарного no-replace rename таблицы
в вашем Trino/Hive Metastore. **До реального приёмочного теста эта предпосылка не доказана.**

## Как сделать данные видимыми в Hive/Trino

Файлы в HDFS сами по себе не становятся SQL-таблицей. Нужны метаданные колонок, формата
и расположения в **Hive Metastore (HMS)**, к которому подключён нужный Hive-каталог Trino,
а также права потребителя. Регистрация не копирует Parquet и не выдаёт права автоматически.

### Что определить до первого расчёта

Заранее согласовать с владельцами кластера:

- **Область хранения HDFS:** выделенный `hdfs_root`, квоты, ACL и доступ читателей.
  Именно этот конфиг определяет, куда Python-писатель загрузит файлы.
- **Hive-базу:** существующая или заранее созданная база `writer_demo` в нужном HMS.
  При необходимости ей задаётся LOCATION по умолчанию; это не настройка Python-писателя.
- **Структуру данных:** имена колонок, Arrow-типы и соответствующие Hive/Trino-типы,
  допустимость NULL. Передавать явную `pa.schema` из примера Python API и использовать
  тот же контракт при создании SQL-таблицы; не рассчитывать на вывод Hive-схемы из файлов.
- **Формат:** external table с `STORED AS PARQUET` в Hive или `format='PARQUET'` в Trino.
  Для HDFS-режима Python уже пишет Parquet с ZSTD; версию logical types (`2.6`) и data pages
  (`1.0`) он наследует от PyArrow 23.0.1, отдельно не задаёт. Согласовать поддержку типов
  и кодека читателями ([параметры ParquetWriter](https://arrow.apache.org/docs/23.0/python/generated/pyarrow.parquet.ParquetWriter.html)).
  В SQL-режиме параметры создаваемых файлов определяются сервером Trino.

Например, предварительное создание базы через Hive SQL-клиент:

```sql
CREATE DATABASE writer_demo
LOCATION 'hdfs://nameservice1/apps/hive/writer_demo.db';
```

Это условный путь, который должен выделить администратор. Для существующей базы проверить
`DESCRIBE DATABASE EXTENDED writer_demo`, а не создавать или менять её вслепую.
**LOCATION базы — путь по умолчанию, LOCATION внешней таблицы — фактический путь её данных.**
Явный LOCATION таблицы может отличаться от LOCATION базы; он должен быть разрешён политиками
кластера ([Hive: базы и расположение](https://hive.apache.org/docs/latest/language/languagemanual-ddl/#create-database)).
В нашем прототипе HDFS-файлы появляются под `hdfs_root/results/<attempt-uuid>`, поэтому
итоговую таблицу регистрируем после commit с точным `receipt.location`, как показано ниже.
Саму структуру и формат согласуем заранее. Создавать базу пакет не умеет; поле `schema`
в его конфиге используется только в SQL-режиме и не создаёт Hive-базу для HDFS-режима.

### После записи через Trino

При `mode=trino` успешный publish уже создаёт и регистрирует итоговую таблицу. Передать
потребителю полное имя из **`receipt.table`** и дать ему SELECT на эту таблицу средствами
авторизации кластера. Не вычислять имя самостоятельно и не читать таблицы `_hrw_*`.
В Hive это та же таблица `<schema>.<table>`, если Hive и Trino используют один HMS.
Не менять её служебный comment: он нужен пакету для lookup.
При `PublicationUnknown` сначала повторить lookup, не публиковать промежуточную таблицу.

### После прямой записи в HDFS

Это отдельный шаг **после commit**; автоматической регистрации в текущем API/CLI нет.
Минимальный вариант — одна внешняя непартиционированная таблица на результат:

1. Получить успешный `publish` или `lookup` с тем же `result_id`:

   ```bash
   uv run hadoop-result-writer lookup --config config.json --result-id calculation-43
   ```

   `state=not_committed` или ошибка — не основание регистрировать найденный наугад каталог.
   Взять **`receipt.location`** из успешного ответа: `<root>/results/<attempt-uuid>`.
   Не использовать `hdfs_root`, общий `results/`, `staging/` или `_commits/` как LOCATION.
2. Сверить Parquet-файлы каталога с `_manifest.json`; посторонних файлов данных быть не должно.
   Схему колонок взять из согласованной Arrow-схемы или проверенной схемы Parquet:
   manifest содержит только `schema_sha256`, не готовое SQL-описание.
   После публикации не дописывать и не менять файлы: SQL-движок читает каталог,
   а не наш manifest/commit. Hive connector Trino 484 пропускает имена с `_` и `.`,
   поэтому `_manifest.json` не является файлом данных; правило нужно проверить для
   вашего reader/версии ([реализация фильтра](https://github.com/trinodb/trino/blob/484/plugin/trino-hive/src/main/java/io/trino/plugin/hive/fs/HiveFileIterator.java)).
3. Зарегистрировать таблицу **одним** из двух способов ниже. Нужны существующая база/схема,
   права создания external table и разрешение использовать выбранный HDFS-путь.
   Читающим службам Hive/Trino нужны собственные рабочие credentials, обход родительских
   каталогов и чтение файлов; SQL-пользователю — доступ к схеме и SELECT.
4. Проверить данные SQL-запросами ниже и сохранить связь `result_id → полное имя таблицы`.
   HDFS commit и регистрация HMS — не общая транзакция: после сбоя регистрации файлы
   остаются опубликованными. Повторять нужно регистрацию по тому же receipt, не загрузку.

Примеры ниже — для DataFrame `calculation-43` из раздела Python API (`account_id`, `amount`).
Заменить имена базы/каталога/таблицы и **весь URI на фактический `receipt.location`**.
Для CLI `sample` схема другая: `row_id BIGINT`, `payload BINARY` в Hive / `VARBINARY` в Trino.
Другие Arrow-типы нужно отдельно сопоставить с поддерживаемыми типами Hive/Trino.

**Без обращения Python-приложения к Trino:** выполнить через Hive SQL-клиент, например
Beeline → HiveServer2, использующий нужный HMS
([Hive: external tables и LOCATION](https://hive.apache.org/docs/latest/language/languagemanual-ddl/#external-tables)):

```sql
CREATE EXTERNAL TABLE writer_demo.calculation_43 (
    account_id BIGINT,
    amount DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://nameservice1/apps/result-writer/results/<attempt-uuid>';
```

**Альтернатива — через Trino SQL-клиент** с Hive connector
([регистрация external table поверх существующих файлов](https://trino.io/docs/current/connector/hive.html#basic-usage-examples)):

```sql
CREATE TABLE hive.writer_demo.calculation_43 (
    account_id BIGINT,
    amount DOUBLE
)
WITH (
    format = 'PARQUET',
    external_location = 'hdfs://nameservice1/apps/result-writer/results/<attempt-uuid>'
);
```

Это только DDL метаданных, без INSERT: в Trino нужен `hive.non-managed-table-creates-enabled`,
но включать `hive.non-managed-table-writes-enabled` для этого шага не нужно.
Не переключать конфиг на `mode=trino` и не вызывать publish ради регистрации
HDFS-результата: это другая загрузка. `TrinoWriter.lookup` также предназначен для таблиц
собственного SQL-протокола, не этих DDL.
Если имя уже занято или ответ на CREATE потерян, проверить существующие LOCATION,
колонки и формат; не маскировать несоответствие через `IF NOT EXISTS` и не удалять данные.

### Проверка видимости и типичные проблемы

В Trino выполнить для зарегистрированной таблицы; для `mode=trino` подставить `receipt.table`:

```sql
SHOW CREATE TABLE hive.writer_demo.calculation_43;
SELECT count(*) AS row_count, count(DISTINCT "$path") AS file_count
FROM hive.writer_demo.calculation_43;
SELECT * FROM hive.writer_demo.calculation_43 LIMIT 10;
SELECT DISTINCT "$path" FROM hive.writer_demo.calculation_43;
```

Сопоставить строки/файлы с `receipt.rows`/`receipt.files`, пути — с `receipt.location`
(для HDFS также с manifest), значения и NULL — с ожидаемыми данными. Эти запросы могут
читать весь результат; они проверяют видимость и соответствие схемы, но не заменяют
проверку SHA-256 файлов.

- Таблица не найдена: проверить регистрацию, правильный HMS/catalog/schema и SQL-права.
- Таблица видна, но чтение падает: проверить серверный Kerberos/ACL HDFS, доступ к DataNode,
  соответствие колонок и Parquet-типов; тикета Python-писателя для reader недостаточно.
- Пусто или неожиданные NULL/строки: сверить LOCATION, список файлов и имена/типы колонок.
- Метаданные менялись вне Trino, а клиент видит старые: проверить кеш клиента и согласовать
  с администратором адресный сброс кеша Trino:
  `CALL hive.system.flush_metadata_cache(schema_name => 'writer_demo', table_name => 'calculation_43');`
  ([процедуры Hive connector](https://trino.io/docs/current/connector/hive.html#procedures)).
  Сброс кеша не создаёт отсутствующую таблицу.

`MSCK REPAIR TABLE` / `sync_partition_metadata` здесь не нужны: пример без партиций,
а UUID-каталоги прототипа не являются раскладкой `key=value`. Для общей таблицы с
партицией на результат понадобится отдельный согласованный шаг регистрации партиции;
в прототипе он не реализован. Проверку видимости выполнять от имени реального потребителя,
а не только администратора или писателя.

## Ошибки и повторы

`Conflict` означает другой fingerprint для того же result_id. `PublicationUnknown` означает,
что состояние нельзя подтвердить; это не отсутствие записи. Сохранять попытку, повторять
lookup. CLI возвращает JSON и exit code 2 при ошибке, exit code 0 при успехе.
Успешная запись HDFS и Kafka offset не образуют общей транзакции; этот пакет вообще не
подключается к Kafka. Интеграция worker должна строиться вокруг result_id и lookup.

## Контейнер

```bash
make image
docker run --rm --read-only --tmpfs /tmp:rw,nosuid,size=768m,uid=10001,gid=10001 \
  hadoop-result-writer:prototype --help
```

Для реального подключения `examples/compose.yaml` принимает абсолютные переменные
`HRW_CONFIG`, `HRW_KEYTAB`, `HRW_KRB5_CONFIG`, `HRW_HADOOP_CONF`.
Keytab должен быть доступен UID 10001 только на чтение. В SQL-режиме Hadoop-конфигурация
не используется, хотя общий образ содержит оба клиента. При собственном CA добавить
read-only mount и `ca_bundle` в JSON. Секретов в build context нет; `.dockerignore`
разрешает только исходники и manifests пакета. Hadoop 3.4.2 скачивается с проверкой SHA-512.
Образ включает pandas; приложение Python может использовать тот же API внутри контейнера.

```bash
docker compose -f examples/compose.yaml run --rm writer
```

Тестовый MIT KDC + защищённый одноузловой HDFS (без host-портов и production-секретов):

```bash
make image
make integration
```

Стенд проверяет настоящий keytab/TGT, HDFS RPC, загрузку, no-replace commit, повтор,
конфликт, обновление credentials и отказ без тикета. Реального Trino в этом стенде нет;
SQL-сценарии покрываются fake-адаптером и требуют приёмки на настоящем Trino.
`make integration` использует обычный Docker без Compose; эквивалентный Compose-профиль
сохранён в `integration/compose.yaml`.

## Официальная документация процесса

Ссылки Hadoop привязаны к версии клиента в образе (3.4.2), Arrow — к 23.0.1 из lock-файла.
Ссылки Trino `current` описывают актуальный сервер: перед настройкой выбрать документацию
версии вашего кластера. Эти материалы объясняют компоненты; commit/receipt-протокол
прототипа описан выше и не является стандартной транзакцией Hive.

- **Получение и проверка тикета:** MIT Kerberos — [kinit: keytab, cache, lifetime](https://web.mit.edu/kerberos/krb5-latest/doc/user/user_commands/kinit.html),
  [klist: проверка кеша и срока](https://web.mit.edu/kerberos/krb5-latest/doc/user/user_commands/klist.html),
  [krb5.conf: realm, KDC и DNS mappings](https://web.mit.edu/kerberos/krb5-latest/doc/admin/conf_files/krb5_conf.html).
  Здесь обновление означает новый `kinit -kt` по keytab, не `kinit -R` для renewable ticket.
- **Подключение к защищённому HDFS:** [Hadoop Secure Mode](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/SecureMode.html),
  [права и ACL](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html),
  [команды put, stat, cat, mv](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/FileSystemShell.html).
- **Устойчивость записи:** [Hadoop close, hflush, hsync и synconclose](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/filesystem/outputstream.html).
  Использовать при согласовании серверных гарантий, не считать rename заменой disk sync.
- **Python → Trino:** [trino-python-client: GSSAPI authentication](https://github.com/trinodb/trino-python-client#gssapi-authentication)
  и [Kerberos/TLS на coordinator](https://trino.io/docs/current/security/kerberos.html).
  В прототипе используется собственный адаптер к приватному credential cache, не копия
  примера `GSSAPIAuthentication` из документации клиента.
- **Trino → Hive/HDFS:** [Hive connector, external tables и права](https://trino.io/docs/current/connector/hive.html),
  [Hive Metastore и его Kerberos](https://trino.io/docs/current/object-storage/metastores.html),
  [HDFS filesystem и серверные credentials](https://trino.io/docs/current/object-storage/file-system-hdfs.html).
- **Входные данные и файлы:** [pandas ↔ Arrow](https://arrow.apache.org/docs/23.0/python/pandas.html),
  [явная Arrow schema](https://arrow.apache.org/docs/23.0/python/generated/pyarrow.schema.html),
  [запись и чтение Parquet](https://arrow.apache.org/docs/23.0/python/parquet.html).
- **Секреты и временное хранилище контейнера:** [read-only bind mounts](https://docs.docker.com/engine/storage/bind-mounts/)
  и [tmpfs: память и время жизни](https://docs.docker.com/engine/storage/tmpfs/).

## Приёмка

1. Проверить principal/keytab, DNS, часы, Hadoop HA и ACL через diagnose.
2. Выполнить sample в выделенном HDFS root, затем lookup и повтор; убедиться в том же receipt.
3. Для SQL — выделить схему и проверить CREATE/INSERT/COMMENT/RENAME, повтор и конфликт.
4. Проверить сбой KDC, потерю ответа и истечение тикета во время работы.
5. Измерить 1 и 5 ГБ отдельно для выбранного канала: RSS, диск, файлы, время, видимость данных.

Статус выполненных проверок и ограничения: `VALIDATION.md`. План OpenSpec:
`openspec/changes/standalone-hdfs-hive-writer/tasks.md`.
