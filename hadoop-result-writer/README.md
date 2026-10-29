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
и не гарантирует автоматическую видимость результата в Trino. Подключение потребителя —
отдельный следующий шаг. Rename должен выполняться в одной HDFS/encryption zone.
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
[гарантии Hadoop OutputStream](https://apache.github.io/hadoop/hadoop-project-dist/hadoop-common/filesystem/outputstream.html).

Физический manifest должен совпадать при повторном publish. Другой размер порций, версия
Arrow/Parquet или компрессия могут дать конфликт для логически тех же строк. После падения
сначала использовать lookup. До commit может остаться незавершённый каталог; автоматического
удаления таких каталогов в прототипе нет.

## Запись через Trino

Настроить `mode=trino`, HTTPS coordinator, CA при необходимости, GSSAPI service name
(часто `HTTP`, зависит от кластера) и отдельную существующую тестовую Hive-схему.
Права: CREATE внешних Parquet-таблиц, INSERT, SELECT, COMMENT, ALTER TABLE RENAME;
служба Trino должна иметь доступ к HDFS root. SQL-режим не использует register_partition.

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

## Приёмка

1. Проверить principal/keytab, DNS, часы, Hadoop HA и ACL через diagnose.
2. Выполнить sample в выделенном HDFS root, затем lookup и повтор; убедиться в том же receipt.
3. Для SQL — выделить схему и проверить CREATE/INSERT/COMMENT/RENAME, повтор и конфликт.
4. Проверить сбой KDC, потерю ответа и истечение тикета во время работы.
5. Измерить 1 и 5 ГБ отдельно для выбранного канала: RSS, диск, файлы, время, видимость данных.

Статус выполненных проверок и ограничения: `VALIDATION.md`. План OpenSpec:
`openspec/changes/standalone-hdfs-hive-writer/tasks.md`.
