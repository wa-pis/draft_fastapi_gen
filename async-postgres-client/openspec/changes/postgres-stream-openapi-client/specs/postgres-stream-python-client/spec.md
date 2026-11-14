## ADDED Requirements

### Requirement: Standalone asynchronous client

Python-пакет SHALL работать через HTTP по каноническому серверному OpenAPI 3.1
`async-postgres-writer/openapi/stream-writer.v1.yaml` без серверных зависимостей
и доступа к PostgreSQL. Клиент SHALL передавать токен через Bearer, поддерживать
таймауты и закрывать HTTP-пул при завершении своего async-контекста.

#### Scenario: Install and use without the server package

- **WHEN** приложение устанавливает клиент и подключается к запущенному серверу по URL
- **THEN** клиент выполняет операции с Bearer token без прямых подключений к PostgreSQL
- **AND** CI проверяет методы и форматы операций `uploadNdjson`, `getUploadReceipt`, `listUploadRows` и `streamUploadRows` против зафиксированной версии OpenAPI

### Requirement: Stream upload bytes and resolve unknown commit

Клиент SHALL передавать PUT с заранее заданным UUID и телом UTF-8
`application/x-ndjson` из асинхронного итератора байтов без накопления
всего тела и изменения исходных байтов. Клиент MUST NOT автоматически повторять
PUT или подменять UUID при неопределённом исходе.

#### Scenario: Return a new or reused receipt

- **WHEN** сервер отвечает `201` для новой загрузки или `200` для точного повтора
- **THEN** клиент возвращает квитанцию со значениями `upload_id`, `sha256`, `row_count`, `byte_count`, `committed_at` и `reused`

#### Scenario: Check the original UUID after a lost response

- **WHEN** ответ PUT потерян и результат записи неизвестен
- **THEN** клиент позволяет отдельно запросить квитанцию исходного UUID, не перепосылая поток
- **AND** отсутствие квитанции обозначается ответом `404` и не интерпретируется как завершённая неудача записи
- **AND** при явном повторе те же байты должны быть доступны вызывающему коду

### Requirement: Support cursor and offset pages

Клиент SHALL поддерживать `limit` от 1 до 1000 и любую одну позицию:
`offset` или `after_row_number` в диапазоне от 0 до максимума signed int64.
Отсутствующие параметры позиции SHALL NOT добавляться в запрос. Декодирование
страниц и строк NDJSON SHALL сохранять точные числа JSONB.

#### Scenario: Follow a cursor across pages with gaps

- **WHEN** приложение начинает с `after_row_number=0`
- **THEN** клиент сохраняет присланный `next_after_row_number` без вычисления номера из `limit` или количества строк
- **AND** значение `null` означает отсутствие следующей страницы

#### Scenario: Preserve the offset mode

- **WHEN** передан только `offset`, включая `offset=0`, или позиция не указана
- **THEN** клиент получает и возвращает прежнюю форму результата с `offset`
- **AND** одновременная передача обеих позиций отклоняется до запроса

### Requirement: Verify complete NDJSON downloads

Клиент SHALL читать поток внутри async-контекста без накопления всего ответа,
разбирая UTF-8 объекты по LF независимо от границ сетевых порций и применяя
настраиваемый лимит байтов на строку. Клиент SHALL проверить media type,
неотрицательный целочисленный `X-Row-Count` и на штатном EOF точное совпадение
числа полных строк. Клиент MUST NOT автоматически возобновлять частичную выгрузку.

#### Scenario: Read a record split across chunks

- **WHEN** строка длиннее одной порции или многобайтовый символ UTF-8 разделён на порции
- **THEN** клиент возвращает одну строку с неповреждёнными данными, если её размер укладывается в настроенный лимит

#### Scenario: Detect truncation after successful HTTP status

- **WHEN** сервер отправил `200`, но транспорт оборвался, строка не завершена LF, превышен лимит строки или счётчик не совпал
- **THEN** итерация завершается ошибкой, а не обычным окончанием

#### Scenario: Stop consuming without leaking resources

- **WHEN** вызывающий код выходит из контекста до конца, в том числе при отмене
- **THEN** HTTP-ответ закрывается и полнота потока остаётся неподтверждённой

### Requirement: Preserve status and error details

Клиент SHALL отличать неуспешный HTTP-статус от транспортной ошибки
и нарушения протокола. Для известных форм сервера SHALL сохраняться статус,
код и сообщение `error/message` либо `detail` со строкой или деталями валидации;
неизвестный формат MUST NOT скрывать HTTP-статус.

#### Scenario: Read a conflict or validation failure

- **WHEN** сервер отвечает `409` с `error/message` либо `422` с `detail`
- **THEN** вызывающий код получает ошибку с нужным статусом и переданными сервером деталями
