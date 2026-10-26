# План bootstrap: PostgreSQL CDC с Debezium и Kafka

**ID плана:** `BOOTSTRAP-001`  
**Версия skill:** `bootstrap 0.1.0`  
**Статус плана:** `APPROVED`
**Согласование:** одобрено пользователем `2026-10-26T15:23:32Z`; одобрение не запускает применение или реализацию
**Режим:** только планирование; применение, загрузка образов, запуск сервисов, сборка, тесты и реализация не выполнялись  
**Дата инвентаризации:** `2026-10-26T06:57:26Z`

## 1. Вход, scope и fingerprints

### 1.1. Входные файлы

| Назначение | Путь | Fingerprint |
|---|---|---|
| Утверждённая спецификация | `docs/spec.md` | SHA-256 `49d4f0f278a4854d48073ccb1675e3aa54b2aa0afaa8beda4baa45bb8540ddc2` |
| Исходный brief | `docs/idea.md` | SHA-256 `5c9e0bc21964485b72ba38068e3e5251b2379b7ba59dfeafb53fdef76fb96c51` |
| Git commit | repository root | `e4171746bf9ce875dfd276d9a25dc5ad8811920f` |

На момент инвентаризации Git working tree был чистым. Файл этого плана до записи отсутствовал; `docs/` и `docs/spec.md` не являются symlink. План расположен отдельно от исходной спецификации и автоматической конфигурации.

### 1.2. Выбранный scope

Выбран единственный согласованный законченный срез из `docs/spec.md`, разделы 2.2 и 10: **вся обязательная базовая версия**, включая AC-001–AC-014. Урезанный MVP для одной таблицы, только happy path или реализация без recovery/schema-change/out-of-order не считается завершённым срезом.

Спецификация имеет статус `APPROVED`; её behavioral contracts, scope, исключения, версии и digest-refs не переопределяются этим планом.

### 1.3. Исключения

Сохраняются исключения спецификации, в частности: Airflow, dbt, ClickHouse, Superset, Spark, Schema Registry, multi-broker cluster, глобальный порядок между topics, автоматическая эволюция SQLite, CDC для `TRUNCATE`, update primary key и заявление exactly-once.

## 2. Наблюдаемое состояние

### 2.1. Репозиторий

**Наблюдалось локально:**

- tracked-файлы: только `docs/idea.md` и `docs/spec.md`;
- отсутствуют `AGENTS.md`, `.gitignore`, dependency manifests, lock-файлы проекта, Compose, Dockerfiles, CI, scripts и тесты;
- отсутствуют project-level `.pi/settings.json`, `.pi-lens.json` и `pi-lens.json`;
- существующих команд сборки, запуска и проверки нет;
- существующего плана bootstrap нет.

Следствие: проект не готов к запуску, но и не содержит legacy-обвязки, которую пришлось бы сохранять или мигрировать.

### 2.2. Локальная машина и CLI

**Наблюдалось локально:**

| Компонент | Состояние |
|---|---|
| ОС | Ubuntu 24.04.5 LTS под WSL2, Linux `x86_64` |
| Host CPU | 16 logical CPU; это не разрешение занять их все |
| Host RAM | 23 GiB total, около 16 GiB available в момент чтения; это не Docker allocation |
| Свободное место на filesystem репозитория | около 914 GiB; это не доказательство свободного места Docker storage |
| Pi | `1.4.0` |
| Docker CLI | `29.8.0` |
| Docker Compose | `v5.5.1` |
| Node/npm | `24.15.0` / `11.12.1` |
| Python host | `3.12.3` |
| Java/Javac/Maven/Gradle | не установлены; для выбранного container-first path не требуются |

**Не проверено:** доступность Docker daemon, Docker Desktop/Engine allocation, Docker storage free space, наличие закреплённых образов в local cache и работоспособность container networking. Команды, обращающиеся к daemon, сервисы и pulls в plan-only режиме не запускались.

### 2.3. Pi и установленные ресурсы

User settings указывают default model `openai/gpt-5.6-sol` и четыре npm-пакета. Установленные версии подтверждены локальными package manifests и lock-файлом:

| Ресурс | Версия | Фактическое состояние в этой сессии | Решение |
|---|---:|---|---|
| `@upstash/context7-pi` | `0.1.3` | tools и skill доступны; запросы передают поисковый вопрос внешнему Context7 API | Оставить для точечных запросов к первичной документации без секретов и полного private context |
| `pi-lens` | `4.4.4` | extension активен; доступна диагностика/навигация; project/global config отсутствует | Использовать при реализации для read-before-edit и diagnostics; новую project config сейчас не добавлять |
| `pi-lmstudio` | `1.5.0` | пакет настроен; текущий default provider — не LM Studio; server readiness не проверялась | Не использовать для этого среза и не менять provider |
| `pi-subagents` | `0.81.0` | loader доступен; запуск не выполнялся | Не использовать на bootstrap/apply по умолчанию; подключать только по отдельному явному запросу пользователя |
| skill `bootstrap` | `0.1.0` | прочитан и применён | Только этот plan-only результат |

Особенность активной среды: у `pi-lens` по умолчанию включены hooks диагностики, форматирования/autofix и test-on-write. Для Markdown effective config выбирает Marksman, typos и документные проверки. Project config не обнаружен. Запись разрешённого файла плана не требует отключения защиты; при будущей реализации результаты hooks должны считаться дополнительными findings, а не заменой явных проверок проекта.

## 3. Выбор возможностей

| Потребность | Существующая возможность | Решение | Основание и ограничение |
|---|---|---|---|
| Навигация и правила для агента | `docs/spec.md`, Git, будущий короткий `AGENTS.md` | Добавить один короткий `AGENTS.md` | Сейчас нет точки входа для следующего агента; файл должен ссылаться на spec и команды, а не дублировать контракт |
| Запуск local lab | установленный Docker Compose v2 | Использовать Compose, не добавлять оркестратор | Это утверждённая среда спецификации; Java/Kafka CLI на host не нужны |
| Повторяемые операторские команды | прямые Compose/SQL команды плюс небольшие shell wrappers | Добавить два небольших интерфейса: `scripts/lab` и `scripts/check` | Убирает длинные опасные команды, даёт preflight/confirmation; не вводит Make/Task runner dependency |
| Python dependency management | pinned Python image и `pip` внутри него | Один runtime lock/requirements с exact pins; host venv не требуется | Основной path контейнерный; обязательный third-party runtime dependency — `confluent-kafka==2.16.0` |
| Unit tests | Python stdlib `unittest` | Использовать сейчас | Не добавляет pytest только ради тестового runner; подходит для parser/ordering/SQLite fixtures |
| Интеграционная приёмка | Compose + SQL/Kafka/SQLite queries + небольшая Python orchestration | Реализовать в `scripts/check acceptance <AC-ID>` | Проверки должны наблюдать реальные сервисы и offsets, а не возвращать пустой `exit 0` |
| Актуальная документация API | Context7 | Оставить по необходимости | Нужен только для конкретного API-вопроса при реализации; в этом плане версии уже закреплены утверждённой spec |
| LSP/linters/autofix | pi-lens | Использовать как дополнительный feedback | Отсутствие findings не заменяет unit/integration acceptance; markdown/code могут быть автоматически отформатированы |
| Делегирование | pi-subagents | Не использовать на этом этапе | Пользователь не просил delegation; сложность сама по себе не даёт полномочия |
| Новый MCP/extension/skill | отсутствующая потребность | Ничего не добавлять | Все необходимые функции закрываются существующими CLI, небольшими scripts и текущими tools |
| CI | локальный воспроизводимый acceptance path | Не добавлять в первый apply | Спецификация требует local lab, а не remote CI; privileged Docker-in-Docker добавил бы отдельную среду и риск расхождения |

**Минимальное решение по агентской обвязке:** один `AGENTS.md`; без `.pi/settings.json`, новых extensions, MCP, project skills и дополнительных файлов состояния.

## 4. Техническое предложение

### 4.1. Зафиксированный stack

Сохранить без изменения значения из утверждённой спецификации:

- PostgreSQL `16.13`, `pgoutput`, image `postgres:16.13-bookworm@sha256:472efd9a66f2b2f1a5aeb18b28de74332e6ef88c2b93a1a5d812fb6db67a5f60`;
- Kafka KRaft `4.3.0`, image `quay.io/debezium/kafka:3.6.3.Final@sha256:c8af47ccb718e5f17cba0493142079f38ed18df3615200f5ae6a6aa21c49c658`;
- Kafka Connect `4.3.0` + Debezium PostgreSQL Connector `3.6.3.Final`, image `quay.io/debezium/connect:3.6.3.Final@sha256:47f266af35d478b646db2e2e4603d21e56861151ab946e879b38edd5b533b6a6`;
- Python `3.12.12`, image `python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c`;
- `confluent-kafka==2.16.0`, включая `librdkafka 2.16.0`.

**Источник:** `docs/spec.md`, разделы 1, 3.6, 5.3, 6.1 и 11; там же зафиксированы ссылки на Debezium 3.6, PostgreSQL 16, Kafka и Confluent client documentation и результаты выполненного ordering/Python-client probe.

**Граница уверенности:** совместимость наблюдалась предыдущим probe на Linux amd64 и зафиксирована в APPROVED spec. В этом plan-only запуске registry metadata и образы повторно не проверялись. Изменять versions/tags/digests без возврата к compatibility review нельзя.

### 4.2. Предлагаемая структура

Структура является планом, а не уже созданными файлами:

```text
AGENTS.md
README.md
.gitignore
.env.example
compose.yaml
config/
  connect/                 # worker и generation-scoped connector templates
  kafka/                   # topic definitions/retention inputs
  generations/g001.env    # несекретные generation identifiers
postgres/
  init/                    # schema, roles, publication, seed; shell wrapper reads secrets from env
  exercises/               # streaming, delete, schema-change SQL
consumer/
  Dockerfile
  requirements.txt         # exact runtime pins
  src/cdc_consumer/        # parser, ordering, SQLite, Kafka loop, diagnostics, migrations
  migrations/              # v1 baseline, v2 customers.phone
scripts/
  lab                      # lifecycle/preflight/diagnostics/recovery commands
  check                    # unit and AC orchestration
  lib/                     # shared shell helpers only if duplication appears
fixtures/
  events/                  # deterministic Debezium envelopes for AC-005/006/012
  expected/                # concise expected state, not copies of the full spec
tests/
  unit/
  integration/
  acceptance/
docs/
  idea.md
  spec.md
  bootstrap.md
  runbook.md               # operator flow, recovery, schema-change, cleanup
.runtime/                  # gitignored: env overrides, SQLite, generated configs, evidence
```

Не создавать отдельный журнал решений или status-файл: источником контракта остаётся `docs/spec.md`, планом — этот документ, операторской инструкцией — будущий `docs/runbook.md`.

### 4.3. Compose и ресурсы

Предложение:

- сервисы: PostgreSQL, Kafka KRaft, Kafka Connect, Python consumer; one-shot bootstrap/check containers запускаются только явными командами;
- явные healthchecks и dependency gates; readiness определяется API/SQL сигналом, а не только `container running`;
- persistent named volumes для PostgreSQL/Kafka и bind-mounted generation directory для SQLite `/data/cdc-g001.sqlite3`;
- table topics создаются явно **до** регистрации connector: три partitions, replication factor 1, `cleanup.policy=delete`, 24h, 256 MiB/partition;
- предложенный `segment.bytes=64 MiB`, чтобы time/size retention наблюдалась в лаборатории целыми segments и не требовала заполнить 256 MiB одним segment; это обратимое delegated technical решение;
- Kafka Connect internal topics имеют собственные compact policies и не наследуют table-topic retention;
- container logs ограничиваются rotation, чтобы логи не обходили общий 25 GiB бюджет;
- service limits задаются ниже верхнего бюджета spec: ориентир суммарно не более 6 vCPU и 8 GiB RAM для steady-state сервисов, оставляя запас для one-shot acceptance и Docker overhead;
- `up` не должен неявно менять версии; загрузка pinned images выносится в отдельную явную команду `scripts/lab pull`;
- destructive cleanup/reset требует generation ID и отдельного подтверждения; обычный `stop` не удаляет volumes.

### 4.4. PostgreSQL bootstrap

Предложение:

- запуск с `wal_level=logical`, достаточными `max_replication_slots`/`max_wal_senders` и `max_slot_wal_keep_size=1GB`;
- bootstrap-admin создаёт БД, source schema, три таблицы, constraints, immutable-ID trigger, seed и publication;
- connector role создаётся из runtime secret, получает только контракт раздела 3.6;
- publication включает ровно `customers`, `orders`, `payments`;
- явные seed identity values сопровождаются корректировкой sequences, чтобы streaming IDs 3/103/1002 создавались детерминированно;
- SQL exercises разделены на наблюдаемые операции и не используют `TRUNCATE`;
- отрицательная DML-проверка connector role считается успешной только при отказе именно по privilege причине.

Секреты не коммитятся: `.env.example` содержит только имена переменных и безопасные placeholders; `.env` и `.runtime/` игнорируются Git. Scripts не печатают пароли в evidence.

### 4.5. Kafka Connect и generation contract

Предложение:

- базовая generation `g001` использует точные имена из spec;
- connector config включает `pgoutput`, `snapshot.mode=initial`, `provide.transaction.metadata=true`, `publication.autocreate.mode=disabled`, explicit include list, JSON converters со schemas и `slot.drop.on.stop=false`;
- connector регистрируется только после PostgreSQL bootstrap, table-topic creation и Connect readiness;
- обычный restart сохраняет Connect offsets, Kafka data и replication slot;
- новый generation helper принимает только возрастающий ID вида `gNNN`, генерирует отдельный connector/slot/publication/topic prefix/group/sink и не перезаписывает старую runtime directory;
- старая SQLite generation не монтируется на запись в новый consumer; best-effort filesystem read-only является дополнительной защитой, но основная гарантия — отдельный путь и отсутствие его в конфигурации нового consumer.

### 4.6. Python consumer и SQLite

Предложение разбить consumer на небольшие компоненты:

1. `decode`: сохранить исходные key/value bytes и headers, разобрать JSON schema/payload;
2. `classify`: topic/table/op/snapshot validation;
3. `token`: нормализация и сравнение `(phase_rank, source_lsn, total_order, collection_order)`;
4. `store`: SQLite migrations, raw archive, headers, errors, projections, watermarks;
5. `processor`: одна SQLite transaction для raw + projection; duplicate/stale/tombstone semantics;
6. `runner`: poll последовательно, synchronous Kafka commit только после SQLite commit;
7. `diagnostics`: event age и controlled status output;
8. `faults`: только явно включаемая one-shot crash injection для AC-004.

Критические границы:

- duplicate coordinate проверяется до повторного ordering apply;
- равный token у разных coordinates — fail-stop;
- delete обновляет watermark до удаления projection row;
- tombstone архивируется, но не меняет watermark/projection;
- unsupported schema field вызывает rollback main transaction;
- после rollback malformed/schema error записывается отдельной короткой SQLite transaction в `ingestion_errors`, затем процесс завершается без Kafka commit;
- Kafka commit использует конкретный processed message offset + 1 и проверяет partition-level errors;
- commit failure после durable SQLite commit приводит к остановке/retry, а не к удалению sink state;
- SQLite projection не имеет cross-table foreign keys;
- monetary values хранятся каноническими decimal strings;
- raw headers сохраняют порядок и repeated names;
- SQLite size guard проверяется до приёма новой работы; дополнительно SQLite page limit должен превращать превышение около 1 GiB в контролируемый rollback/exit, а не повреждение файла.

### 4.7. Командный интерфейс

Предлагаемые интерфейсы ещё не существуют и должны быть реализованы:

```text
scripts/lab preflight
scripts/lab pull
scripts/lab up
scripts/lab bootstrap g001
scripts/lab status g001
scripts/lab lag g001
scripts/lab stop
scripts/lab new-generation g002
scripts/lab destroy g001 --confirm g001

scripts/check unit
scripts/check smoke
scripts/check acceptance AC-001
scripts/check acceptance all
```

Требования:

- команды печатают выполняемые этапы и завершаются ненулевым status при несовпадении expected result;
- `preflight` не меняет runtime resources;
- `pull`, `bootstrap`, schema migration, fault injection, generation switch и destruction — отдельные явные действия;
- `acceptance all` не маскирует failed/skipped case и сохраняет evidence по каждому AC;
- повторный bootstrap одной generation либо подтверждает уже совпадающее состояние, либо останавливается с понятным конфликтом; не создаёт дубли.

## 5. Неизвестные и минимальные будущие probes

Ни один probe ниже в этом режиме не выполнялся.

| Неизвестность | Минимальный эксперимент | Изменяемые ресурсы и ограничения | Ожидаемый сигнал | Дальнейшее решение |
|---|---|---|---|---|
| Доступен ли Docker daemon и достаточна ли его allocation | `scripts/lab preflight`: client/server version, architecture, memory/CPU summary без полного `docker info` dump | Ничего не создаёт; не печатает registry auth/config | Server отвечает; доступные limits не ниже planned steady-state caps | Если ниже — уменьшить caps в пределах spec или запросить изменение Docker allocation |
| Доступны ли exact digest refs | Сначала inspect local cache; затем только после одобрения `scripts/lab pull` | Сеть и image cache; без запуска containers | Каждый image разрешается ровно в указанный digest | Любое несовпадение останавливает работу; spec не переписывается молча |
| Работает ли stack на текущем WSL2 daemon | Запустить только базовые сервисы, дождаться healthchecks, запросить versions | Создаёт только namespaced `cdc-lab-g001` containers/networks/volumes | PostgreSQL 16.13, Kafka/Connect 4.3.0, connector 3.6.3.Final | При mismatch остановить до fixtures/данных |
| Достаточно ли Docker storage для load exercise | После старта PostgreSQL проверить free blocks filesystem его data volume | Read-only `df` внутри собственного container | Не менее 5 GiB free | Иначе не запускать AC-011/load; предложить cleanup только ресурсов этого lab |
| Поддерживают ли digest images macOS/arm64 напрямую | На arm64 host выполнить manifest/platform inspection и короткий startup smoke | Registry metadata; при smoke — собственные containers | native arm64 manifest либо документированно рабочая amd64 emulation | Зафиксировать platform note; не менять digest автоматически |
| Действуют ли topic retention/segment settings как ожидается | Создать тестовый backlog в generation-scoped topics и наблюдать configs/log-start offsets | Только g001 test records; остановка при достижении заданного небольшого объёма | Configs совпадают, deletion идёт целыми eligible segments | При расхождении корректировать delegated segment setting, не 24h/256MiB contract |
| Корректен ли Python wheel/build в финальном Dockerfile | Build consumer image и импорт/version check | Build cache и сеть только при явном build/pull шаге | Python 3.12.12, confluent-kafka/librdkafka 2.16.0 | Иначе остановить; не ослаблять exact pin без review |

Критерий остановки для всех probes: mismatch exact version/digest, выход за 8 vCPU/16 GiB/25 GiB, неожиданный доступ вне lab namespace, отсутствие безопасного cleanup или риск затронуть чужие volumes/containers.

## 6. План будущих изменений

### 6.1. Работа агента

| ID | Сохранить/добавить/изменить | Зачем | Зависимость | Критерий проверки | Локальный откат |
|---|---|---|---|---|---|
| A-01 | Добавить короткий `AGENTS.md` | Дать следующему агенту source of truth, команды, границы destructive/network действий | Утверждение этого плана | Файл ссылается на `docs/spec.md` и `docs/bootstrap.md`, не копирует всю spec; перечисляет proposed commands и запреты | Удалить только `AGENTS.md` |
| A-02 | Не добавлять `.pi/settings.json`, project skills/extensions/MCP | Не создавать обвязку без незакрытой потребности | Нет | Git diff не содержит Pi config | Ничего откатывать |
| A-03 | Сохранить `docs/spec.md` и `docs/idea.md` неизменными | Не менять утверждённый контракт | Все задачи | Hash исходных документов не меняется без отдельного решения пользователя | Восстановить только случайный diff этих файлов |
| A-04 | Добавить ссылки на runbook/commands в `README.md`, а детали эксплуатации — в одном `docs/runbook.md` | Разделить быстрый вход и учебные процедуры без дублирующих журналов | E-05, E-06 | Все команды в документации реально существуют; recovery/schema-change ссылаются на AC | Удалить только добавленные docs, не runtime data |

### 6.2. Среда и проверки

| ID | Сохранить/добавить/изменить | Зачем | Зависимость | Критерий проверки | Локальный откат |
|---|---|---|---|---|---|
| E-01 | `.gitignore`, `.env.example`, generation metadata без секретов | Не коммитить credentials, SQLite, generated configs/evidence | A-01 | Secret/runtime paths игнорируются; example не содержит рабочего пароля | Удалить файлы; runtime secrets отдельно не трогать автоматически |
| E-02 | `compose.yaml` с pinned refs, healthchecks, volumes, limits, log rotation | Воспроизводимый основной path | Probe daemon/images | `docker compose config --quiet`; runtime versions совпадают | `docker compose down` без `-v`; удалить compose diff |
| E-03 | PostgreSQL bootstrap и exercises | Source schema, least privilege, publication, deterministic fixtures | E-02 | Catalog/grant queries и seed checks; negative DML fail | Удалить только lab DB/volume через подтверждённый destroy |
| E-04 | Kafka topic/bootstrap и connector registration | Явные partitions/retention/naming и connector lifecycle | E-02, E-03 | Topic configs, connector/task RUNNING, exact config fields | Удалить только generation-scoped connector/topics после подтверждения |
| E-05 | Consumer Dockerfile + exact requirements | Контейнерный Python path без host dependency drift | Image/client probe | Version check и import; no unpinned runtime range | Удалить image/build files; не чистить общий Docker cache автоматически |
| E-06 | `scripts/lab` и `scripts/check` | Единый безопасный интерфейс | E-02–E-05 | shell syntax, clear nonzero failures, explicit network/destructive steps | Удалить scripts; созданные runtime resources требуют отдельного confirmed cleanup |
| E-07 | Unit/integration/acceptance fixtures и evidence format | Проверять contracts, а не только process startup | R-01–R-08 | Непустой набор тестов; deliberate failing fixture действительно падает | Удалить test code; evidence под `.runtime` удаляется отдельно |

### 6.3. Реализация результата

| ID | Область | Вход | Наблюдаемый выход | Зависимости | Связанные AC |
|---|---|---|---|---|---|
| R-01 | Source model, seed, role, publication | Разделы 3.1, 3.6, 9.1 | Три таблицы/seed, immutable IDs, restricted connector role | E-02, E-03 | AC-001, AC-002, AC-013 |
| R-02 | Topics и connector g001 | Разделы 3.2–3.5, 5.1 | Три 3-partition topics; snapshot затем streaming | R-01, E-04 | AC-001, AC-008, AC-014 |
| R-03 | SQLite schema/migrations | Разделы 4.1–4.5 | Raw, headers, errors, projections, watermarks, migration version | E-05 | AC-001–AC-008, AC-012 |
| R-04 | Parser/validation/token comparator | Разделы 5.1–5.6 | Deterministic outcomes `applied/stale/tombstone/fail-stop` | R-03 | AC-003, AC-005, AC-006, AC-012 |
| R-05 | Kafka consume/durable commit loop | Разделы 4.2, 5.4 | Sequential processing; sync offset commit after SQLite commit | R-03, R-04 | AC-002, AC-004, AC-014 |
| R-06 | Schema-change migration flow | Раздел 5.5 | Stop before migration; replay same offset after v2 migration | R-03–R-05 | AC-007 |
| R-07 | Diagnostics, retention and disk guards | Раздел 6 | Separate partition lag, event age, retained WAL; bounded stop | R-02, R-05 | AC-009, AC-011 |
| R-08 | Recovery/rebuild generation tooling | Раздел 7 | g002 isolated from g001; old sink preserved; verification gates | R-01–R-07 | AC-010 |
| R-09 | Acceptance orchestration and runbook | Разделы 8–12 | One command per AC, expected state and evidence | R-01–R-08 | AC-001–AC-014 |

Рекомендуемый порядок: `A-01/E-01 → E-02/E-05 → R-01/R-03 → R-02/R-04 → R-05 → R-06/R-07 → R-08 → E-07/R-09 → A-04`.

## 7. Проверки и evidence

### 7.1. Быстрые проверки

Предлагаемые команды, которые предстоит реализовать:

| Команда | Тип | Ожидаемый результат | Evidence |
|---|---|---|---|
| `scripts/check unit` | Без сервисов, внутри pinned Python image | Parser, comparator, duplicate, delete watermark, malformed и migrations tests проходят; test count > 0 | stdout + unittest summary |
| `scripts/check smoke` | Короткий Compose smoke | Config валиден, сервисы healthy, versions exact, connector может быть зарегистрирован | version/status JSON и Compose status |
| `scripts/lab status g001` | Read-only runtime inspection | Connector/task, topics, group и sink paths относятся только к g001 | stdout без secrets |
| `scripts/lab lag g001` | Read-only diagnostics | Kafka lag по partition, event age и WAL bytes показаны раздельно | structured text/JSON |

`docker compose config --quiet`, container startup или пустой test suite сами по себе не подтверждают ни один behavioral AC.

### 7.2. AC → команда → ожидаемый результат → evidence

Все команды в таблице **предложены и ещё не существуют**.

| AC | Будущая команда | Ожидаемый результат | Обязательное evidence |
|---|---|---|---|
| AC-001 | `scripts/check acceptance AC-001` | Snapshot rows всех трёх таблиц в raw/projection, connector streaming | source counts/rows, raw `op=r`, projection rows, connector state |
| AC-002 | `scripts/check acceptance AC-002` | Streaming create/update отражены в raw и projection | source vs SQLite row diff, Kafka coordinates |
| AC-003 | `scripts/check acceptance AC-003` | Delete+tombstone архивированы; row удалён; watermark сохранён | два raw records, projection absence, watermark row |
| AC-004 | `scripts/check acceptance AC-004` | Crash after SQLite commit before Kafka commit даёт duplicate без второго apply | один raw record, increased delivery count, unchanged projection, committed offset after restart |
| AC-005 | `scripts/check acceptance AC-005` | Старый update получает `stale` | два unique raw records, token/watermark, final projection |
| AC-006 | `scripts/check acceptance AC-006` | Старое событие после delete не воскрешает row | delete watermark, stale status, projection absence |
| AC-007 | `scripts/check acceptance AC-007` | До migration fail-stop/no offset commit; после migration replay succeeds | exit status, same Kafka coordinate, offsets before/after, `phone` values |
| AC-008 | `scripts/check acceptance AC-008` | Child может примениться раньше parent; final source parity | промежуточный dangling state и итоговый diff=empty |
| AC-009 | `scripts/check acceptance AC-009` | Три отдельные метрики доступны | per-partition lag, event-age sample, WAL bytes/query output |
| AC-010 | `scripts/check acceptance AC-010` | g002 использует отдельные resources и достигает parity/lag 0 | resource manifest обоих generations, old sink checksum/read-only state, g002 rows и два стабильных lag checks |
| AC-011 | `scripts/check acceptance AC-011` | Retention configs верны; WAL warning/slot limit наблюдаемы; SQLite controlled stop; <5GiB blocks load | topic configs/log offsets, slot query, size/exit status, preflight refusal |
| AC-012 | `scripts/check acceptance AC-012` | Malformed record сохраняет error, не меняет projection и offset | original bytes/error row/attempts, unchanged projection and committed offset, nonzero exit |
| AC-013 | `scripts/check acceptance AC-013` | Snapshot/streaming работают с least privilege; DML отвергнут | grants/catalog output, connector state, expected privilege error |
| AC-014 | `scripts/check acceptance AC-014` | Restart не создаёт новый snapshot, streaming продолжается | before/after raw op counts, connector offsets/slot identity, new streaming row |

### 7.3. Evidence storage

- По умолчанию команды печатают redacted human-readable summary и machine-readable JSON.
- При acceptance evidence сохраняется под `.runtime/evidence/<generation>/<AC-ID>/<timestamp>/` и игнорируется Git.
- Evidence содержит только нужные query/config/status outputs, coordinates и checksums; пароли, auth config, полный environment dump и Docker inspect не сохраняются.
- Failure/skip остаются видимыми в итоговом index; «не запускалось» не превращается в pass.

## 8. Приёмка будущего apply

Apply bootstrap/implementation можно считать завершённым только если одновременно:

1. Git diff ограничен согласованными project files; `docs/spec.md` и `docs/idea.md` не изменены без отдельного решения.
2. Нет новых global Pi settings, packages, model/provider changes или скрытых background jobs.
3. Images/dependencies закреплены; нет `latest` и неограниченных version ranges для runtime.
4. Network, pulls и destructive cleanup вынесены в явные команды.
5. Обычный stop/restart не удаляет volumes, slot, offsets или sink.
6. Unit suite непустой и проходит; затем smoke проходит на exact stack.
7. AC-001–AC-014 имеют фактическое evidence; непроверенные AC явно помечены, а не объявлены выполненными.
8. Resource guards и cleanup ограничены только lab namespace/generation.
9. Повторное применение без новых входов не дублирует publication, connector, topics, seed, migrations или инструкции.

## 9. Что требует отдельного запроса на выполнение

План одобрен, но следующие действия не входят в этот plan-only запуск и могут быть выполнены только по отдельному запросу на реализацию:

- создание/изменение всех project files из раздела 6;
- network access для pull/build/dependency resolution;
- запуск Docker containers, services и acceptance probes;
- создание volumes, topics, connector, replication slot/publication и runtime data;
- schema-change и fault-injection exercises;
- destructive cleanup, reset или переход на новую generation.

Блокирующих вопросов для начала ограниченного implementation этапа не обнаружено. План согласован. Первый безопасный следующий шаг — отдельно поручить реализацию этапа `A-01 + E-01 + E-02 + E-05` без запуска сервисов; после review diff отдельно согласовать image pulls и smoke probe.
