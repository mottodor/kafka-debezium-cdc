# Task status

## TASK_ID: A-01

- Source: `docs/bootstrap.md`, раздел 6.1, строка задачи `A-01`
- Status: REVIEW_READY
- Updated: 2026-10-27T17:45:45+03:00
- Baseline HEAD: `b36fdf4dad22b1bd0daa5aa06b5da7aa5b7d4f40`

### Progress
- Implemented: добавлен корневой `AGENTS.md` со ссылками на утверждённые источники, proposed commands и границами network/destructive действий.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.1, `A-01`, критерий проверки | PASS | fast: Python content assertions, exit 0 — ссылки на `docs/spec.md`/`docs/bootstrap.md`, proposed commands и запреты присутствуют; файл 54 строки; negative mutation удалённой ссылки обнаружена. `git diff --check`, exit 0; LSP diagnostics для `AGENTS.md` и `docs/status.md`: 0 findings. |

### Issues
#### A-01/B01
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: первый reviewer заблокировал diff из-за наличия `docs/status.md`, применив общий запрет bootstrap-плана и не учтя прямое требование текущего вызова `implement`, которое делает этот файл обязательным служебным scope.
- Evidence: reviewer call 1: `VERDICT BLOCK`; единственный finding требует удалить `docs/status.md`, при этом содержимое `AGENTS.md` признано соответствующим A-01.
- Unblock: повторный fresh review с явным указанием приоритета и обязательности `docs/status.md` в текущем пользовательском контракте.
- Resolution: reviewer call 2: `VERDICT OK`; прямой пользовательский контракт признан узким исключением, первый blocker воспроизводимо опровергнут.

### Review
- Verdict: OK
- Calls: 3
- Fix iterations: 0
- Details: Итоговый fresh read-only reviewer проверил полный текущий diff, AC evidence, обязательный `docs/status.md` и resolved A-01/B01; findings отсутствуют, `VERDICT OK`.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: E-01

- Source: `docs/bootstrap.md`, раздел 6.2, строка задачи `E-01`
- Status: REVIEW_READY
- Updated: 2026-10-27T19:58:09+03:00
- Baseline HEAD: `b36fdf4dad22b1bd0daa5aa06b5da7aa5b7d4f40`

### Progress
- Implemented: добавлены `.gitignore` для root `.env`, `.runtime`, SQLite и Python-generated файлов; `.env.example` с двумя явными placeholder-значениями; `config/generations/g001.env` с точными несекретными identifiers из spec; targeted validator и final diff gate прошли.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.2, `E-01`, критерий проверки | PASS | fast: Python validator, exit 0, 79 ms — 6 secret/runtime/SQLite paths игнорируются, `.env.example` и `config/generations/g001.env` не игнорируются, 12 generation identifiers точно совпадают с `docs/spec.md` §3.5, secret-like keys в metadata отсутствуют, оба password values являются `replace-with-a-local-secret`; 3 negative mutations обнаружены. `git diff --check`, exit 0. LSP: `docs/status.md` clean; три non-code файла не имеют настроенного LSP и проверены validator-ом. |

### Issues
none

### Review
- Verdict: OK
- Calls: 2
- Fix iterations: 0
- Details: итоговый fresh read-only reviewer проверил полный текущий tracked/untracked diff, E-01 evidence/status и сохранность A-01; BLOCKER, DEFERRED и IMPROVEMENT отсутствуют, `VERDICT OK`.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: E-02

- Source: `docs/bootstrap.md`, раздел 6.2, строка задачи `E-02`; технические требования — разделы 4.1–4.3
- Status: REVIEW_READY
- Updated: 2026-10-29T10:02:11+03:00
- Baseline HEAD: `4153dd8c2eb90bec5b0d74e5753ca5b8f29cdcb2`

### Progress
- Implemented: добавлен `compose.yaml` для PostgreSQL, single-node Kafka KRaft и Kafka Connect с exact digest refs, healthchecks, loopback-only host ports, fail-closed password interpolation, generation labels, resource limits/log rotation и семью раздельными named volumes, явно покрывающими все image-declared volume paths; consumer service остаётся отдельной задачей `E-05`.
- Remaining: none в scope E-02; container startup/smoke требует отдельного разрешения и относится к последующим runtime probes.

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.2, `E-02`, критерий проверки | PASS | fast final state: Compose config с `.env.example` + `g001.env`, exit 0; validator сверил exact image metadata/versions, 3 healthchecks, limits 5.5 CPU/7 GiB, loopback ports, 7/7 image-declared volume paths → 7 уникальных generation-labeled named volumes и обнаружил 4 negative mutations, включая удалённый mount; отсутствие admin secret даёт ожидаемый exit 1. `git diff --check`, exit 0; LSP diagnostics: 0 findings. Container startup не выполнялся без отдельного разрешения. |

### Issues
#### E-02/B01
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: первый reviewer-вызов не запустился из-за временной ошибки аутентификации провайдера.
- Evidence: reviewer call 1 завершился infrastructure failure: OAuth refresh `invalid_grant`; содержательный review не выполнен.
- Unblock: после подтверждённого пользователем восстановления соединения повторить тот же fresh read-only review.
- Resolution: reviewer call 2 успешно выполнился в read-only fresh context и вернул содержательный `VERDICT BLOCK`.

#### E-02/B02
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.2 `E-02`; lifecycle/disk bounds
- Problem: Kafka/Connect image-declared volume paths не полностью покрыты явными mounts, что создаёт пять неуправляемых anonymous volumes.
- Evidence: image inspect: оба образа объявляют `/kafka/config`, `/kafka/data`, `/kafka/logs`; исходный Compose явно монтировал только Kafka `/kafka/data`.
- Unblock: добавить раздельные generation-scoped named volumes для всех Kafka/Connect paths и валидатор полного покрытия.
- Resolution: validator финального Compose подтвердил покрытие 7/7 image-declared paths семью уникальными named volumes с generation/purpose labels; mutation с удалённым Connect `/kafka/logs` mount корректно обнаружена.

#### E-02/B03
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FINAL_EVIDENCE_AND_DIFF_GATE`
- Problem: после reviewer call 3 поле Progress/Remaining ошибочно продолжало указывать на ожидаемый re-review при уже полученном `VERDICT OK`.
- Evidence: финальное перечитывание секции показало противоречие между `Remaining` и Review/Status.
- Unblock: исправить Remaining на фактическое состояние и выполнить свежий read-only review изменённой статусной записи.
- Resolution: reviewer call 4 подтвердил согласованность исправленной записи и полного diff, `VERDICT OK`.

### Review
- Verdict: OK
- Calls: 4
- Fix iterations: 2
- Details: итоговый fresh read-only reviewer проверил полный финальный diff и исправленную секцию статуса, подтвердил устранение E-02/B02 и E-02/B03, вернул `VERDICT OK`; runtime smoke/readiness и persistence после restart остаются DEFERRED до отдельного разрешения.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: E-03

- Source: `docs/bootstrap.md`, раздел 6.2, строка задачи `E-03`; технические требования — разделы 4.4 и 7.2
- Status: REVIEW_READY
- Updated: 2026-10-29T12:35:54+03:00
- Baseline HEAD: `7c04def139ca9fa751cf204ab70517a75a6cb62b`

### Progress
- Implemented: добавлены source schema/constraints/triggers, deterministic seed и identity positions; least-privilege connector role, exact publication и startup verifier; streaming/delete/schema-change exercises; runtime access checker с positive SELECT и ожидаемым SQLSTATE `42501`; Compose подключает init scripts и fail-closed connector secret.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.2, `E-03`, критерий проверки | PASS | standard runtime: pinned PostgreSQL startup/health, exit 0, 6814 ms; in-container `verify-access.sh`, exit 0, 206 ms, подтвердил catalog/grants/publication/seed/identity positions, connector SELECT и denied UPDATE с SQLSTATE `42501`; после negative probes sequence восстановлена и checker повторно прошёл. PostgreSQL config assertion: `server_version_num=160013`, `wal_level=logical`, `max_slot_wal_keep_size=1GB`, exit 0, 141 ms. Identity/FK negative probes обнаружили SQLSTATE `428C9`, `23000`, `23503`, exit 0, 411 ms. Service остановлен за 604 ms; volume/network сохранены. Текущая попытка подтвердила применимость evidence: HEAD и protected docs неизменны, stopped container config hash совпадает с current Compose, fast checks прошли за 72 ms. |
| `docs/spec.md` §9, `AC-001` (source/seed contribution) | PASS | standard runtime: PostgreSQL содержит exact deterministic seed `customers=2`, `orders=2`, `payments=1`; catalog/seed verifier и connector-role SELECT прошли. Полный snapshot/connector AC остаётся scope последующих задач. |
| `docs/spec.md` §9, `AC-002` (source exercise contribution) | NOT_RUN | Exercise SQL статически проверен, но source operations намеренно не выполнялись до connector task, чтобы сохранить initial seed для snapshot; полный CDC AC относится к последующим E-04/R-02/R-05. |
| `docs/spec.md` §9, `AC-013` (role/grants/negative DML contribution) | PASS | standard runtime: role attributes и effective privileges проверены через catalogs; connector SELECT seed counts `2\|2\|1`; UPDATE завершился ожидаемым SQLSTATE `42501`; PostgreSQL checker exit 0. Полный connector snapshot/streaming AC относится к E-04. |

### Issues
#### E-03/B01
- State: RESOLVED
- Category: PERMISSION
- Affects: `docs/bootstrap.md` §6.2 `E-03`; `AC-001`, `AC-002`, `AC-013`; обязательный gate `EVIDENCE_GATE`
- Problem: критерий E-03 требует реальных PostgreSQL catalog/seed/negative-DML assertions, но container startup и создание generation-scoped volume/network требуют отдельного явного разрешения.
- Evidence: exact PostgreSQL image успешно pulled/inspected и digest совпал; `.env` secret preconditions и Compose config прошли с redacted output. Project containers/volumes/networks отсутствовали (`initial_stopped`); host `psql` отсутствует.
- Unblock: пользователь явно разрешает generation-scoped startup pinned PostgreSQL `g001` и выполнение in-container access checker.
- Resolution: разрешение получено; preflight подтвердил exact image, redacted `.env` preconditions, 16 Docker CPU, ~23.5 GiB memory, свободный port 5432 и отсутствие project resources. PostgreSQL достиг healthy, runtime checker прошёл на финальном состоянии, затем task container остановлен; generation volume/network сохранены.

#### E-03/B02
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §4.4/§6.2 `E-03`; observable delete exercise; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: delete exercise допускает `DELETE 0` для всех трёх строк и после этого успешно подтверждает только их отсутствие, не гарантируя создание delete events.
- Evidence: reviewer call 1, P1/BLOCKER: `postgres/exercises/02-delete.sql` проходит при запуске до streaming fixture или после частичного прогона, хотя observable deletes/tombstones не возникают; `VERDICT BLOCK`.
- Unblock: проверять `ROW_COUNT = 1` для каждого delete в порядке `payments → orders → customers`, повторить affected validator и fresh re-review.
- Resolution: каждый delete теперь выполняется в отдельном `DO` и требует `ROW_COUNT = 1`; affected fast checks прошли, validator обнаружил mutation с удалённым row-count guard. Также password перенесён из argv в psql `\\getenv`, а `postgres/` смонтирован read-only для exact in-container checker path.

#### E-03/B03
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FINAL_EVIDENCE_AND_DIFF_GATE`
- Problem: после reviewer call 2 поле Progress/Remaining продолжало указывать на ожидаемый re-review, хотя `VERDICT CODE_REVIEW_ONLY OK` уже получен.
- Evidence: финальное перечитывание секции выявило stale next-state description после успешного call 2.
- Unblock: исправить Remaining на фактический runtime-only остаток и выполнить fresh read-only review изменённой status-записи.
- Resolution: reviewer call 3 подтвердил исправленную status-секцию, сохранность code diff и отсутствие новых blockers; `VERDICT CODE_REVIEW_ONLY OK`.

#### E-03/B04
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FINAL_EVIDENCE_AND_DIFF_GATE`
- Problem: после итогового reviewer call 4 поле Progress/Remaining содержало stale фразу об ожидаемом review; её исправление является содержательной правкой status-записи и требует нового fresh review, но лимит 4 reviewer-вызовов текущей попытки исчерпан.
- Evidence: call 4 вернул `VERDICT OK`, после чего финальное перечитывание выявило противоречие между `Status: REVIEW_READY` и фразой `итоговый fresh review ожидается отдельно` в Progress/Remaining.
- Unblock: в новой попытке E-03 подтвердить принадлежность текущего diff, проверить применимость runtime evidence и выполнить один fresh read-only review исправленной status-записи без повторения реализации.
- Resolution: текущая попытка подтвердила применимость evidence; fresh read-only reviewer проверил исправленную status-запись и полный diff, findings отсутствуют, `VERDICT OK`.

### Review
- Verdict: OK
- Calls: 1
- Fix iterations: 0
- Details: fresh read-only reviewer проверил полный tracked/untracked diff, исправленную status-запись, применимость сохранённого runtime evidence, scope и сохранность чужих секций; findings отсутствуют, `VERDICT OK`.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: E-04

- Source: `docs/bootstrap.md`, раздел 6.2, строка задачи `E-04`; технические требования — разделы 4.5 и 7.2
- Status: REVIEW_READY
- Updated: 2026-10-29T13:28:20+03:00
- Baseline HEAD: `75fb90d921f1c6e19d8a74ce95dbafade8ad769a`

### Progress
- Implemented: добавлены generation-safe topic bootstrap для трёх table topics и transaction topic, exact retention/partition verification, g001 Debezium connector config, proxy-free Python stdlib registration/status checker со строгой проверкой password placeholder, Connect env config-provider и read-only mounts; runtime/restart assertions прошли, initial_stopped state восстановлен без удаления volumes.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.2, `E-04`, критерий проверки | PASS | standard: exact pinned services стали healthy за 27990 ms; bootstrap создал/проверил 3 table topics по 3 partitions и transaction topic по 1 partition с RF=1, delete/24h/256MiB/64MiB configs, 13120 ms; connector config совпал, connector/task `RUNNING`, slot `postgres_cdc_g001` active. После review-fix mock обнаружил чужой password placeholder/cross-generation topic, 765 ms; runtime strict config/topic recheck прошёл, 43506 ms. Final fast, exit 0, 98 ms; LSP 0 findings. |
| `docs/spec.md` §9, `AC-001` (connector snapshot/streaming contribution) | PASS | standard: Kafka содержит exact initial snapshot `op=r`: customers=2, orders=2, payments=1; source table/marker assertions и end-offset sums `2/2/1` прошли, 23137 ms; connector/task достигли `RUNNING`. SQLite projection остаётся scope consumer tasks. |
| `docs/spec.md` §9, `AC-013` (least-privilege connector contribution) | PASS | standard: connector с `cdc_connector`, pre-created publication и `publication.autocreate.mode=disabled` выполнил snapshot; PostgreSQL checker подтвердил catalog/grants/SELECT и denied DML, exit 0; slot=`pgoutput|logical|cdc_lab|active`, publication tables exact, 521 ms. |
| `docs/spec.md` §9, `AC-014` (connector lifecycle/restart contribution) | PASS | standard: Connect restart сохранил connector offsets и тот же slot; connector/task снова `RUNNING`, table-topic end offsets остались `2/2/1`, новых snapshot records нет, slot active; 26091 ms. Новое streaming source change и sink assertion остаются последующим full acceptance. |

### Issues
#### E-04/B01
- State: RESOLVED
- Category: PERMISSION
- Affects: `docs/bootstrap.md` §6.2 `E-04`; `AC-001`, `AC-013`, `AC-014`; обязательные gates `EVIDENCE_GATE` и `FRESH_READ_ONLY_REVIEW`
- Problem: runtime-критерий E-04 требует запуска generation-scoped Kafka/Connect/PostgreSQL и регистрации connector, а container startup и обращения к Docker daemon требуют отдельного явного разрешения.
- Evidence: предыдущая попытка остановилась после fast/static проверок без обращения к Docker daemon.
- Unblock: пользователь явно разрешает запуск pinned g001 services, создание generation-scoped topics/connector/slot и read-only runtime assertions без удаления volumes или данных.
- Resolution: разрешение получено; preflight подтвердил exact local image digests, ресурсы/диск, непустые local secrets и initial_stopped state; runtime выполнен и сервисы снова остановлены, volumes/network сохранены.

#### E-04/B02
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.2 `E-04`; обязательный gate `EVIDENCE_GATE`
- Problem: connector registration script предполагает наличие `jq`, которого нет в exact pinned Connect image.
- Evidence: первый runtime registration завершился exit 69 — `required command is unavailable: jq`; image probe подтвердил `python3`, но `jq=missing`.
- Unblock: заменить jq-зависимость на доступный Python 3 stdlib JSON/HTTP path, повторить static negative tests и runtime registration.
- Resolution: Python stdlib checker выполнился внутри exact Connect image; idempotent config verification и connector/task `RUNNING`, exit 0, 204 ms.

#### E-04/B03
- State: RESOLVED
- Category: CODE
- Affects: connector registration runtime path; обязательный gate `EVIDENCE_GATE`
- Problem: Python stdlib HTTP client наследует proxy environment и может отправить loopback Kafka Connect request через proxy вместо локального API.
- Evidence: targeted mock после jq-removal получил HTTP 403 от proxy вместо local test server.
- Unblock: использовать explicit proxy-free urllib opener для generation-local Connect API и повторить create/idempotent/mismatch/secret-redaction mock.
- Resolution: proxy-free opener; targeted mock подтвердил create, idempotent recheck, worker-added name, mismatch detection и отсутствие secret в output, exit 0, 761 ms.

#### E-04/B04
- State: RESOLVED
- Category: CODE
- Affects: `AC-001`, `AC-013`; connector authentication and обязательный gate `EVIDENCE_GATE`
- Problem: Debezium Connect image преобразует в worker properties только переменные с префиксом `CONNECT_`; исходные config-provider/topic-creation env names не активировали свойства worker.
- Evidence: registration получил HTTP 400 password authentication failure при успешном PostgreSQL access checker; worker properties отсутствовали, entrypoint обрабатывает только `CONNECT_*`.
- Unblock: использовать `CONNECT_CONFIG_PROVIDERS`, `CONNECT_CONFIG_PROVIDERS_ENV_CLASS`, `CONNECT_TOPIC_CREATION_ENABLE`, пересоздать только Connect container и повторить assertions.
- Resolution: recreated Connect worker содержит exact `config.providers`, provider class и `topic.creation.enable=false`; connector зарегистрирован с environment-resolved password и достиг `RUNNING`.

#### E-04/B05
- State: RESOLVED
- Category: CODE
- Affects: connector idempotency verification; обязательный gate `EVIDENCE_GATE`
- Problem: Kafka Connect GET config добавляет служебное поле `name`, а verifier требовал буквальное равенство только исходному `config` object.
- Evidence: connector зарегистрирован, затем verifier завершился exit 1; единственным дополнительным полем было exact `name=postgres-cdc-g001`.
- Unblock: разрешить только matching `name`, сохранив строгую проверку всех остальных полей; повторить mock mutation и runtime idempotent registration/status.
- Resolution: exact matching name принят, остальные поля сравниваются строго; mismatch mutation обнаружена, runtime повторная регистрация не создала connector заново и подтвердила `RUNNING`.

#### E-04/B06
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.2 `E-04`; exact connector config и обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: verifier проверяет только наличие `database.password`, но не требует сохранения approved `${env:POSTGRES_CONNECTOR_PASSWORD}` placeholder.
- Evidence: reviewer call 1: существующий connector с literal password или другим env placeholder прошёл бы equality check после удаления password из actual/expected; `VERDICT BLOCK`.
- Unblock: сравнивать password field с expected placeholder без вывода значения, добавить negative mutation и повторить affected runtime check/fresh review.
- Resolution: verifier требует exact placeholder и выдаёт redacted diagnostic; targeted mock обнаружил `${env:OTHER_PASSWORD}`, runtime existing connector прошёл strict check и остался `RUNNING`. Неблокирующая рекомендация reviewer также выполнена: table topics обязаны соответствовать prefix и быть уникальными.

#### E-04/B07
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FINAL_EVIDENCE_AND_DIFF_GATE`
- Problem: после reviewer call 2 поле Progress/Remaining всё ещё указывало на ожидаемый review, хотя содержательный diff уже получил `VERDICT OK`.
- Evidence: финальное перечитывание секции выявило stale Remaining после успешного call 2.
- Unblock: исправить Remaining на фактическое `none` и выполнить fresh read-only review изменённой status-записи.
- Resolution: reviewer call 3 подтвердил исправленную status-запись, неизменность code diff и отсутствие blockers; `VERDICT OK`.

### Review
- Verdict: OK
- Calls: 3
- Fix iterations: 2
- Details: call 1 обнаружил blocker password-placeholder; после targeted fix call 2 подтвердил содержательный diff/runtime evidence; call 3 проверил финальную status-запись и неизменный code diff, findings отсутствуют, `VERDICT OK`.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: E-05

- Source: `docs/bootstrap.md`, раздел 6.2, строка задачи `E-05`; технические требования — разделы 4.1 и 4.6; exact versions — `docs/spec.md` §1 и §11.2
- Status: REVIEW_READY
- Updated: 2026-10-29T15:05:14+03:00
- Baseline HEAD: `6f1e748d9d0862e1d6f35e76100187838ae1f0db`

### Progress
- Implemented: добавлены `consumer/Dockerfile` с exact Python image digest, non-root runtime и build-time version probe; `consumer/requirements.txt` с exact `confluent-kafka==2.16.0`; `consumer/check-runtime.py` с проверкой версий Python client и linked librdkafka.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.2, `E-05`, критерий проверки | PASS | fast final state: `python3 -m py_compile`, targeted validator и 4 negative mutations, exit 0, 63 ms; `git diff --check`, exit 0. Standard: exact API probe подтвердил `version()='2.16.0'`, `libversion()[0]='2.16.0'`, exit 0, 4797 ms; network-enabled build exact base/dependency прошёл, build-time import/version assertion вывел `confluent-kafka 2.16.0; librdkafka 2.16.0`, exit 0, 6459 ms; два network-disabled one-shot container probes подтвердили Python `3.12.12`, uid/gid `10001`, writable `/data`, read-only app directory, positive versions и detection мутированной expected version, exit 0, 980 ms. |

### Issues
#### E-05/B01
- State: RESOLVED
- Category: PERMISSION
- Affects: `docs/bootstrap.md` §6.2 `E-05`; обязательные gates `EVIDENCE_GATE` и `FRESH_READ_ONLY_REVIEW`
- Problem: финальная сборка требует загрузить exact Python dependency из сети, а network-enabled dependency download/build не разрешены текущим запросом явно.
- Evidence: base image exact digest доступен локально; build с `--network=none --pull=false` дошёл до `pip install`, затем завершился exit 1 из-за недоступной сети; итоговый image `cdc-lab-consumer:g001` не создан, project services не запускались.
- Unblock: пользователь явно разрешает network-enabled `docker build` для загрузки `confluent-kafka==2.16.0`, после чего агент повторяет build, запускает container version/import probe и fresh read-only review.
- Resolution: пользователь дал разрешение; network-enabled build успешно загрузил exact CPython 3.12 wheel `confluent-kafka==2.16.0` и дошёл до build-time version assertion.

#### E-05/B02
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.2 `E-05`; обязательный gate `EVIDENCE_GATE`
- Problem: `check-runtime.py` ошибочно индексирует результат `confluent_kafka.version()`, поэтому сравнивает ожидаемую версию со строкой `2` вместо `2.16.0`.
- Evidence: разрешённый build установил exact package, затем завершился exit 1 с диагностикой `expected 2.16.0, got 2`; итоговый image не создан.
- Unblock: подтвердить фактические return types `version()`/`libversion()` в exact runtime, минимально исправить checker и повторить build/container probe.
- Resolution: exact runtime probe подтвердил `version()` как строку и `libversion()` как tuple; checker исправлен на `version()`, affected fast checks прошли, повторный build и positive/negative container probes завершились exit 0.

#### E-05/B03
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FINAL_EVIDENCE_AND_DIFF_GATE`
- Problem: после reviewer call 1 поле Progress/Remaining продолжало указывать на ожидаемый review, хотя вызов уже вернул `VERDICT OK`; исправление Progress не входит в узкое исключение служебной финализации.
- Evidence: post-review перечитывание секции после call 1 и call 2 показывало review-state текст в Progress/Remaining; оба reviewer-вызова вернули `VERDICT OK`, но прямое исправление Progress не входит в узкое исключение служебной финализации.
- Unblock: установить Remaining в `none` и выполнить fresh read-only review финальной status-записи и неизменного code diff.
- Resolution: reviewer call 3 проверил финальную Progress/status-запись и неизменный code diff; stale Progress отсутствует, findings нет, `VERDICT OK`.

### Review
- Verdict: OK
- Calls: 3
- Fix iterations: 2
- Details: три fresh read-only reviewer-вызова проверили полный tracked/untracked diff, final-state evidence и status consistency; calls 1–3 вернули `VERDICT OK`, итоговый call 3 подтвердил финальную Progress-запись и закрытие B03 без findings.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.
