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

## TASK_ID: R-01

- Source: `docs/bootstrap.md`, раздел 6.3, строка задачи `R-01`; behavioral contract — `docs/spec.md` §§3.1, 3.6 и 9.1
- Status: REVIEW_READY
- Updated: 2026-10-29T15:41:00+03:00
- Baseline HEAD: `2d7655b001f48ee500e4881aed40c16a533d4fde`

### Progress
- Implemented: расширен bootstrap verifier: exact source columns/identity metadata, monetary precision, enabled immutable-ID triggers, RESTRICT foreign keys и отсутствие column-level write grants; runtime checker теперь отрицательно проверяет изменение `id` во всех трёх таблицах и connector DML через ожидаемые SQLSTATE.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.3, `R-01`, критерий проверки | PASS | fast final after review-fix: shell + validator с 15 static mutations и 7 mock failure modes, exit 0, 1044 ms; `git diff --check`, exit 0. Standard final: verifier дважды прошёл, exit 0, 544/523 ms; direct identity `428C9`, transactional trigger `23000`, connector DML `42501`, post-probe catalog/seed/sequence checks PASS; config/sequences 96 ms; stop 550 ms. |
| `docs/spec.md` §9, `AC-001` (source/seed contribution) | PASS | standard: оба runtime verifier run подтвердили exact три source-таблицы, seed counts/values `2/2/1` и identity positions; full sink AC остаётся последующим scope. |
| `docs/spec.md` §9, `AC-002` (source-model contribution) | PASS | standard final: exact schema/constraints/triggers прошли; direct assignments всех `id` отклонены `428C9`, а transactional `SET id = DEFAULT` для каждой таблицы достиг immutable-ID trigger и отклонён `23000`; sequences восстановлены в `3:false|103:false|1002:false`. Full CDC streaming относится к R-02/R-05. |
| `docs/spec.md` §9, `AC-013` (role/publication contribution) | PASS | standard: runtime verifier подтвердил role attributes, CONNECT/USAGE/SELECT, отсутствие database/schema/table/column write privileges, exact publication; connector UPDATE отклонён SQLSTATE `42501`. |

### Issues
#### R-01/B01
- State: RESOLVED
- Category: PERMISSION
- Affects: `docs/bootstrap.md` §6.3 `R-01`; обязательные gates `EVIDENCE_GATE` и `FRESH_READ_ONLY_REVIEW`
- Problem: новые PostgreSQL catalog assertions и immutable-ID SQLSTATE probes должны быть выполнены на exact g001 runtime, но container startup требует отдельного явного разрешения.
- Evidence: пользователь явно разрешил продолжение с запуском контейнеров; preflight подтвердил initial_stopped, exact pinned image ID/digest, существующий read-only bind `/opt/cdc/postgres`, generation volume и свободный port 5432. Fast/mock checks прошли, но runtime assertion ещё не выполнен.
- Unblock: основной агент запускает только `cdc-lab-g001-postgres-1`, выполняет verifier и останавливает container без удаления volumes/network.
- Resolution: пользователь дал разрешение; exact existing PostgreSQL container стал healthy за 5886 ms, runtime verifier был запущен, а cleanup восстановил stopped state с exit 0 без удаления volumes/network.

#### R-01/B02
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.3 `R-01`; обязательный gate `EVIDENCE_GATE`
- Problem: verifier не выполняется из-за неоднозначности между PL/pgSQL variable `table_name` и одноимённой колонкой `information_schema.columns`.
- Evidence: runtime attempt 1 exit 3 в column-array query; после alias fix attempt 2 exit 3 в двух monetary `information_schema.columns` subqueries. PostgreSQL стал healthy за 5951 ms, cleanup снова восстановил stopped state.
- Unblock: квалифицировать обе monetary subqueries отдельными aliases, расширить validator на отсутствие любых unqualified `table_name` в `information_schema` queries и повторить runtime verifier.
- Resolution: обе monetary subqueries квалифицированы; validator обнаружил 8 mutations, а следующий runtime запуск выполнил весь catalog DO block и перешёл к immutable-ID probes.

#### R-01/B03
- State: RESOLVED
- Category: TEST
- Affects: обязательный gate `EVIDENCE_GATE`
- Problem: первая targeted mutation заменила только одну из трёх ссылок `source_column.table_name`, а validator проверял лишь наличие хотя бы одной ссылки и не обнаружил частичную mutation.
- Evidence: affected-fast exit 1: `mutation 4 was not detected`; фактическое число qualified references равно 3.
- Unblock: требовать exact count трёх qualified `table_name` references и повторить mutation/check.
- Resolution: validator теперь требует exact count трёх qualified `source_column.table_name` references; affected-fast прошёл за 25 ms и обнаружил все 6 mutations, включая частичную потерю alias.

#### R-01/B04
- State: RESOLVED
- Category: TEST
- Affects: `docs/bootstrap.md` §6.3 `R-01`; immutable-ID assertion
- Problem: direct `SET id = 9001` probes подтверждают identity mode через `428C9`, но не исполняют immutable-ID row trigger и потому не доказывают его behavior.
- Evidence: reviewer call 1: при сохранённых trigger names/wiring, но удалённой проверке `NEW.id/OLD.id`, catalog assertions и все `428C9` probes прошли бы; `SET id = DEFAULT` мог бы изменить ключ.
- Unblock: сохранить `428C9` identity probes и добавить transactional `SET id = DEFAULT` для каждой таблицы с ожидаемым trigger SQLSTATE `23000`, обязательным rollback и восстановлением `last_value/is_called` sequences.
- Resolution: transactional `SET id = DEFAULT` probes для customers/orders/payments получили ожидаемый trigger SQLSTATE `23000`; каждая операция выполняется внутри rollback-bound transaction, sequence state сохраняется/restores, а финальный catalog verifier подтверждает seed и positions. Два последовательных runtime run прошли.

#### R-01/B05
- State: RESOLVED
- Category: TEST
- Affects: обязательный gate `EVIDENCE_GATE`; sequence restoration trigger probes
- Problem: sequence-state parser ожидает `t/f`, тогда как PostgreSQL text concatenation возвращает `true/false`.
- Evidence: post-review runtime fix-run: PostgreSQL стал healthy за 5960 ms, catalog verifier прошёл (`DO`), затем checker завершился `unexpected sequence state for public.customers_id_seq`; cleanup восстановил stopped state.
- Unblock: принимать exact `true|false`, обновить mock negative case и повторить fast/runtime checks.
- Resolution: parser принимает exact `true|false`; final fast mutation обнаруживает возврат к `t/f`, два runtime verifier run и independent config/sequence query подтвердили `3:false|103:false|1002:false`.

### Review
- Verdict: OK
- Calls: 2
- Fix iterations: 1
- Details: call 1 обнаружил BLOCKER trigger behavior; после targeted fix call 2 проверил полный финальный diff/evidence, подтвердил безопасный rollback/sequence restore и отсутствие blockers, `VERDICT OK`. DEFERRED: publication operation flags/row filters/column lists. IMPROVEMENT: hardening на случай принудительного завершения shell между sequence-consuming probe и restore.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: R-02

- Source: `docs/bootstrap.md`, раздел 6.3, строка задачи `R-02`; behavioral contract — `docs/spec.md` §§3.2–3.5, 5.1, 9.1, `AC-001`, `AC-008`, `AC-014`
- Status: REVIEW_READY
- Updated: 2026-10-29T16:27:04+03:00
- Baseline HEAD: `3fdd4204dcbcb7ed804af510af351a14e10b186b`

### Progress
- Implemented: exact g001 verifier проверяет ordered key/envelope/table/source/transaction descriptors, required optionality и snapshot enum metadata, полный row set/Decimal fixtures, per-key partition/order и полный Kafka history (`low=0`, contiguous offsets, `position=high`); generation guard включает connector/slot/publication. Две fix-итерации и final pre/post-restart runtime прошли; services возвращены в stopped state, volumes/network сохранены.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.3, `R-02`, наблюдаемый выход | PASS | fast final: `py_compile`, self-test, `git diff --check`, exit 0, 0.08 s; LSP 0 findings. Mutations обнаруживают transaction/snapshot/partition/history, missing/wrong amount, missing schema, composite key, wrong source optionality, optional op и reordered source/envelope. Standard final: exact full-history run до/после restart — `r=5,c=3,u=3`, 11 records, 9 complete partition states, exit 0, 13.45/15.29 s. |
| `docs/spec.md` §9, `AC-001` (connector/Kafka contribution) | PASS | final: все 5 snapshot events проверены внутри полного history — ordered embedded schemas, exact keys/rows, Decimal `120.50/49.99`, initial markers, nonzero LSN; все partition low=0 и прочитаны до high. SQLite raw/projection остаются scope R-03–R-05. |
| `docs/spec.md` §9, `AC-008` (topic-independence contribution) | PASS | final: три exact g001 topics × 3 partitions; same table/key остаётся в одной partition и exact offset order, cross-topic order не предполагается; rows/topics/history проверены независимо. SQLite child-before-parent относится к R-04/R-05. |
| `docs/spec.md` §9, `AC-014` (connector lifecycle contribution) | PASS | final: Connect restart 2.54 s; pre/post earliest/latest идентичны и low=0; post-restart verifier снова exact `r=5,c=3,u=3`, connector/task `RUNNING`, slot `postgres_cdc_g001|pgoutput|cdc_lab|active`. Sink assertion относится к R-05. |

### Issues
#### R-02/B01
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.3 `R-02`; `AC-001`; обязательные gates `EVIDENCE_GATE`, `FRESH_READ_ONLY_REVIEW`
- Problem: verifier проверяет только subset row fields и имя value envelope, поэтому может принять отсутствующие/неверные amounts, неполную embedded schema или дополнительное key field.
- Evidence: reviewer call 1: synthetic self-test проходит с сокращённым `after` и `value.schema={name}`; `amount`/`total_amount` отсутствуют в expected fixtures; key schema требует лишь наличие `id`; `VERDICT BLOCK`.
- Unblock: проверять exact key/schema/envelope/table fields, декодировать Kafka Connect Decimal, добавить negative mutations и повторить live snapshot/streaming.
- Resolution: rows/Decimal/key shape, ordered schema field descriptors, exact source optionality, `op.optional=false` и snapshot enum metadata проверяются; targeted mutations wrong optionality/optional op/reordered source/reordered envelope обнаружены; два final live run прошли на pinned Debezium schema.

#### R-02/B02
- State: RESOLVED
- Category: TEST
- Affects: `AC-001`, `AC-014`; обязательные gates `EVIDENCE_GATE`, `FRESH_READ_ONLY_REVIEW`
- Problem: verifier начинает с текущего log start и не требует `low=0`, поэтому после retention может принять неполный suffix как полную generation history.
- Evidence: reviewer call 1: полученный low watermark сохраняется только как начальная позиция; synthetic offsets можно сдвинуть на 100 без failure; runtime evidence не фиксировало log-start offsets; `VERDICT BLOCK`.
- Unblock: fail-closed требовать `low=0` для всех девяти partitions, выводить low/high/final position и добавить negative mutation ненулевого low.
- Resolution: verifier требует `low=0`, contiguous `range(low, high)` и `position=high`, выводит state каждой partition; mutation low=1 обнаружена; pre/post-restart runtime подтвердил неизменные low/high и полный read.

### Review
- Verdict: OK
- Calls: 3
- Fix iterations: 2
- Details: call 1 выявил B01/B02; call 2 подтвердил B02 и часть B01, затем выявил schema order/optionality blocker; call 3 проверил полный финальный diff/status, подтвердил обе fixes и final evidence, findings отсутствуют, `VERDICT OK`.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: R-03

- Source: `docs/bootstrap.md`, раздел 6.3, строка задачи `R-03`; behavioral contract — `docs/spec.md` §§4.1–4.5 и 9, `AC-001`–`AC-008`, `AC-012`
- Status: REVIEW_READY
- Updated: 2026-10-29T16:54:22+03:00
- Baseline HEAD: `24221df6c19794844f4150ee2eb60e4613e20728`

### Progress
- Implemented: atomic migration runner с checksum/history validation; v1 schema/version/generation binding в одной transaction; immutable singleton binding защищён от INSERT/REPLACE/UPDATE/DELETE; insert/update triggers изолируют generation во всех six raw/error/projection/watermark tables; source упакован в consumer image path; 11 targeted unit/negative tests и container probes прошли.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.3, `R-03`, наблюдаемый выход | PASS | fast final: 11 unit/negative tests, `py_compile`, `git diff --check`, exit 0, 0.22 s; LSP 0 findings. Standard final: exact Python image suite exit 0, 0.54 s; network-none packaged-source build 0.63 s; packaged REPLACE/atomicity/generation probe exit 0, 0.47 s; temporary image removed. |
| `docs/spec.md` §9, `AC-001`–`AC-008` (SQLite schema contribution) | PASS | Assertions подтвердили raw bytes/headers/coordinate PK, independent projections, ordering/delete watermark, checksum/idempotency, ordinary + inline-COMMIT rollback, atomic initial binding, no pending migration on wrong generation, immutable binding including `INSERT OR REPLACE`, reject g002 insert и update для всех six scoped tables. Полные behavioral AC требуют R-04–R-06. |
| `docs/spec.md` §9, `AC-012` (ingestion error schema contribution) | PASS | Durable error bytes/attempt count проверены; g002 error insert/update в g001 sink отклоняются; rebinding через REPLACE запрещён. Полный fail-stop/offset AC относится к R-04/R-05. |

### Issues
#### R-03/B01
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.3 `R-03`; обязательный gate `EVIDENCE_GATE`
- Problem: изменение общего Dockerfile `ENV` до dependency layer инвалидировало существующий cache, поэтому network-disabled final image build пытался повторно скачать exact dependency.
- Evidence: первый `docker build --network=none --pull=false --tag cdc-lab-consumer:r03 consumer`, exit 1 после 9.15 s; pip не смог получить `confluent-kafka==2.16.0`. После перестановки второй clean build также не имел BuildKit cache, exit 1 после 8.86 s; сеть намеренно не включалась.
- Unblock: сохранить dependency prefix Dockerfile byte-identical baseline E-05, проверить только новый source suffix через derived image от ранее проверенного exact image и выполнить runtime probes без сети.
- Resolution: assertion подтвердил неизменный Dockerfile prefix через dependency/user creation; network-none derived build от `cdc-lab-consumer:g001` прошёл за 0.59 s, packaged migration/negative generation probe — за 0.66 s; временный image удалён, project services остались stopped, volumes/network сохранены.

#### R-03/B02
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.3 `R-03`; atomic migration contract
- Problem: transaction-statement guard видит только начало строки; inline `COMMIT` может зафиксировать partial DDL до последующей ошибки без записи migration version.
- Evidence: reviewer call 1 `VERDICT BLOCK`; targeted reproduction с `CREATE TABLE leaked ...; COMMIT; INSERT INTO missing_table...` оставил `leaked`, а `max(schema_migrations.version)=1`. Первая fix-проверка: unit suite exit 1 — authorizer также запретил runner rollback (`sqlite3.DatabaseError: not authorized`), поэтому B02 остаётся OPEN.
- Unblock: запретить transaction control SQLite-aware способом независимо от форматирования и добавить regression, проверяющий отсутствие DDL/version после inline `COMMIT` failure.
- Resolution: runner теперь split-ит SQL statements, владеет `BEGIN IMMEDIATE` и запрещает `SQLITE_TRANSACTION`/`SQLITE_SAVEPOINT` через authorizer только для migration statements; authorizer снимается до rollback. Regression с inline `COMMIT` не оставляет table/version; финальные host/exact-image/container checks прошли.

#### R-03/B03
- State: RESOLVED
- Category: CODE
- Affects: generation isolation; `AC-001`–`AC-008`, `AC-012`
- Problem: generation columns не связаны с `sink_metadata`; pending migrations применяются до проверки existing binding, а initial migration и первое binding имеют crash window.
- Evidence: reviewer call 1 `VERDICT BLOCK`; targeted reproduction записал g002 ingestion error в g001 sink и применил `pending_leak` migration до ожидаемого bind mismatch. Reviewer call 2 подтвердил прежние fixes, но выявил `INSERT OR REPLACE` bypass; targeted reproduction успешно сменил binding g001→g002.
- Unblock: проверять binding до pending migrations, атомарно связывать initial migration с первым binding и fail-closed отклонять generation mismatch для всех generation-scoped tables; добавить negative regressions.
- Resolution: добавлен `BEFORE INSERT` guard при существующей singleton row, поэтому `INSERT OR REPLACE`/`REPLACE` abort до conflict replacement; update/delete guards сохранены. Regression проверяет g002 insert и update во всех six scoped tables, все binding mutation paths и сохранность g001; exact-image и packaged probes прошли.

### Review
- Verdict: OK
- Calls: 3
- Fix iterations: 2
- Details: call 1 выявил B02/B03; call 2 подтвердил B02 и выявил REPLACE bypass; call 3 проверил полный финальный tracked/untracked diff, закрытие обоих классов дефектов, status/evidence и вернул `VERDICT OK` без BLOCKER/IMPROVEMENT. DEFERRED: full end-to-end AC остаются R-04–R-06; clean network dependency rebuild не требуется для неизменного E-05 prefix.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: R-04

- Source: `docs/bootstrap.md`, раздел 6.3, строка задачи `R-04`; behavioral contract — `docs/spec.md` §§5.1–5.6 и 9, `AC-003`, `AC-005`, `AC-006`, `AC-012`
- Status: REVIEW_READY
- Updated: 2026-10-29T17:45:53+03:00
- Baseline HEAD: `e8b013b40251be929258c800ec3fc8ba84659866`

### Progress
- Implemented: добавлены strict parser/embedded-schema validation для `before`/`after`, signed-int64 bounds, lexicographic ordering token, generation-bound atomic processor, historical collision detection, delete/tombstone/stale/duplicate semantics и durable fail-stop errors.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.3, `R-04`, наблюдаемый выход | PASS | final current attempt: host suite 26 tests, exit 0, 0.47 s; exact-image read-only/network-none suite 26 tests, exit 0, 0.55 s; twelve mutations detected, включая reject-valid-empty-strings; packaged-source build/import probe exit 0, 0.82/0.42 s; `git diff --check` exit 0; LSP 0 findings; temp image removed. |
| `docs/spec.md` §9, `AC-003` | PASS | current host/exact-image targeted test подтверждает create→delete→tombstone archive, watermark-before-delete trigger, отсутствие projection row и неизменность watermark после tombstone. |
| `docs/spec.md` §9, `AC-005` | PASS | current host/exact-image targeted test подтверждает newer applied, older archived `stale`, late snapshot stale по `phase_rank`, projection/watermark не откатываются. |
| `docs/spec.md` §9, `AC-006` | PASS | current host/exact-image targeted test подтверждает old update после delete как `stale`, отсутствие resurrection и сохранённый delete watermark. |
| `docs/spec.md` §9, `AC-012` | PASS | current host/exact-image tests подтверждают durable fail-stop для malformed envelope/key/op/ordering/schema/projection и oversized signed-int64 в `before`/`after`; original bytes/diagnostics и repeated attempt count сохранены. Kafka offset commit остаётся R-05 scope. |

### Issues
#### R-04/B01
- State: RESOLVED
- Category: TEST
- Affects: обязательный gate `EVIDENCE_GATE`; negative/mutation evidence для comparator/delete/error paths
- Problem: первый mutation harness запустил тестовый модуль из рабочего репозитория вместо временной мутированной копии, поэтому mutation comparator не была обнаружена.
- Evidence: harness сообщил `MUTATION_NOT_DETECTED stale-comparator`, но команда импортировала `consumer.tests...` из текущего cwd при `PYTHONPATH` только для temp `src`; обычные host и exact-image suites при этом прошли по 20 tests.
- Unblock: запускать временный test file из temp cwd или через discovery temp tree, затем повторить четыре mutations и подтвердить ожидаемые failures.
- Resolution: исправленный harness запускал discovery из каждой temp-копии и обнаружил пять mutations: reversed stale comparator, skipped watermark, skipped delete, skipped error upsert и skipped generation check.

#### R-04/B02
- State: RESOLVED
- Category: TEST
- Affects: standard packaged-source probe; обязательный gate `EVIDENCE_GATE`
- Problem: первый derived-image import probe не задал `PYTHONPATH`, потому что использовал старый verified base image как foundation и не повторил актуальный Dockerfile `ENV`; импорт нового модуля завершился `ModuleNotFoundError`.
- Evidence: network-none derived build и `py_compile` прошли, затем runtime probe exit 1; inspect ранее показывал отсутствие `PYTHONPATH` в env base image. Cleanup удалил только временный image `cdc-lab-consumer:r04-check`.
- Unblock: повторить derived Dockerfile с `ENV PYTHONPATH=/opt/cdc/consumer/src`, затем выполнить network-none import/runtime probe и удалить временный image.
- Resolution: corrected derived Dockerfile задал актуальный `PYTHONPATH`; network-none build/py_compile прошёл за 0.73 s, read-only import/token probe — за 0.39 s; temporary image удалён.

#### R-04/B03
- State: RESOLVED
- Category: CODE
- Affects: `docs/spec.md` §5.3 equal-token fail-stop; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: collision проверяется только против current watermark; повтор token ранее архивированного stale event на другой Kafka coordinate ошибочно снова архивируется как stale.
- Evidence: reviewer call 1: sequence token 300 applied → token 200 stale → token 200 на другом offset не вызывает `OrderingCollisionError`; `VERDICT BLOCK`.
- Unblock: искать equal ordering token во всей raw history того же generation/table/key до insert и добавить regression для повторённого stale token.
- Resolution: query ищет token во всей raw history generation/table/key; `newest → stale → repeated stale token` regression и skip-history-collision mutation подтверждают fail-stop с rollback raw и durable error.

#### R-04/B04
- State: RESOLVED
- Category: CODE
- Affects: `AC-012`; missing/invalid primary-key fail-stop
- Problem: empty/missing Kafka key отклоняется до protected transaction/error-upsert path; `KafkaRecord.key` не допускает `None`, поэтому исходный nullable key нельзя сохранить в `ingestion_errors`.
- Evidence: reviewer call 1: `key=b''` вызывает `ValueError` до `try`, error row/attempt count отсутствуют; `VERDICT BLOCK`.
- Unblock: разрешить nullable key, отделить persistable coordinate от content validation, писать nullable key безопасно и добавить repeated-attempt regression.
- Resolution: `KafkaRecord.key` допускает `None`; missing/empty key валидируются внутри protected path, nullable bytes upsert безопасен; repeated-attempt regression подтверждает `attempt_count=2`, allow-missing-key mutation обнаружена.

#### R-04/B05
- State: RESOLVED
- Category: CODE
- Affects: `AC-003`; `docs/spec.md` §5.3 delete ordering invariant
- Problem: processor физически удаляет projection row до обновления delete watermark, вопреки обязательному порядку.
- Evidence: reviewer call 1: `_write_projection()` вызывается перед `_write_watermark()` для всех operations; одна transaction обеспечивает rollback, но не требуемый operation order; `VERDICT BLOCK`.
- Unblock: для delete сначала записывать watermark, затем удалять projection; добавить trigger/trace regression, наблюдающий watermark во время DELETE.
- Resolution: delete branch пишет watermark до projection delete; test trigger наблюдает новый delete watermark внутри `BEFORE DELETE`; reversed-order mutation обнаружена, final host/exact-image suites проходят.

#### R-04/B06
- State: RESOLVED
- Category: TEST
- Affects: обязательный gate `EVIDENCE_GATE`; post-review mutation evidence
- Problem: post-fix mutation harness остановился после первой mutation, потому что общий target `_write_watermark(...)` теперь встречается в delete и non-delete branches дважды.
- Evidence: `stale-comparator` mutation обнаружена, затем harness завершился exit 1 с `mutation target count=2`; exact-image final suite 23 tests прошла. Вторая попытка с block target не декодировала literal `\\n` и также остановилась до test execution.
- Unblock: использовать уникальный branch block и декодировать escaped newlines в mutation helper, затем повторить все eight mutations.
- Resolution: corrected helper заменил escaped newlines на реальные; все eight mutations обнаружены, включая skip-history-collision, allow-missing-key и reversed delete/watermark order.

#### R-04/B07
- State: RESOLVED
- Category: CODE
- Affects: `AC-012`; incompatible numeric type durable fail-stop; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: `_require_int` не ограничивает Connect `int64` сверху; oversized key/domain/ordering values могут вызвать Python SQLite `OverflowError` вне `ProcessingError` path и не записать `ingestion_errors`.
- Evidence: reviewer call 2: `orders.after.customer_id = 2**63` проходит parser, SQLite bind выбрасывает `OverflowError`; raw rollback есть, durable error/attempt count отсутствуют; `VERDICT BLOCK`.
- Unblock: enforce signed int64 upper bound для всех embedded int64, добавить repeated-attempt regression и mutation, повторить host/exact-image/packaged-source checks и fresh review.
- Resolution: введён `INT64_MAX=2**63-1` в общей int validation; oversized `orders.customer_id` regression дважды получает `RecordValidationError`, raw/projection отсутствуют, original bytes и attempt count сохраняются; allow-oversized-int64 mutation обнаружена, final 24-test host/exact-image suites проходят.

#### R-04/B08
- State: RESOLVED
- Category: CODE
- Affects: `AC-012`; malformed embedded `before` values; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: update/delete `before` payload проверяется только как object/id; остальные non-null embedded values обходят type/int64 validation.
- Evidence: reviewer call 3: `orders.before.customer_id = 2**63` принимается и event может архивироваться/применяться; B07 regression покрывает только `after`; `VERDICT BLOCK`.
- Unblock: валидировать каждое non-null `before` field по embedded row schema с поддержкой null old-row fields; добавить repeated-attempt update/delete regressions, повторить checks/mutation и последний fresh review.
- Resolution: `_validate_before_row` проверяет exact fields и каждый non-null value по embedded schema, сохраняя допустимые null old-row fields; update/delete oversized regressions дважды подтверждают durable error/no raw/projection; skip-update/delete-before mutations обнаружены, final suites проходят.

#### R-04/B09
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FINAL_EVIDENCE_AND_DIFF_GATE`; согласованность итоговой status-записи
- Problem: перед reviewer call 4 поле Progress/Remaining ошибочно оставили в pre-review состоянии; его исправление не входит в узкое исключение служебной финализации после итогового OK, а лимит четырёх reviewer-вызовов исчерпан.
- Evidence: reviewer call 4 вернул `VERDICT OK`, но финальное перечитывание обнаружило stale фразу `выполнить четвёртый ... reviewer-вызов`; содержательное изменение Progress требует fresh review, недоступный в текущей попытке по бюджету.
- Unblock: новая попытка R-04 подтверждает неизменность code diff/evidence, исправляет Progress/Remaining и выполняет один fresh read-only review финальной секции.
- Resolution: текущая попытка повторно подтвердила evidence; reviewer call 2 проверил финальные `Progress/Remaining`, полный diff и status consistency, подтвердил B03–B10 и вернул `VERDICT OK`.

#### R-04/B10
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §6.3 `R-04`; source/projection parity; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: row validation требует non-empty для всех domain strings, хотя source DDL разрешает пустые `customers.email` и `payments.provider_ref`.
- Evidence: reviewer call 1 текущей попытки: `processing.py` отклоняет `email=''`/`provider_ref=''`, а `postgres/init/10-source-schema.sql` задаёт только `TEXT NOT NULL UNIQUE` без non-empty CHECK; `VERDICT BLOCK`.
- Unblock: разрешить пустые строки ровно для полей, где source contract это допускает; добавить after и update/delete before regressions, affected mutation/checks и fresh review.
- Resolution: field-aware string validation разрешает пустые значения только для `customers.email` и `payments.provider_ref`; regression применяет empty after и update/delete before values, проверяет projection/raw outcomes; reject-valid-empty-strings mutation обнаружена, final 26-test host/exact-image suites проходят.

### Review
- Verdict: OK
- Calls: 2
- Fix iterations: 1
- Details: call 1 выявил R-04/B10; после targeted fix call 2 проверил полный финальный tracked/untracked diff, status consistency и отсутствие регрессий B03–B08, вернул `VERDICT OK`. IMPROVEMENT: усилить negative oracle, временно разрешающий пустые строки для других domain fields; текущая реализация корректно их отклоняет. DEFERRED: runtime evidence не перезапускалось read-only reviewer tools.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: R-05

- Source: `docs/bootstrap.md`, раздел 6.3, строка задачи `R-05`; behavioral contract — `docs/spec.md` §§4.2, 5.4 и 9, `AC-002`, `AC-004`, `AC-014`
- Status: REVIEW_READY
- Updated: 2026-10-29T18:14:34+03:00
- Baseline HEAD: `a9c926d29f6b22a78c70a8a2e8f97cba6b091b78`

### Progress
- Implemented: добавлены sequential consume loop, ручная synchronous commit boundary с проверкой partition result, generation-scoped Compose consumer/volume, exact-coordinate crash-window fault injection и targeted unit/mutation tests; exact-image и live g001 assertions прошли.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.3, `R-05`, наблюдаемый выход | PASS | fast final after review-fix: host suite 34 tests, exit 0, 0.53 s; `py_compile`, resolved Compose config (`consumer.restart=no`) и `git diff --check`, exit 0; five targeted mutations обнаружены. standard: exact-image suite 34 tests, exit 0, 0.10 s; runtime versions 2.16.0. Live consumer последовательно обработал history/streaming records, synchronous commits дали lag 0. |
| `docs/spec.md` §9, `AC-002` (consumer/commit contribution) | PASS | standard live: initial consumer обработал full Kafka history `r=5,c=3,u=3`; raw=11, projection точно совпала с source `customers=3/orders=3/payments=2`, ingestion errors=0, committed lag=0. Последующие streaming updates применены; final raw=17, source/sink возвращены к deterministic fixture (`Alice Cooper`, `Bob Brown`), lag=0. |
| `docs/spec.md` §9, `AC-004` | PASS | acceptance final через actual Compose service: fault at `customers:1:4` завершил container `exited|86|restart_count=0` и оставался exited через 3 s; raw count=1/delivery=1 и projection durable, group offset остался 4 при log-end 5. Только explicit `compose up --force-recreate consumer` повторно получил coordinate (`duplicate=true`), сохранил raw count=1, увеличил delivery_count=2 и committed offset 5/lag 0; fixture cleanup также committed, final lag=0. |
| `docs/spec.md` §9, `AC-014` (sink contribution) | PASS | acceptance live: Connect restart достиг connector/task `RUNNING` за 33.1 s с тем же active slot `postgres_cdc_g001`; после source update новый record `customers:2:2` применён в sink за 1.60 s, snapshot raw count остался 5 и group lag=0. Cleanup updates также обработаны, final source/sink fixture восстановлен без нового snapshot. |

### Issues
#### R-05/B01
- State: RESOLVED
- Category: ENVIRONMENT
- Affects: обязательный gate `EVIDENCE_GATE`; exact-image standard checks
- Problem: network-disabled build финального Dockerfile не имеет BuildKit cache dependency layer и пытается повторно скачать `confluent-kafka==2.16.0`.
- Evidence: `docker build --network=none --pull=false --tag cdc-lab-consumer:r05-check consumer`, exit 1 после 8.6 s; exact dependency недоступна без сети, существующий проверенный image `cdc-lab-consumer:g001` доступен локально.
- Unblock: построить derived image от ранее проверенного exact `cdc-lab-consumer:g001`, скопировав только новый source/CMD без dependency download, и выполнить exact-image tests/probes.
- Resolution: derived network-none build от exact verified image прошёл за 8.8 s; exact-image suite 33 tests прошла за 0.59 s, runtime version probe подтвердил confluent-kafka/librdkafka 2.16.0 за 0.47 s.

#### R-05/B02
- State: RESOLVED
- Category: TEST
- Affects: container runtime path; обязательный gate `EVIDENCE_GATE`
- Problem: derived image для network-none проверки не воспроизвёл актуальный Dockerfile `PYTHONPATH`, поэтому default consumer command не может импортировать package.
- Evidence: первый one-shot consumer run, exit 1: `ModuleNotFoundError: No module named 'cdc_consumer'`; image inspect подтвердил отсутствие `PYTHONPATH`, хотя source files присутствуют.
- Unblock: добавить `ENV PYTHONPATH=/opt/cdc/consumer/src` в derived image, повторить exact-image default-command import/test probe и one-shot runtime.
- Resolution: corrected derived image содержит `PYTHONPATH=/opt/cdc/consumer/src`; import probe и exact-image 33-test suite прошли, после чего default one-shot consumer успешно обработал/закоммитил live records.

#### R-05/B03
- State: RESOLVED
- Category: CODE
- Affects: `AC-004`, fail-stop contract и обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: consumer наследует общий Compose `restart: unless-stopped`, поэтому controlled fault, malformed event или commit failure автоматически перезапускаются вместо устойчивого fail-stop до явного operator restart.
- Evidence: reviewer call 1, `VERDICT BLOCK`: `compose.yaml` consumer не переопределяет service-default restart policy; live AC-004 выполнялся one-shot `compose run`, а не основным Compose service.
- Unblock: установить consumer `restart: "no"`, добавить config regression и повторить AC-004 через фактический service с проверкой exited(86), неизменного group offset и explicit restart duplicate/commit.
- Resolution: consumer явно задаёт `restart: "no"`; resolved Compose config и static regression это подтверждают. Actual service fault остался `exited|86|restart_count=0` через 3 s с lag 1, а duplicate/offset commit произошли только после explicit recreate; five-mutation harness обнаруживает удаление override.

#### R-05/B04
- State: RESOLVED
- Category: TEST
- Affects: exact-image standard checks after R-05/B03 fix
- Problem: новый Compose regression test ожидает repository-root `compose.yaml`, но первый exact-image test invocation смонтировал только `consumer/tests`.
- Evidence: exact-image 34-test suite, exit 1: `FileNotFoundError: /opt/cdc/compose.yaml`; остальные 33 tests прошли.
- Unblock: смонтировать `compose.yaml` read-only в ожидаемый `/opt/cdc/compose.yaml` и повторить exact-image suite.
- Resolution: exact-image suite повторена с read-only mount `compose.yaml` в `/opt/cdc/compose.yaml`; все 34 tests прошли за 0.10 s.

### Review
- Verdict: OK
- Calls: 2
- Fix iterations: 1
- Details: call 1 выявил blocker R-05/B03; после targeted restart-policy fix call 2 проверил полный финальный tracked/untracked diff, actual Compose-service evidence, закрытие R-05/B03/B04 и вернул `VERDICT OK` без findings.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.

## TASK_ID: E-06

- Source: `docs/bootstrap.md`, раздел 6.2, строка задачи `E-06`; command contract — разделы 4.7 и 7.1
- Status: REVIEW_READY
- Updated: 2026-10-29T22:45:44+03:00
- Baseline HEAD: `c22211a21f299a0fa08599300e3552900ee9eecc`

### Progress
- Implemented: в текущей status-only попытке PRECHECK подтвердил тот же HEAD и согласованный E-06 diff; code-файлы не менялись после итогового reviewer call 4, protected docs совпадают с approved fingerprints, project services stopped, 8 volumes и network сохранены. Safe command interfaces и shared verifier завершены; B06/B11–B18 закрыты, final evidence применимо.
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/bootstrap.md` §6.2, `E-06`, критерий проверки | PASS | Применимость final evidence подтверждена в текущей status-only попытке: HEAD `c22211a...`, tracked/untracked code scope и SHA-256 пяти E-06 code-файлов зафиксированы; их mtimes предшествуют итоговому reviewer artifact, после него менялся только `docs/status.md`; protected spec/idea fingerprints совпадают, services stopped, 8 volumes/network сохранены. Current fast: shell syntax PASS 4 ms; exact-image Python 3.12.12 suite 34 tests PASS 908 ms; `git diff --check` PASS. Сохранённое final evidence: 43 safety mutations, exact labels для 8 volumes/4 containers, targeted catalog/publication/table-property mutations, live lag с numeric `safe_wal_size`, exact destroy mocks и hostile-env smoke exit 0, 67.60 s. Real destroy и pull не выполнялись; acceptance/new-generation fail closed до E-07/R-09/R-08. |

### Issues
#### E-06/B01
- State: RESOLVED
- Category: CODE
- Affects: `docs/bootstrap.md` §4.7 read-only `preflight`; обязательный gate `EVIDENCE_GATE`
- Problem: preflight использует daemon `DockerRootDir` как host path; в Docker Desktop `/var/lib/docker` находится внутри VM и недоступен из WSL host, поэтому безопасная disk-проверка ошибочно блокируется.
- Evidence: `scripts/lab preflight`, exit 69: `cannot inspect Docker storage path read-only: /var/lib/docker`; daemon сообщает `OperatingSystem=Docker Desktop`, а read-only `df /mnt/wsl/docker-desktop` доступен.
- Unblock: добавить fail-closed выбор доступного read-only storage path для native Engine, WSL Docker Desktop и явного operator override; повторить preflight и negative mutation.
- Resolution: preflight выбрал доступный WSL Docker Desktop path, подтвердил 11 GiB free и завершился exit 0; override на несуществующий path завершился ожидаемым exit 69.

#### E-06/B02
- State: RESOLVED
- Category: CODE
- Affects: `scripts/check unit`; обязательный gate `EVIDENCE_GATE`
- Problem: `trap RETURN` обращается к local-переменной `output` после выхода из функции под `set -u`, поэтому успешно прошедшие 34 tests превращаются в exit 1.
- Evidence: `scripts/check unit`: `Ran 34 tests ... OK`, затем `line 123: output: unbound variable`, итоговый exit 1.
- Unblock: использовать exit-safe cleanup с немедленно подставленным temp path, повторить non-empty unit suite.
- Resolution: exit-safe temp cleanup; `scripts/check unit` выполнил 34 tests в exact image с `network=none`, exit 0, 0.58 s.

#### E-06/B03
- State: RESOLVED
- Category: CODE
- Affects: `scripts/lab bootstrap g001`, `scripts/check smoke`; обязательный gate `EVIDENCE_GATE`
- Problem: повторный bootstrap всегда запускает initial-seed verifier; корректно эволюционировавшая persistent g001 после streaming имеет другие source values и ошибочно блокируется.
- Evidence: smoke поднял exact services, затем `verify-access.sh` завершился exit 3 с `customers seed mismatch`; cleanup вернул все services в stopped state, volumes/network сохранены.
- Unblock: full seed verifier выполнять только до первой регистрации connector; для существующего connector проверять текущие runtime/grant/publication invariants без требования initial fixture, затем повторить smoke.
- Resolution: connector registration probe выбрал existing-generation path; runtime/grant/publication/slot DO block, topic verification, connector idempotency, consumer start и full `scripts/lab status g001` прошли на evolved persistent state.

#### E-06/B04
- State: RESOLVED
- Category: TEST
- Affects: `scripts/check smoke`; обязательный gate `EVIDENCE_GATE`
- Problem: smoke сравнивает display `server_version`, содержащий Debian package suffix, со строкой `16.13` и ложно падает на точном pinned runtime.
- Evidence: PostgreSQL вернул `16.13 (Debian 16.13-1.pgdg12+1)`; smoke завершился exit 1 после успешных bootstrap/status assertions; cleanup восстановил stopped state.
- Unblock: проверять exact numeric `server_version_num=160013`, повторить smoke.
- Resolution: post-B04 smoke проверил `server_version_num=160013` и завершился exit 0 за 57.77 s; последующие regression runs повторно подтвердили assertion, актуальный final smoke после fix 3 — 67.60 s с восстановлением initial_stopped.

#### E-06/B05
- State: RESOLVED
- Category: TEST
- Affects: обязательный gate `EVIDENCE_GATE`; mutation coverage command safety contracts
- Problem: B06 runtime mutation harness передал host `/tmp/*.sql` как container path, поэтому psql завершился до выполнения mutation.
- Evidence: expected function-body probe exit 1: `psql: error: /tmp/e06-function-body.sql: No such file or directory`; предыдущие validator target/path ошибки сохранены в истории этой проблемы.
- Unblock: передавать SQL через stdin либо mounted repository path, повторить function-body/extra-trigger/extra-FK/unvalidated-FK probes и normal verifier.
- Resolution: mutation SQL передаётся в psql через stdin; function-body/extra-trigger/extra-FK/unvalidated-FK probes выполнились в container и дали ожидаемый exit 3, normal verifier после rollback прошёл.

#### E-06/B06
- State: RESOLVED
- Category: CODE
- Affects: idempotent `scripts/lab bootstrap g001`; least-privilege/source invariants; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: existing-generation verifier пропускает source columns, immutable-ID triggers, foreign keys и column-level DML grants, поэтому повторный bootstrap может принять опасный drift.
- Evidence: reviewer calls 1–3 последовательно выявили grant/trigger/FK drift и подтвердили их fixes; reviewer call 4, P1: verifier всё ещё не проверяет PK/UNIQUE/CHECK constraints и column defaults, поэтому `ALTER TABLE public.payments DROP CONSTRAINT payments_pkey` проходит catalog predicates; `VERDICT BLOCK`.
- Unblock: в новой попытке E-06 добавить exact полный набор `p/u/c/f` constraints через `pg_get_constraintdef(..., true)` + validation и exact column defaults через `pg_get_expr`, targeted mutation минимум для dropped PK, затем affected checks и fresh review.
- Resolution: verifier exact-сравнивает полный набор `p/u/c/f` constraints через `pg_get_constraintdef(..., true)` с validation/deferrability flags и полный набор defaults через `pg_get_expr`; 8 transactional mutations, включая `DROP payments_pkey`, дали ожидаемый exit 3, normal verifier после rollback прошёл.

#### E-06/B07
- State: RESOLVED
- Category: CODE
- Affects: generation-scoped destructive boundary; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: destroy scope проверяет containers/volumes, но не network, которую затем удаляет `compose down --volumes`.
- Evidence: reviewer call 1, P1: network с отсутствующим/чужим generation label проходит guard и остаётся целью удаления; `VERDICT BLOCK`.
- Unblock: fail-closed проверить exact project network name и project/generation labels до destructive command; добавить negative assertion без удаления ресурсов.
- Resolution: guard требует ровно одну network, exact name `cdc-lab-g001_cdc-lab`, Compose project label и generation `g001`; live labels подтверждены без удаления, а mutation удаления network-list guard обнаружена validator-ом.

#### E-06/B08
- State: RESOLVED
- Category: CODE
- Affects: `scripts/lab lag g001`; `docs/spec.md` §6; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: slot query с нулём строк даёт psql exit 0, поэтому lag может успешно завершиться без третьей обязательной метрики.
- Evidence: reviewer call 1, P1: отсутствующий `postgres_cdc_g001` печатает пустую WAL-секцию и не меняет exit status; `VERDICT BLOCK`.
- Unblock: потребовать ровно одну строку ожидаемого slot, валидный `wal_status` и fail-stop для missing/lost; добавить targeted negative assertion.
- Resolution: pure validator принимает valid slot row и возвращает ожидаемый exit 69 для empty/lost; live `scripts/lab lag g001` вывел exact slot, `wal_status=reserved`, retained bytes и threshold, exit 0.

#### E-06/B09
- State: RESOLVED
- Category: CODE
- Affects: `scripts/lab lag g001`; `docs/spec.md` §6 / AC-009 contribution; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: lag требует running consumer, хотя контракт требует диагностику при временно остановленном/fail-stop consumer.
- Evidence: reviewer call 2, P1: `require_running_service consumer` завершает command exit 69 до Kafka/event-age/WAL metrics; `VERDICT BLOCK`.
- Unblock: при stopped consumer читать generation sink через ephemeral network-none read-only inspector с exact read-only volume, сохраняя running path; проверить оба состояния.
- Resolution: lag больше не требует running consumer; при stopped/exited consumer exact generation-labeled sink volume монтируется read-only в ephemeral `--network none --read-only --rm` inspector. Live test после core-only `lab up` подтвердил consumer state `exited` и все три метрики, exit 0.

#### E-06/B10
- State: RESOLVED
- Category: TEST
- Affects: обязательный gate `EVIDENCE_GATE`; final static mutation validator
- Problem: validator заменяет только первое вхождение safety marker, а presence assertion проходит за счёт второго вхождения и не обнаруживает mutation.
- Evidence: final fast завершился exit 1: `mutation not detected scripts/lab --pull never --no-build`; unit suite до этой точки не запускался.
- Unblock: mutation harness должен удалять все вхождения либо проверять exact counts/targeted blocks, затем повторить final fast checks.
- Resolution: replacement изменён на marker-free token; post-fix validator обнаружил 30 safety mutations, `bash -n`, Compose argv/slot pure probes, exact-image 34 tests и whitespace checks прошли; final fast 1.113 s, unit 1.031 s.

#### E-06/B11
- State: RESOLVED
- Category: CODE
- Affects: generation-scoped destructive boundary; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: Compose wrapper не pin-ит project name, поэтому внешний `COMPOSE_PROJECT_NAME` может направить destructive invocation на project, отличный от проверенного guard-ом.
- Evidence: reviewer call 1, P1: `verify_destroy_scope` проверяет `$COMPOSE_PROJECT`, но `docker compose` без `--project-name` принимает environment override; `VERDICT BLOCK`.
- Unblock: всегда передавать `--project-name "$COMPOSE_PROJECT"` и добавить безопасный argv mock с внешним override.
- Resolution: Compose wrapper всегда передаёт `--project-name cdc-lab-g001`; argv mock при `COMPOSE_PROJECT_NAME=foreign` подтвердил exact target, а live `lab up` с override поднял только g001 resources и восстановил stopped state.

#### E-06/B12
- State: RESOLVED
- Category: CODE
- Affects: idempotent existing-generation bootstrap; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: publication verifier проверяет только table names и пропускает publish flags, column lists и row filters.
- Evidence: reviewer call 1, P1: `ALTER PUBLICATION ... SET (publish = 'insert')` сохраняет table rows и проходит текущий verifier; `VERDICT BLOCK`.
- Unblock: exact-проверить `pg_publication` flags и отсутствие column lists/row filters; добавить transactional mutations.
- Resolution: verifier exact-сверяет `pg_publication` flags и raw `pg_publication_rel` без `prattrs`/`prqual`; publish flags, column-list и row-filter mutations дали expected exit 3, normal verifier после rollback прошёл.

#### E-06/B13
- State: RESOLVED
- Category: CODE
- Affects: `scripts/lab lag g001`; `docs/spec.md` §6.1; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: lag query/validator не выводит `safe_wal_size`, доступный в закреплённом PostgreSQL 16.
- Evidence: reviewer call 1, P1: output содержит slot/status/retained bytes/threshold, но не обязательный доступный `safe_wal_size`; `VERDICT BLOCK`.
- Unblock: добавить поле в query/output, валидировать numeric/NULL semantics и targeted pure mutations.
- Resolution: lag query/output включает `safe_wal_size`; validator требует non-negative numeric value для usable slot и по-прежнему fail-stop для `lost`. Pure valid/missing/non-numeric/lost cases и live numeric output прошли.

#### E-06/B14
- State: RESOLVED
- Category: CODE
- Affects: generation isolation; idempotent bootstrap; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: exported generation-scoped variables override validated `--env-file` values and могут направить startup/topic creation в foreign namespace.
- Evidence: reviewer call 2, P1: hostile `KAFKA_TOPIC_PREFIX/topics`, connector/slot/publication/group/sink overrides имеют Compose precedence; Kafka bootstrap может создать foreign topics до later conflict; `VERDICT BLOCK`.
- Unblock: fail-closed удалить exported overrides перед Compose interpolation либо pin exact validated values; добавить hostile config/runtime mock для всех generation-scoped keys.
- Resolution: Compose wrapper в subshell удаляет 15 validated generation/secret variable overrides до чтения env files и отдельно pin-ит `CDC_GENERATION`; hostile `compose config` не содержал sentinel и подтвердил 18 exact g001 service values.

#### E-06/B15
- State: RESOLVED
- Category: CODE
- Affects: generation-scoped destructive boundary; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: volume guard перечисляет только resources с ожидаемой project label; exact Compose-declared volume с missing/foreign label исключается из проверки, но остаётся целью `down --volumes`.
- Evidence: reviewer call 2, P1: label-filter enumeration fail-open для `cdc-lab-g001_consumer-data` и других declared volumes; `VERDICT BLOCK`.
- Unblock: direct inspect восьми exact declared volume names и exact labels до destructive invocation; сохранить rejection неожиданных project volumes и добавить non-destructive mock mutation.
- Resolution: guard direct-inspect-ит восемь exact declared volume names и требует exact project/generation/purpose labels, затем отклоняет неожиданные project-scoped volumes. Wrong-label mock завершился expected exit 65; exact-label positive mock достиг только mocked `compose down --volumes`.

#### E-06/B16
- State: RESOLVED
- Category: TEST
- Affects: обязательный gate `EVIDENCE_GATE`; B15 positive destroy-scope mock
- Problem: первый positive mock exact labels завершился exit 65 до mocked `compose down`; temp diagnostics были удалены cleanup trap.
- Evidence: `scripts/lab destroy g001 --confirm g001` под mock Docker, exit 65; stdout/stderr не сохранены после trap, поэтому assertion не подтверждён.
- Unblock: повторить mock с сохранением diagnostics, исправить mock либо guard по наблюдаемой причине и получить positive + wrong-label outcomes без real destroy.
- Resolution: diagnostics выявили реальный label spelling `com.docker.compose.oneoff=False`; guard исправлен на exact value. Corrected positive mock прошёл полный path: `down` без `--volumes` и 8 exact `volume rm` calls.

#### E-06/B17
- State: RESOLVED
- Category: CODE
- Affects: idempotent existing-generation bootstrap; delete/update CDC; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: source verifier не проверяет table replica identity и принимает `REPLICA IDENTITY NOTHING` при сохранённых PK/constraints.
- Evidence: reviewer call 3, P1: `ALTER TABLE public.payments REPLICA IDENTITY NOTHING` не меняет текущие predicates, но ломает публикуемые UPDATE/DELETE; `VERDICT BLOCK`.
- Unblock: exact-проверить ordinary permanent table properties и `relreplident='d'` для всех source tables; добавить transactional mutation и normal pass.
- Resolution: verifier exact-сверяет ordinary/permanent/default-replica-identity/non-RLS/non-partitioned properties всех source tables; `REPLICA IDENTITY NOTHING` и `ENABLE ROW LEVEL SECURITY` mutations дали expected exit 3, normal verifier после rollback прошёл.

#### E-06/B18
- State: RESOLVED
- Category: CODE
- Affects: generation-scoped destructive boundary; обязательный gate `FRESH_READ_ONLY_REVIEW`
- Problem: `compose down --volumes` может удалить anonymous volumes, прикреплённые к target containers, вне восьми validated named volumes.
- Evidence: reviewer call 3, P1: guard не инспектирует mounts, а `--volumes` расширяет destructive target; `VERDICT BLOCK`.
- Unblock: безопасный вариант — `compose down` без `--volumes`, затем повторно проверить и удалить явно только 8 exact named volumes; добавить mock, запрещающий broad volume flag/unknown removal.
- Resolution: destructive path выполняет Compose `down` без `--volumes`, затем повторно валидирует и удаляет только 8 exact named volumes; exact project/service/oneoff container set также проверяется. Positive mock подтвердил отсутствие broad flag/unknown removal и ровно 8 removals.

#### E-06/B19
- State: RESOLVED
- Category: REVIEW
- Affects: обязательный gate `FINAL_EVIDENCE_AND_DIFF_GATE`; согласованность итоговой status-записи
- Problem: перед reviewer call 4 поле `Progress/Remaining` содержало pre-review действие; после `VERDICT OK` оно стало stale, а изменение Progress не входит в узкую служебную финализацию.
- Evidence: reviewer call 4 вернул `VERDICT OK` без findings; финальное перечитывание показало `Remaining: выполнить четвёртый ... re-review`. Лимит 4 calls / 3 fixes исчерпан.
- Unblock: в новой попытке E-06 подтвердить неизменность code diff/evidence, установить `Remaining: none` до вызова и выполнить один fresh read-only review финальной status-секции.
- Resolution: текущая попытка установила `Remaining: none` до review; fresh read-only reviewer call 1 проверил полный diff, применимость evidence и status consistency, findings отсутствуют, `VERDICT OK`.

### Review
- Verdict: OK
- Calls: 1
- Fix iterations: 0
- Details: fresh read-only reviewer проверил полный tracked/untracked diff, все E-06 code-файлы, применимость evidence, исправленную Progress-запись, B19 и сохранность чужих status-секций; BLOCKER/DEFERRED/IMPROVEMENT отсутствуют, `VERDICT OK`.

### Next action
Пользователю проверить review-ready diff и при согласии создать checkpoint commit.
