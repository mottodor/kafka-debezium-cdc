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
