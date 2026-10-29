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
