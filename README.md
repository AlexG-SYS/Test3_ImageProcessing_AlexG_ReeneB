# Test3_ImageProcessing_AlexG_ReeneB

# ImageLab (Version 3: short polling + long polling + Server-Sent Events)

Asynchronous image processing: `202 Accepted` + durable job + one background worker. The browser can observe a job with
**one-second short polling** (Assessment 1), **version-based long polling** (Assessment 2) or **Server-Sent Events** (Assessment 3), chosen in the UI.
Version 3 adds only the SSE observation path; upload, worker, database, variants and the two polling modes are unchanged (no new migration).
Version 3 changes are marked `[V3]` in the source: `grep -rn "\[V3\]" cmd ui scripts`.

> **Assessment 1 baseline:** tag `assessment-1-baseline` (commit: `________`). Create it before committing Version 2:
> `git tag assessment-1-baseline <commit>`.

Every Version 2 change in the source is marked `[V2]`: `grep -rn "\[V2\]" cmd internal ui migrations scripts`.

## Prerequisites

- Go 1.25.5
- PostgreSQL 18 (examples use port 5433; change it to match your server)
- Python 3 (only for the experiment scripts; `matplotlib` is optional, for charts)

## Environment configuration

The app is configured only through command-line flags (no `.env` file). `go run ./cmd/api -h` lists them all.

| Flag | Default | Purpose |
|---|---|---|
| `-db-dsn` | `postgresql://alex:alex@localhost/imagelab?sslmode=disable` | PostgreSQL connection string. **Include the port if it is not 5432**, e.g. `localhost:5433` |
| `-port` | `4000` | API server port |
| `-worker-poll-interval` | `250ms` | How often the worker checks for queued jobs |
| `-processing-delay` | `0` | Artificial quiet delay before real processing (**experiment condition A: `30s`**) |
| `-sim-duration` | `0` (off) | **[V2]** Length of the simulated progress phase (**C1/C2/C3: `10s`**) |
| `-sim-interval` | `2s` | **[V2]** Time between simulated updates (**Assessment 3 condition C: `200ms`**) |
| `-sse-heartbeat` | `15s` | **[V3]** Idle SSE heartbeat comment interval (`0` = off) |
| `-storage-originals-dir` | `./storage/originals` | Where uploaded originals are written |
| `-storage-variants-dir` | `./storage/variants` | Where thumbnail/preview/display files are written |

Flags must start with a dash (`-processing-delay=30s`); without it Go silently ignores them.
The simulation and delay are **off by default** and must stay off for ordinary use. At start-up the server logs an
`experiment_config` line so every log shows which settings were active.

## Database setup

```bash
# Create the role and database (adjust name/password to match your -db-dsn)
sudo -u postgres psql -p 5433 -c "CREATE ROLE alex WITH LOGIN PASSWORD 'alex';"
sudo -u postgres psql -p 5433 -c "CREATE DATABASE imagelab OWNER alex;"

# Postgres 15+ requires this explicitly, even for the owner
sudo -u postgres psql -p 5433 -d imagelab -c "GRANT ALL ON SCHEMA public TO alex;"
sudo -u postgres psql -p 5433 -d imagelab -c "GRANT CREATE ON SCHEMA public TO alex;"
```

## Migrations

Plain `.sql` files in `migrations/`. Apply every `.up.sql` in order (**000004 is new for Version 2**):

```bash
export DSN="postgresql://alex:alex@localhost:5433/imagelab?sslmode=disable"
for f in migrations/*.up.sql; do psql "$DSN" -f "$f"; done
```

Roll back by applying the `.down.sql` files in reverse order:
`for f in $(ls -r migrations/*.down.sql); do psql "$DSN" -f "$f"; done`

## Running the app

Run from the repository root (`./ui/static` and `./storage` are relative paths).

```bash
go build -o imagelab-api ./cmd/api
./imagelab-api -db-dsn="$DSN"
```

Then open `http://localhost:4000`.

## Version 3: Server-Sent Events (SSE)

### What changed for Version 3

| File | Change |
|---|---|
| `cmd/api/sse.go` | **New.** `GET /v1/jobs/{id}/events`: subscribe, capture the change channel, read the job, send the snapshot, wait, repeat. Heartbeats, per-frame write deadline, stream open/close/event log lines |
| `cmd/api/routes.go` | Registers the SSE route |
| `cmd/api/main.go` | `-sse-heartbeat` flag; atomic counters for active and peak SSE streams; heartbeat shown in the `experiment_config` log line |
| `ui/static/app.js` | `runSseObservation`: ONE `EventSource`, `job-update` listener, reconnect handling, terminal `close()`, SSE counters and raw SSE log |
| `ui/static/render.js`, `state.js`, `index.html` | Third mode radio ("SSE"), reconnecting message, SSE counters in the experiment log |
| `scripts/v3_analysis.py` | **New.** Builds the 18-run CSV from server and client logs, prints the run-by-run and summary tables, draws the required graphs |

The worker, notifier, database layer and long-poll handler are **unchanged**: the SSE handler reuses the same per-job notifier and the same persisted versions.

### The SSE endpoint

`GET /v1/jobs/{id}/events` (used by the browser `EventSource`):

- Unknown or malformed job id: `404` (normal JSON error) before any stream begins.
- Headers: `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`.
- **Initial snapshot:** the first frame is the current complete job representation.
- **Later updates:** each newer committed version produces one complete snapshot frame:

```
event: job-update
data: {"id":"...","status":"processing","version":4,"stage":"preview_ready","variants_completed":2,...}
<blank line>
```

- Every frame ends with a blank line and is flushed. Flush pushes the bytes to the browser; it does not end the response.
- **Heartbeat:** every `-sse-heartbeat` (default 15 s) an idle stream gets a comment line `: keep-alive` followed by a blank line. EventSource hides comments, so heartbeats are counted from the server log only.
- **Terminal jobs:** after the completed or failed snapshot the server ends the stream, and the browser calls `EventSource.close()`.
- No SSE `id:` field and no `Last-Event-ID` replay. The JSON `version` is a state version, not an event cursor: a reconnecting client simply receives the latest snapshot.

### How a committed version becomes an event for every tab

1. The worker commits (the version is incremented in the same UPDATE), logs `job_commit`, then calls `Notify`.
2. `Notify` closes the job's channel, which wakes **every** SSE handler for that job (and any long-poll handlers).
3. Each handler captures the new channel, **re-reads PostgreSQL**, and writes a snapshot only if its version is newer than the last one sent on that stream.
4. Because the channel is captured before the read, a commit between the read and the wait still wakes the handler. Several commits can collapse into one wake-up, so intermediate versions may be skipped, but the latest snapshot (including the terminal one) is always delivered.
5. If the browser disconnects, only that handler ends (`end_reason=cancelled`); the worker is unaffected and the browser's `EventSource` reconnects and receives the latest snapshot.

### Using SSE in the browser

1. Choose **SSE** in the Observation mode box (before or during a job) and upload. The stream opens for that job; no second upload happens.
2. **Stop watching** closes the stream; the worker keeps going. **Resume** opens one new `EventSource` for the same job and its first snapshot restores the display.
3. To see reconnecting: while a job is running, DevTools → Network → Offline for a few seconds (or restart the server). The card shows "Connection interrupted · reconnecting", the job is **not** marked failed, and the same `EventSource` reconnects.
4. Two tabs: open the app in two tabs, upload in one, paste the job id into the other tab's address as `http://localhost:4000/v1/jobs/<id>/events` to watch the raw frames, or use `curl -N http://localhost:4000/v1/jobs/<id>/events`.

```bash
curl -N http://localhost:4000/v1/jobs/<job id>/events     # watch the raw frames; the request stays open
curl -si http://localhost:4000/v1/jobs/00000000-0000-0000-0000-000000000000/events   # 404 before a stream begins
```

### Assessment 3 experiment (3 conditions x 2 methods x 3 repetitions = 18 runs)

| Cond. | Start the server with | Server log |
|---|---|---|
| A quiet interval | `-processing-delay=30s` | `results_v3/server_A.log` |
| B real progress | no experiment flags | `results_v3/server_B.log` |
| C frequent updates | `-sim-duration=10s -sim-interval=200ms` | `results_v3/server_C.log` |

```bash
mkdir -p results_v3
./imagelab-api -db-dsn="$DSN" -processing-delay=30s 2>&1 | tee -a results_v3/server_A.log
```

Per run: write your prediction first; same image (`test.jpg`); empty queue; one observer tab in the foreground; alternate the method order
(for example long, SSE, SSE, long, long, SSE). Select the method, upload, wait for the terminal state, press **Download client log (JSON)**, and
**save the file into `results_v3/`** before the next run (the log exists only in the page).

```bash
python3 scripts/v3_analysis.py ingest --results results_v3 --dry-run      # expect 3 per condition and method
python3 scripts/v3_analysis.py ingest --results results_v3                 # rebuilds runs_v3.csv and client_logs/
python3 scripts/v3_analysis.py report --csv results_v3/runs_v3.csv --out results_v3
```

The report prints the run-by-run table and the summary table (median, min-max) and writes `graph1_gets.png` (observation GET count),
`graph2_detection.png` (detection delay in ms) and `graph3_version_trace.png` (version over time, condition C). Both required graphs show the
individual repetitions, the median and the min-max range. Charts need matplotlib (`sudo apt install python3-matplotlib`).

**Counting rules:** `observation_gets` = HTTP observation GETs for long polling (including the first); for SSE it is 0 and `sse_stream_gets`
counts stream GETs (from the server's `sse_stream_open` lines). `job_events_received` counts snapshots received. Heartbeats come from the server log.
`observation_db_reads` = job reads by the observation handlers (long polling: `long_poll` `db_reads`; SSE: `sse_stream_close` `db_reads`); image lookups are not counted.
`stream_seconds` and `peak_active_streams` come from the SSE stream log lines.
Detection delay = browser `Date.now()` at receipt of the terminal snapshot minus the server `commit_us` of the terminal commit (same computer; estimate with a few ms of jitter).
Requests and streams are counted, not TCP connections: HTTP can reuse connections.

### Version 3 limitations

- Snapshot semantics only: no event history, no `Last-Event-ID` replay, no exactly-once delivery.
- One server process and one worker; notifications are in-process.
- Each SSE stream holds a handler goroutine and a database connection only while it is reading; the handler does not poll PostgreSQL while waiting.
- Browsers limit open HTTP/1.1 connections per host (about six); many SSE tabs would exhaust that limit.

---

## Version 2: long polling (retained)

## Using the two observation modes

1. In the **Processing job** card choose **Short polling** or **Long polling** before uploading (the choice persists).
2. Upload an image. The page fetches one prompt snapshot (the baseline version), then observes in the chosen mode. The card shows
   the mode, job id, last observed version, status, stage and "Variants ready: n of 3". "Simulated progress" appears only while the experiment simulation is running.
3. You can switch modes mid-job, press **Stop watching** (the worker keeps going; **Resume** observes the same job again), or
   simulate a retrieval error (DevTools → Network → Offline) and press **Try again** (same job, no new upload).
4. The **Experiment log** panel shows request counters; **Download client log (JSON)** saves the raw per-request log.

Short polling waits about 1 s after each response (never overlapping requests). Long polling sends its next request immediately
after a valid active 200 or a 204, with no artificial pause.

## What changed for Version 2

| File | Change |
|---|---|
| `migrations/000004_add_job_versions.*.sql` | **New.** `version` (default 1, backfills old jobs), `stage`, `variants_completed`, `simulation_step`, with CHECK constraints |
| `internal/data/jobs.go` | `Job` has the new fields; every change does `version = version + 1` in the same UPDATE; new `UpdateProgress` / `UpdateSimulation`; `MarkCompleted` / `MarkFailed` bump the version; updates only apply while `status = 'processing'`; `GetByPublicID` takes a context; `ClaimNext` orders by `queued_at, id` |
| `internal/data/errors.go` | New `ErrJobNotActive` |
| `cmd/api/notifier.go` | **New.** One channel per job; `Notify` closes it so every waiting handler wakes; `Release` cleans up |
| `cmd/api/jobs.go` | Keeps the prompt snapshot; adds `?after=&wait=` long polling (200 / 204 / 400 / 404), `Cache-Control: no-store`, UUID check (malformed id gives 404, not 500), per-request write deadline, one `long_poll` log line per request |
| `cmd/api/worker.go` | After every commit: log `job_commit` with `commit_us`, then notify; progress after each real variant; optional simulated phase |
| `cmd/api/main.go`, `server.go` | `-sim-duration` / `-sim-interval`, notifier, held requests cancelled at shutdown |
| `cmd/api/images.go` | The 202 body also contains `version` |
| `ui/static/*` | Mode selector, shared render function, one outstanding request, 30 s long-poll timeout, 204 continuation, Stop/Resume, mode switching with a generation token, Try again, raw client log |
| `scripts/summarize_runs.py` | Joins server and client logs into CSV rows, prints median and min–max tables, draws the charts |
| `scripts/ingest_all.py` | Builds `runs.csv` for all runs at once (matches client logs to server logs by job id) |

## API summary

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/images` | Accept an upload, return `202` with `job_id`, `version` and `status_url` |
| `GET` | `/v1/jobs/{id}` | Prompt snapshot (short polling). Never waits |
| `GET` | `/v1/jobs/{id}?after=N&wait=S` | **[V2]** Long poll: snapshot newer than version `N`, waiting up to `S` seconds (1 to 20, default 20) |
| `GET` | `/v1/jobs/{id}/events` | **[V3]** SSE stream: current snapshot immediately, then a `job-update` event for each newer committed version |
| `GET` | `/v1/images/{id}/variants/{name}` | Fetch one variant (`thumbnail` / `preview` / `display`) |
| `GET` | `/v1/healthcheck` | Liveness only |

Long-poll outcomes: client behind or job finished → `200` immediately; versions equal and job active → held; deadline with no
change → `204` (no body); `after` above the current version, `wait` without `after`, or malformed values → `400`; unknown or
malformed job id → `404`. Every `200` has the Version 1 fields plus `version`, `stage`, `variants_completed`, `simulation_step`.

## How waiting avoids missing a change

The handler (1) subscribes, (2) captures the current change channel, and (3) only then reads the job from PostgreSQL. The worker
commits first and notifies afterwards. A change before the read is seen by the read; a change after it closes the channel the
handler already holds. A notification only means "read the database again"; it never proves that processing succeeded. The
waiting handler does not poll PostgreSQL: it reads at entry, after each wake-up, and at the holding deadline.

## Experiments (5 conditions × 2 modes × 3 repetitions)

| Cond. | Start the server with | Server log |
|---|---|---|
| A quiet job | `-processing-delay=30s` | `results/server_A.log` |
| B real progress | no experiment flags | `results/server_B.log` |
| C1 slow updates | `-sim-duration=10s -sim-interval=2s` | `results/server_C1.log` |
| C2 fast updates | `-sim-duration=10s -sim-interval=200ms` | `results/server_C2.log` |
| C3 fast + latency | same as C2, plus 200 ms latency | `results/server_C3.log` |

```bash
mkdir -p results
./imagelab-api -db-dsn="$DSN" -sim-duration=10s -sim-interval=2s 2>&1 | tee -a results/server_C1.log
```

Per run: same image (`test.jpg`), empty queue, one foreground tab, alternate which mode runs first. Select the mode, upload, wait for the
terminal state, press **Download client log (JSON)** and keep the file (copy it into `results/`). The client log exists only in the page,
so download it **before** the next upload.

**C3 latency:** Chrome DevTools → Network → throttling → add a custom profile with Latency 200 ms, and keep DevTools open for the C3 runs.
Check one prompt GET shows about 200 ms "Waiting for server response" and record it. Turn throttling off for every other condition.

**Build the results** (from the repository root; both scripts live in `results/` or `scripts/`):

```bash
python3 results/ingest_all.py --client-dir results --dry-run   # check the counts per condition and mode
python3 results/ingest_all.py --client-dir results              # rebuilds results/runs.csv and results/client_logs/
python3 results/summarize_runs.py report --csv results/runs.csv --charts results/
```

This writes `runs.csv`, the median and min–max table, `chart_requests.png` and `chart_detection.png`. Charts need `matplotlib`
(`sudo apt install python3-matplotlib`). `ingest_all.py` rebuilds the CSV every time, so add hand-written notes after the last run.

**Timestamp method:** `detection_ms` = browser `Date.now()` at receipt of the terminal response minus the server's `commit_us / 1000`
(logged right after the terminal commit), both on the same computer. Server resolution is 1 µs, browser 1 ms, with a few ms of
scheduling jitter. `observation_db_reads` counts job-table reads only (long polling: sum of `db_reads` in `long_poll` lines; short polling: one per GET).

## Verifying the long-poll contract with curl

```bash
BASE=http://localhost:4000
JOB=<job id from an upload>
curl -si "$BASE/v1/jobs/$JOB"                          # prompt snapshot
curl -si "$BASE/v1/jobs/$JOB?after=0&wait=5"           # older version: immediate 200
curl -si "$BASE/v1/jobs/$JOB?after=999"                # 400: after exceeds the current version
curl -si "$BASE/v1/jobs/$JOB?wait=5"                   # 400: wait without after
curl -si "$BASE/v1/jobs/$JOB?after=1&wait=21"          # 400: wait out of range
curl -si "$BASE/v1/jobs/abc"                           # 404: malformed id
```

To see a held request, start the server with `-processing-delay=20s`, read the current `version`, then call
`?after=<version>&wait=5`: it returns `204` after about 5 s. Run the same call in two terminals or tabs to check that both observers wake on one change.

## Running tests

There is no automated test suite for the Go backend; verification was done manually with `curl`, `psql`, the browser, and the measured experiments above.

## Project layout

```
cmd/api/             HTTP handlers, routing, worker, notifier, main()
internal/data/       Database models (Image, Job, Variant)
internal/validator/  Generic validation helpers
migrations/          SQL schema, applied in order (000004 = Version 2)
scripts/ or results/ summarize_runs.py, ingest_all.py, logs, runs.csv, charts
ui/static/           Frontend (vanilla JS, no build step)
storage/             Uploaded originals and generated variants (gitignored)
```

## Known limitations

- One worker and in-process notifications only; a job can stay `processing` if the process is killed mid-job.
- `variants_completed` is updated right after the variant row is inserted (two statements); it never exceeds the stored variants.
- The final variant milestone and `completed` use two versions (`display_ready`, then `completed`).

---

## Assessment 1 measurements (short polling, curl)

Start the server with a delay:
`./imagelab-api -db-dsn="$DSN" -processing-delay=2s`

Paste this helper into a terminal to measure acknowledgement time, polls and completion with curl:

```bash
BASE=http://localhost:4000

measure() {   # usage: measure test.jpg 1
  local t0=$(date +%s.%N)
  # 1) upload; curl reports HTTP code + time to the 202 response
  local out=$(curl -s -w '\n%{http_code} %{time_total}' -F "image=@$1" $BASE/v1/images)
  local http=$(echo "$out" | tail -1 | cut -d' ' -f1)
  local ack=$(echo "$out" | tail -1 | cut -d' ' -f2)
  local url=$(echo "$out" | grep -o '"status_url": *"[^"]*"' | sed 's/.*: *"\(.*\)"/\1/')

  # 2) poll every 1s (same as the UI) and count polls
  local polls=0 status="" proc=""
  while [[ $status != completed && $status != failed ]]; do
    sleep 1
    polls=$((polls+1))
    local body=$(curl -s $BASE$url)
    status=$(echo "$body" | grep -o '"status": *"[^"]*"' | sed 's/.*: *"\(.*\)"/\1/')
    if [[ $status == processing && -z $proc ]]; then
      proc=$(awk -v a=$(date +%s.%N) -v b=$t0 'BEGIN{printf "%.2f", a-b}')
    fi
  done
  local total=$(awk -v a=$(date +%s.%N) -v b=$t0 'BEGIN{printf "%.2f", a-b}')
  echo "img#$2 http=$http ack=${ack}s first_seen_processing=${proc:-n/a}s total=${total}s polls=$polls final=$status"
  echo "   $body" | grep -o '"\(queued_at\|started_at\|completed_at\)": *"[^"]*"' | tr '\n' ' '; echo
}
```

Single image: `measure test.jpg 1`

Five images at once: `for i in 1 2 3 4 5; do measure test.jpg $i & done | sort -t= -k2`

Watch the jobs table live:

```bash
watch -n 0.5 "psql \"$DSN\" -c \"SELECT id, status, version, stage, started_at, completed_at FROM jobs ORDER BY id DESC LIMIT 5;\""
```

If `watch` fails because of authentication, add your password and username:
`watch -n 0.5 "PGPASSWORD='password' psql -h 127.0.0.1 -p 5433 -U alex -d imagelab -c \"SELECT id, status, version, stage FROM jobs ORDER BY id DESC LIMIT 5;\""`
