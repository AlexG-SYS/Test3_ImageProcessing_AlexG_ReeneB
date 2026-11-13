package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/AlexG-SYS/Test3_ImageProcessing_AlexG_ReeneB/internal/data"
)

// [V3] NEW FILE — Server-Sent Events observation of one job (SSE-01 … SSE-09).
//
//	GET /v1/jobs/{id}/events
//
// ONE response stays open for as long as the job is active. Every time a newer
// committed version exists, a complete snapshot is written into that SAME response
// as a named "job-update" event and flushed. PostgreSQL stays authoritative: the
// notifier (notifier.go, unchanged) only wakes the handler, which then RE-READS the
// job before sending anything (SYNC-04).

// [V3] A stalled client must not hold a handler forever: every frame gets this
// write deadline. (It replaces the server-wide 10 s WriteTimeout for this response,
// which would otherwise cut the stream after 10 seconds.)
const sseWriteTimeout = 10 * time.Second

func (app *application) jobEventsHandler(w http.ResponseWriter, r *http.Request) {
	publicID := r.PathValue("id")
	// Malformed ids can never exist: answer 404 before any stream begins.
	if !uuidPattern.MatchString(publicID) {
		app.notFoundResponse(w, r)
		return
	}

	started := time.Now()
	rc := http.NewResponseController(w)
	dbReads, events, heartbeats := 0, 0, 0
	endReason := "error"
	streamOpen := false

	// [V3] SYNC-03/SYNC-06: register for notifications BEFORE the first read, and
	// release on every exit path. No mutex is held while reading the database or
	// writing to the network (the notifier only locks for its map operations).
	sub := app.notifier.Subscribe(publicID)
	defer sub.Release()

	// One structured line when the stream ends: the raw server-side evidence
	// (stream duration, events, heartbeats, observer DB reads, active handlers).
	defer func() {
		if !streamOpen {
			return // 404/500 before the stream began: nothing was opened
		}
		active := app.sseActive.Add(-1)
		app.logger.Info("sse_stream_close",
			"job_id", publicID,
			"end_reason", endReason,
			"stream_ms", time.Since(started).Milliseconds(),
			"events_sent", events,
			"heartbeats", heartbeats,
			"db_reads", dbReads,
			"active_streams", active,
		)
	}()

	// [V3] SSE-08: optional idle heartbeat (a comment line, not a job-update event).
	// A nil channel blocks forever, so -sse-heartbeat=0 disables it.
	var heartbeatC <-chan time.Time
	if d := app.config.sseHeartbeat; d > 0 {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		heartbeatC = ticker.C
	}

	lastSent := 0 // highest version already written on THIS stream (SYNC-05)

	for {
		// [V3] SYNC-03: capture the change channel FIRST, then read the row.
		// A commit before the read is seen by the read; a commit after it closes the
		// channel we already hold, so the wait below returns immediately.
		changed := sub.Channel()

		dbReads++
		job, err := app.models.Jobs.GetByPublicID(r.Context(), publicID)
		if err != nil {
			if !streamOpen {
				// SSE-02: still before the first byte, so a normal HTTP error is possible.
				if errors.Is(err, data.ErrRecordNotFound) {
					app.notFoundResponse(w, r)
				} else {
					app.serverErrorResponse(w, r, err)
				}
				return
			}
			if r.Context().Err() != nil {
				endReason = "cancelled"
			} else {
				endReason = "db_error"
			}
			return // stream already open: end it; EventSource will reconnect
		}

		// [V3] SSE-03: headers go out with the FIRST frame, after the 404 check above.
		if !streamOpen {
			h := w.Header()
			h.Set("Content-Type", "text/event-stream; charset=utf-8")
			h.Set("Cache-Control", "no-cache")
			h.Set("X-Accel-Buffering", "no") // ask proxies (nginx) not to buffer the stream
			streamOpen = true
			n := app.sseActive.Add(1)
			for { // keep a running maximum of simultaneously open streams
				p := app.ssePeak.Load()
				if n <= p || app.ssePeak.CompareAndSwap(p, n) {
					break
				}
			}
			app.logger.Info("sse_stream_open",
				"job_id", publicID,
				"active_streams", n,
				"peak_active_streams", app.ssePeak.Load(),
			)
		}

		// [V3] SSE-04/05 + SYNC-05: send a COMPLETE snapshot, but only if it is newer
		// than what this stream already sent. The first pass always sends (initial
		// snapshot); later passes skip duplicates, so coalesced wake-ups are harmless.
		if job.Version > lastSent {
			response, err := app.jobRepresentation(r.Context(), job)
			if err != nil {
				endReason = "error"
				return
			}
			payload, err := json.Marshal(response) // compact JSON: always ONE data line
			if err != nil {
				endReason = "error"
				return
			}
			if err := writeSSE(w, rc, "event: job-update\ndata: "+string(payload)+"\n\n"); err != nil {
				endReason = "write_error" // client gone or stalled
				return
			}
			lastSent = job.Version
			events++
			app.logger.Info("sse_event",
				"job_id", publicID,
				"version", job.Version,
				"status", job.Status,
				"stage", job.Stage,
				"sent_us", time.Now().UnixMicro(), // server wall clock right after Flush
			)
		}

		// [V3] SSE-07 / JOB-06: after the terminal snapshot the server ends the stream.
		// (The browser also calls EventSource.close(), so it does not reconnect.)
		if job.Terminal() {
			endReason = "terminal"
			return
		}

	wait:
		for {
			select {
			case <-changed:
				break wait // a SIGNAL only: loop around and re-read PostgreSQL
			case <-heartbeatC:
				if err := writeSSE(w, rc, ": keep-alive\n\n"); err != nil {
					endReason = "write_error"
					return
				}
				heartbeats++
			case <-r.Context().Done():
				// [V3] SYNC-07: the browser disconnected (tab closed, Stop watching, network
				// drop) or the server is shutting down. This ends the OBSERVER only;
				// the worker's job is untouched.
				endReason = "cancelled"
				return
			}
		}
	}
}

// writeSSE writes one frame and flushes it (SSE-06). A frame ends with a blank line.
// Flush pushes the bytes to the browser; it does NOT end or close the response.
func writeSSE(w http.ResponseWriter, rc *http.ResponseController, frame string) error {
	_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
	if _, err := io.WriteString(w, frame); err != nil {
		return err
	}
	return rc.Flush()
}
