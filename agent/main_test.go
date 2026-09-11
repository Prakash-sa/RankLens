package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollectSpoolAndDurableDrain(t *testing.T) {
	root := t.TempDir()
	capture := filepath.Join(root, "capture")
	spool := filepath.Join(root, "spool")
	if err := os.MkdirAll(capture, 0o750); err != nil {
		t.Fatal(err)
	}
	summary := `{"schema_version":2,"complete":true,"capture_state":"finalized"}`
	if err := os.WriteFile(filepath.Join(capture, "rank-00000-summary.json"), []byte(summary), 0o640); err != nil {
		t.Fatal(err)
	}
	events := "{\"record_kind\":\"api_call\",\"rank\":0}\n{\"record_kind\":\"api_call\",\"rank\":0}\n"
	if err := os.WriteFile(filepath.Join(capture, "rank-00000-events.jsonl"), []byte(events), 0o640); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(root, "token")
	token := "machine-token-with-at-least-24-characters"
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		var segment segmentUpload
		if err := json.NewDecoder(request.Body).Decode(&segment); err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		if segment.RecordCount < 1 || segment.PayloadSHA256 == "" {
			http.Error(response, "invalid segment", http.StatusBadRequest)
			return
		}
		index := requests.Add(1)
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusCreated)
		fmt.Fprintf(response, `{"receipt_id":"receipt-%d","status":"DURABLE"}`, index)
	}))
	defer server.Close()

	instance, err := newAgent(config{
		CaptureDir:    capture,
		SpoolDir:      spool,
		APIURL:        server.URL,
		TokenFile:     tokenPath,
		ClusterID:     "cluster-a",
		AttemptID:     "attempt-1",
		ProducerID:    "agent-1",
		MaxSpoolBytes: defaultSpoolMax,
		Interval:      time.Second,
		Once:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.collect(); err != nil {
		t.Fatal(err)
	}
	pending, err := filepath.Glob(filepath.Join(spool, "pending", "*.segment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected two pending segments, found %d", len(pending))
	}
	if instance.state.NextSequence["rank-summary"] != 1 || instance.state.NextSequence["mpi-events"] != 2 {
		t.Fatalf("unexpected per-stream sequences: %#v", instance.state.NextSequence)
	}
	if err := instance.collect(); err != nil {
		t.Fatal(err)
	}
	pendingAgain, _ := filepath.Glob(filepath.Join(spool, "pending", "*.segment.json"))
	if len(pendingAgain) != 2 {
		t.Fatalf("unchanged capture should not create segments, found %d", len(pendingAgain))
	}
	if err := instance.drain(); err != nil {
		t.Fatal(err)
	}
	pendingAfter, _ := filepath.Glob(filepath.Join(spool, "pending", "*.segment.json"))
	receipts, _ := filepath.Glob(filepath.Join(spool, "receipts", "*.json"))
	if len(pendingAfter) != 0 || len(receipts) != 2 || requests.Load() != 2 {
		t.Fatalf("unexpected drain result: pending=%d receipts=%d requests=%d", len(pendingAfter), len(receipts), requests.Load())
	}
}

func TestTransportSafetyChecks(t *testing.T) {
	if err := validateEndpoint("http://example.com"); err == nil {
		t.Fatal("expected plaintext non-loopback endpoint to be rejected")
	}
	if err := validateEndpoint("https://example.com"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 32)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readToken(path); err == nil {
		t.Fatal("expected permissive token file to be rejected")
	}
	secure := filepath.Join(t.TempDir(), "secure-token")
	if err := os.WriteFile(secure, []byte(strings.Repeat("x", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	link := secure + "-link"
	if err := os.Symlink(secure, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readToken(link); err == nil {
		t.Fatal("expected token symlink to be rejected")
	}
}

func TestSpoolAndSummaryBudgets(t *testing.T) {
	root := t.TempDir()
	instance, err := newAgent(config{
		CaptureDir:    root,
		SpoolDir:      filepath.Join(root, "spool"),
		ClusterID:     "cluster-a",
		AttemptID:     "attempt-1",
		ProducerID:    "agent-1",
		MaxSpoolBytes: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.spoolPayload("events", []byte("{\"value\":1}\n"), 1); err == nil || !strings.Contains(err.Error(), "spool quota") {
		t.Fatalf("expected spool quota rejection, got %v", err)
	}
	large := filepath.Join(root, "large-summary.json")
	if err := os.WriteFile(large, []byte(strings.Repeat("x", maxSummaryBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBoundedFile(large, maxSummaryBytes); err == nil {
		t.Fatal("expected oversized summary to be rejected")
	}
}
