// Command ch-exporter ships collector samples from the local SQLite stores
// into ClickHouse.
//
// The collectors keep writing SQLite exactly as upstream intends; this reads
// what they wrote and forwards it. Keeping the two apart means the venue code
// carries no ClickHouse concerns and stays mergeable with upstream.
//
// Rows are tracked by the SQLite rowid watermark, so an export is resumable
// and re-running it never duplicates. Delivery is at-least-once in the sense
// that a crash between INSERT and watermark persist replays a batch; the
// ClickHouse table is MergeTree rather than Replacing, so operators should
// treat a duplicated batch as possible after an unclean shutdown.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// Columns read from the collector store, in the order ClickHouse receives
// them. Kept as one list so the SELECT and the JSON payload cannot drift.
var columns = []string{
	"venue", "completed_at", "run_id", "iteration", "scenario", "transport",
	"order_type", "measurement_mode", "latency_mode", "batch_size",
	"sent_at", "scheduled_at",
	"network_ns", "raw_network_ns", "adjusted_network_ns", "network_floor_ns",
	"speed_bump_ns", "speed_bump_source",
	"submission_ns", "start_delay_ns", "write_delay_ns",
	"ok", "classification", "classification_reason", "error",
	"cleanup_attempted", "cleanup_ok", "cleanup_duration_ns", "cleanup_error",
}

var timestampColumns = map[string]bool{
	"completed_at": true, "sent_at": true, "scheduled_at": true,
}

type options struct {
	stores      string
	url         string
	database    string
	table       string
	statePath   string
	interval    time.Duration
	batch       int
	metricsAddr string
	dryRun      bool
}

type metrics struct {
	mu          sync.Mutex
	rows        map[string]int64
	errors      map[string]int64
	lastSuccess map[string]int64
	pending     map[string]int64
}

func newMetrics() *metrics {
	return &metrics{
		rows: map[string]int64{}, errors: map[string]int64{},
		lastSuccess: map[string]int64{}, pending: map[string]int64{},
	}
}

func (m *metrics) render() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	emit := func(name, help string, values map[string]int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		keys := make([]string, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%s{store=%q} %d\n", name, k, values[k])
		}
	}
	emit("perps_bench_exporter_rows_total", "Rows shipped to ClickHouse.", m.rows)
	emit("perps_bench_exporter_errors_total", "Export attempts that failed.", m.errors)
	emit("perps_bench_exporter_last_success_seconds", "Unix time of the last successful export.", m.lastSuccess)
	emit("perps_bench_exporter_pending_rows", "Rows written by the collector but not yet exported.", m.pending)
	return b.String()
}

func main() {
	var opts options
	flag.StringVar(&opts.stores, "stores", "data/*.db", "Glob matching collector SQLite stores.")
	flag.StringVar(&opts.url, "clickhouse-url", envOr("CLICKHOUSE_URL", ""), "ClickHouse HTTP endpoint, e.g. https://host:8443.")
	flag.StringVar(&opts.database, "database", envOr("CLICKHOUSE_DATABASE", "market_data"), "Target database.")
	flag.StringVar(&opts.table, "table", "latency_samples", "Target table.")
	flag.StringVar(&opts.statePath, "state", "data/ch-exporter-state.json", "Watermark file.")
	flag.DurationVar(&opts.interval, "interval", 30*time.Second, "Export interval.")
	flag.IntVar(&opts.batch, "batch", 1000, "Maximum rows per export.")
	flag.StringVar(&opts.metricsAddr, "metrics-addr", ":9101", "Prometheus metrics listen address.")
	flag.BoolVar(&opts.dryRun, "dry-run", false, "Read and format rows but do not insert, and do not advance the watermark.")
	flag.Parse()

	if opts.url == "" && !opts.dryRun {
		log.Fatal("clickhouse-url is required (or set CLICKHOUSE_URL)")
	}

	m := newMetrics()
	go serveMetrics(opts.metricsAddr, m)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()
	for {
		if err := exportOnce(ctx, opts, m); err != nil {
			log.Printf("export: %v", err)
		}
		select {
		case <-ctx.Done():
			log.Print("shutting down")
			return
		case <-ticker.C:
		}
	}
}

func serveMetrics(addr string, m *metrics) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(m.render()))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics listener: %v", err)
	}
}

func exportOnce(ctx context.Context, opts options, m *metrics) error {
	paths, err := filepath.Glob(opts.stores)
	if err != nil {
		return fmt.Errorf("glob %q: %w", opts.stores, err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no stores matched %q", opts.stores)
	}
	state, err := loadState(opts.statePath)
	if err != nil {
		return err
	}
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".db")
		shipped, err := exportStore(ctx, opts, path, state, m, name)
		if err != nil {
			m.mu.Lock()
			m.errors[name]++
			m.mu.Unlock()
			log.Printf("store %s: %v", name, err)
			continue
		}
		if shipped > 0 && !opts.dryRun {
			log.Printf("store %s: shipped %d rows (watermark %d)", name, shipped, state[path])
		}
	}
	return saveState(opts.statePath, state)
}

func exportStore(ctx context.Context, opts options, path string, state map[string]int64, m *metrics, name string) (int, error) {
	// Read-only so a half-written collector transaction is never observed, and
	// so the exporter can never block the collector's writes.
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return 0, err
	}
	defer db.Close()

	after := state[path]
	query := fmt.Sprintf(
		"SELECT id, %s FROM samples WHERE id > ? ORDER BY id LIMIT %d",
		strings.Join(columns, ", "), opts.batch,
	)
	rows, err := db.QueryContext(ctx, query, after)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	maxID := after
	count := 0
	for rows.Next() {
		values := make([]any, len(columns)+1)
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return 0, err
		}
		if id, ok := toInt64(values[0]); ok && id > maxID {
			maxID = id
		}
		record := make(map[string]any, len(columns))
		for i, column := range columns {
			record[column] = normalize(column, values[i+1])
		}
		if err := enc.Encode(record); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	pending, err := pendingRows(ctx, db, maxID)
	if err == nil {
		m.mu.Lock()
		m.pending[name] = pending
		m.mu.Unlock()
	}
	if count == 0 {
		return 0, nil
	}
	if opts.dryRun {
		// Leave the watermark alone so a dry run never hides rows from the
		// real export that follows it.
		first, _, _ := strings.Cut(body.String(), "\n")
		log.Printf("store %s: dry-run, would ship %d rows up to id %d; first row: %s",
			name, count, maxID, first)
		return count, nil
	}
	if err := insert(ctx, opts, &body); err != nil {
		return 0, err
	}

	state[path] = maxID
	m.mu.Lock()
	m.rows[name] += int64(count)
	m.lastSuccess[name] = time.Now().Unix()
	m.mu.Unlock()
	return count, nil
}

func pendingRows(ctx context.Context, db *sql.DB, after int64) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, "SELECT count(*) FROM samples WHERE id > ?", after).Scan(&n)
	return n, err
}

func insert(ctx context.Context, opts options, body *bytes.Buffer) error {
	statement := fmt.Sprintf("INSERT INTO %s.%s (%s) FORMAT JSONEachRow",
		opts.database, opts.table, strings.Join(columns, ", "))
	endpoint := strings.TrimRight(opts.url, "/") + "/?query=" + urlEncode(statement)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return err
	}
	if user := os.Getenv("CLICKHOUSE_USER"); user != "" {
		request.SetBasicAuth(user, os.Getenv("CLICKHOUSE_PASSWORD"))
	}
	request.Header.Set("Content-Type", "application/x-ndjson")

	client := &http.Client{Timeout: 60 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		message := make([]byte, 512)
		n, _ := response.Body.Read(message)
		return fmt.Errorf("clickhouse %d: %s", response.StatusCode, strings.TrimSpace(string(message[:n])))
	}
	return nil
}

// normalize converts a SQLite value into what the ClickHouse column expects.
// Timestamps arrive as RFC3339 with nanoseconds and DateTime64(6) wants a
// space separator and microseconds. A failed sample can carry an empty
// sent_at or scheduled_at, which becomes the epoch; those rows always have
// ok = 0, so filter on that rather than reading a zero timestamp as real.
func normalize(column string, value any) any {
	if !timestampColumns[column] {
		if value == nil {
			return ""
		}
		if b, ok := value.([]byte); ok {
			return string(b)
		}
		return value
	}
	text := ""
	switch v := value.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05.000000")
	}
	if strings.TrimSpace(text) == "" {
		return "1970-01-01 00:00:00.000000"
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC().Format("2006-01-02 15:04:05.000000")
		}
	}
	return "1970-01-01 00:00:00.000000"
}

func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	}
	return 0, false
}

func loadState(path string) (map[string]int64, error) {
	state := map[string]int64{}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}
	return state, nil
}

// saveState writes via a temporary file so an interrupted write cannot leave a
// truncated watermark, which would replay every row in the store.
func saveState(path string, state map[string]int64) error {
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func urlEncode(s string) string {
	replacer := strings.NewReplacer(" ", "%20", ",", "%2C", "(", "%28", ")", "%29", "*", "%2A", "\n", "%0A")
	return replacer.Replace(s)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
