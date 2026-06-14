// gateway: public edge of the arena. Accepts submission bundles, enqueues
// benchmark runs, serves the leaderboard REST + WebSocket feed and the web UI.
// Stateless — all state lives in Redis and the shared submissions volume —
// so it scales horizontally behind any L4 balancer.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"arena/internal/protocol"
)

const (
	streamRuns   = "runs.jobs"
	chanEvents   = "events.leaderboard"
	keySnapshot  = "leaderboard:snapshot"
	maxBundle    = 256 << 20
	maxBots      = 5000
	maxDuration  = 300
	maxRPS       = 100_000
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func rid() string {
	b := make([]byte, 6)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type server struct {
	rdb     *redis.Client
	dataDir string
	hub     *hub
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	s := &server{
		rdb:     rdb,
		dataDir: env("DATA_DIR", "/data/submissions"),
		hub:     newHub(),
	}
	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		log.Fatalf("data dir: %v", err)
	}
	go s.hub.run()
	go s.pumpEvents(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/submissions", s.createSubmission)
	mux.HandleFunc("POST /api/runs", s.createRun)
	mux.HandleFunc("GET /api/runs", s.listRuns)
	mux.HandleFunc("GET /api/runs/{id}", s.getRun)
	mux.HandleFunc("GET /api/leaderboard", s.leaderboard)
	mux.HandleFunc("GET /ws", s.ws)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.Handle("GET /", http.FileServer(http.Dir(env("WEB_DIR", "/web"))))

	addr := ":" + env("PORT", "8090")
	log.Printf("gateway listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func httpErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// createSubmission stores the uploaded build context (.tar.gz with a
// Dockerfile at its root) on the shared volume and registers it in Redis.
func (s *server) createSubmission(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBundle)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		httpErr(w, 400, "bad multipart form: "+err.Error())
		return
	}
	team := strings.TrimSpace(r.FormValue("team"))
	name := strings.TrimSpace(r.FormValue("name"))
	if team == "" || name == "" {
		httpErr(w, 400, "fields 'team' and 'name' are required")
		return
	}
	f, hdr, err := r.FormFile("bundle")
	if err != nil {
		httpErr(w, 400, "field 'bundle' (tar.gz build context) is required")
		return
	}
	defer f.Close()
	if !strings.HasSuffix(hdr.Filename, ".tar.gz") && !strings.HasSuffix(hdr.Filename, ".tgz") {
		httpErr(w, 400, "bundle must be a .tar.gz docker build context")
		return
	}

	id := rid()
	dst, err := os.Create(filepath.Join(s.dataDir, id+".tar.gz"))
	if err != nil {
		httpErr(w, 500, "store bundle: "+err.Error())
		return
	}
	defer dst.Close()
	n, err := io.Copy(dst, f)
	if err != nil {
		httpErr(w, 500, "store bundle: "+err.Error())
		return
	}

	ctx := r.Context()
	s.rdb.HSet(ctx, "submission:"+id, map[string]any{
		"id": id, "team": team, "name": name,
		"bytes": n, "created": time.Now().Unix(),
	})
	s.rdb.LPush(ctx, "submissions.index", id)
	s.rdb.LTrim(ctx, "submissions.index", 0, 199)
	log.Printf("submission %s team=%q name=%q (%d bytes)", id, team, name, n)
	writeJSON(w, 201, map[string]string{"id": id})
}

type runReq struct {
	SubmissionID string `json:"submission_id"`
	Bots         int    `json:"bots"`
	DurationS    int    `json:"duration_s"`
	MaxRPS       int    `json:"max_rps"`
}

func (s *server) createRun(w http.ResponseWriter, r *http.Request) {
	var req runReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpErr(w, 400, "bad json: "+err.Error())
		return
	}
	ctx := r.Context()
	if n, _ := s.rdb.Exists(ctx, "submission:"+req.SubmissionID).Result(); n == 0 {
		httpErr(w, 404, "unknown submission_id")
		return
	}
	if req.Bots <= 0 {
		req.Bots = 200
	}
	if req.DurationS <= 0 {
		req.DurationS = 45
	}
	if req.MaxRPS <= 0 {
		req.MaxRPS = 4000
	}
	req.Bots = min(req.Bots, maxBots)
	req.DurationS = min(req.DurationS, maxDuration)
	req.MaxRPS = min(req.MaxRPS, maxRPS)

	job := protocol.RunJob{
		RunID:        rid(),
		SubmissionID: req.SubmissionID,
		Bots:         req.Bots,
		DurationS:    req.DurationS,
		MaxRPS:       req.MaxRPS,
	}
	payload, _ := json.Marshal(job)
	s.rdb.HSet(ctx, "run:"+job.RunID, map[string]any{
		"id": job.RunID, "submission_id": job.SubmissionID,
		"status": "queued", "created": time.Now().Unix(),
		"bots": job.Bots, "duration_s": job.DurationS, "max_rps": job.MaxRPS,
	})
	s.rdb.LPush(ctx, "runs.index", job.RunID)
	s.rdb.LTrim(ctx, "runs.index", 0, 99)
	if err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamRuns,
		Values: map[string]any{"payload": payload},
	}).Err(); err != nil {
		httpErr(w, 500, "enqueue: "+err.Error())
		return
	}
	log.Printf("run %s queued for submission %s (%d bots, %ds, %d rps)",
		job.RunID, job.SubmissionID, job.Bots, job.DurationS, job.MaxRPS)
	writeJSON(w, 201, map[string]string{"id": job.RunID})
}

func (s *server) runView(ctx context.Context, id string) map[string]string {
	run, _ := s.rdb.HGetAll(ctx, "run:"+id).Result()
	if len(run) == 0 {
		return nil
	}
	if res, _ := s.rdb.HGetAll(ctx, "result:"+id).Result(); len(res) > 0 {
		for k, v := range res {
			run["m_"+k] = v
		}
	}
	if sub, _ := s.rdb.HGetAll(ctx, "submission:"+run["submission_id"]).Result(); len(sub) > 0 {
		run["team"] = sub["team"]
		run["name"] = sub["name"]
	}
	return run
}

func (s *server) listRuns(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ids, _ := s.rdb.LRange(ctx, "runs.index", 0, 49).Result()
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		if v := s.runView(ctx, id); v != nil {
			out = append(out, v)
		}
	}
	writeJSON(w, 200, out)
}

func (s *server) getRun(w http.ResponseWriter, r *http.Request) {
	v := s.runView(r.Context(), r.PathValue("id"))
	if v == nil {
		httpErr(w, 404, "unknown run")
		return
	}
	writeJSON(w, 200, v)
}

func (s *server) leaderboard(w http.ResponseWriter, r *http.Request) {
	snap, err := s.rdb.Get(r.Context(), keySnapshot).Result()
	if err != nil {
		snap = "[]"
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, snap)
}

// --- WebSocket fan-out -------------------------------------------------

type hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]struct{}
	bcast   chan []byte
}

func newHub() *hub {
	return &hub{clients: map[*websocket.Conn]struct{}{}, bcast: make(chan []byte, 64)}
}

func (h *hub) run() {
	for msg := range h.bcast {
		h.mu.Lock()
		for c := range h.clients {
			c.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.WriteMessage(websocket.TextMessage, msg); err != nil {
				c.Close()
				delete(h.clients, c)
			}
		}
		h.mu.Unlock()
	}
}

func (h *hub) add(c *websocket.Conn)    { h.mu.Lock(); h.clients[c] = struct{}{}; h.mu.Unlock() }
func (h *hub) remove(c *websocket.Conn) { h.mu.Lock(); delete(h.clients, c); h.mu.Unlock() }

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (s *server) ws(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	if snap, err := s.rdb.Get(r.Context(), keySnapshot).Result(); err == nil {
		c.WriteMessage(websocket.TextMessage, []byte(snap))
	}
	s.hub.add(c)
	go func() { // reader: detect close, discard input
		defer func() { s.hub.remove(c); c.Close() }()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

// pumpEvents relays ingester leaderboard publishes to all WS clients.
func (s *server) pumpEvents(ctx context.Context) {
	for {
		sub := s.rdb.Subscribe(ctx, chanEvents)
		for msg := range sub.Channel() {
			s.hub.bcast <- []byte(msg.Payload)
		}
		sub.Close()
		time.Sleep(time.Second) // redis hiccup: resubscribe
	}
}
