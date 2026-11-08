package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// --------------- SLO instrumentation ----------------------------------------

var (
	sentOK    atomic.Uint64
	sentErr   atomic.Uint64
	sentTotal atomic.Uint64
)

// --------------- cycle-time instrumentation ----------------------------------

// Tracks the most recent cycle duration (in nanoseconds) per goroutine.
// The stats reporter reads all values once per minute to compute avg/max.
type cycleTracker struct {
	mu       sync.Mutex
	durations map[string]time.Duration // homeID → last cycle duration
}

var cycles = &cycleTracker{durations: make(map[string]time.Duration)}

func (ct *cycleTracker) record(homeID string, d time.Duration) {
	ct.mu.Lock()
	ct.durations[homeID] = d
	ct.mu.Unlock()
}

func (ct *cycleTracker) snapshot() (avg, max time.Duration, count int) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if len(ct.durations) == 0 {
		return 0, 0, 0
	}
	var total time.Duration
	for _, d := range ct.durations {
		total += d
		if d > max {
			max = d
		}
	}
	count = len(ct.durations)
	avg = total / time.Duration(count)
	return
}

// --------------- env helpers ------------------------------------------------

func env(name string, def *string, fallbacks ...string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	for _, fb := range fallbacks {
		if v, ok := os.LookupEnv(fb); ok {
			return v
		}
	}
	if def == nil {
		return ""
	}
	return *def
}

func envInt(name string, def int, fallbacks ...string) int {
	v := env(name, strPtr(strconv.Itoa(def)), fallbacks...)
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func envFloat(name string, def float64, fallbacks ...string) float64 {
	v := env(name, strPtr(fmt.Sprintf("%g", def)), fallbacks...)
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func strPtr(s string) *string { return &s }

// --------------- data types -------------------------------------------------

type HomeVal struct {
	HomeID      string  `json:"home_id"`
	PlugID      *string `json:"plug_id"`
	Voltage     float64 `json:"voltage"`
	Current     float64 `json:"current"`
	PowerFactor float64 `json:"power_factor"`
	PowerW      float64 `json:"power_w"`
	EnergyWh    float64 `json:"energy_wh"`
	Timestamp   string  `json:"timestamp"`
}

type PlugVal struct {
	HomeID      string  `json:"home_id"`
	PlugID      string  `json:"plug_id"`
	Voltage     float64 `json:"voltage"`
	Current     float64 `json:"current"`
	PowerFactor float64 `json:"power_factor"`
	PowerW      float64 `json:"power_w"`
	EnergyWh    float64 `json:"energy_wh"`
	Timestamp   string  `json:"timestamp"`
}

func roundF(val float64, places int) float64 {
	p := math.Pow10(places)
	return math.Round(val*p) / p
}

// --------------- per-home simulator (runs in its own goroutine) -------------

// homeSimulator simulates one independent home site.
// It owns all state for that home (energy accumulators) — no shared mutation.
func homeSimulator(
	ctx context.Context,
	homeID string,
	plugIDs []string,
	sleepSec float64,
	producerStream string,
	writerHome *kafka.Writer,
	writerPlug *kafka.Writer,
) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(hashStr(homeID))))

	homeEnergy := 0.0
	plugEnergy := make(map[string]float64, len(plugIDs))

	// Pre-allocate reusable buffers (avoids allocs in the hot loop)
	// Old approach: allocated raw per tick and converted keys on every sendOne call
	// raw := make([]float64, len(plugIDs))  // was inside the for/ticker loop
	// sendOne(ctx, writerPlug, homeID+":"+plugID, pv)  // string key, converted to []byte each call
	// sendOne(ctx, writerHome, homeID, hv)
	raw := make([]float64, len(plugIDs))
	plugKeyBytes := make([][]byte, len(plugIDs))
	for i, pid := range plugIDs {
		plugKeyBytes[i] = []byte(homeID + ":" + pid)
	}
	homeKeyBytes := []byte(homeID)

	ticker := time.NewTicker(time.Duration(sleepSec * float64(time.Second)))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		cycleStart := time.Now()

		// --- generate consistent readings ---
		voltage := roundF(rng.NormFloat64()*2+230, 1)
		pf := roundF(0.8+rng.Float64()*0.2, 2)
		totalCurrent := roundF(0.2+rng.Float64()*19.8, 2)
		totalW := voltage * totalCurrent * pf

		ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

		// random weights for splitting power across plugs (reuses pre-allocated slice)
		rawSum := 0.0
		for i := range raw {
			raw[i] = rng.Float64()
			rawSum += raw[i]
		}
		if rawSum == 0 {
			rawSum = 1.0
		}

		// --- home record ---
		homeEnergy += totalW * sleepSec / 3600.0

		if producerStream == "both" || producerStream == "general" {
			hv := HomeVal{
				HomeID:      homeID,
				PlugID:      nil,
				Voltage:     voltage,
				Current:     roundF(totalCurrent, 2),
				PowerFactor: pf,
				PowerW:      roundF(totalW, 2),
				EnergyWh:    roundF(homeEnergy, 3),
				Timestamp:   ts,
			}
			sentTotal.Add(1)
			if err := sendOne(ctx, writerHome, homeKeyBytes, hv); err != nil {
				sentErr.Add(1)
			} else {
				sentOK.Add(1)
			}
		}

		// --- plug records (powers sum exactly to home power) ---
		if producerStream == "both" || producerStream == "plug" {
			for i, plugID := range plugIDs {
				w := raw[i] / rawSum
				plugW := totalW * w
				denom := voltage * pf
				plugCurrent := 0.0
				if denom != 0 {
					plugCurrent = plugW / denom
				}
				plugEnergy[plugID] += plugW * sleepSec / 3600.0

				pv := PlugVal{
					HomeID:      homeID,
					PlugID:      plugID,
					Voltage:     voltage,
					Current:     roundF(plugCurrent, 2),
					PowerFactor: pf,
					PowerW:      roundF(plugW, 2),
					EnergyWh:    roundF(plugEnergy[plugID], 3),
					Timestamp:   ts,
				}
				sentTotal.Add(1)
				if err := sendOne(ctx, writerPlug, plugKeyBytes[i], pv); err != nil {
					sentErr.Add(1)
				} else {
					sentOK.Add(1)
				}
			}
		}

		// Record cycle time for this home
		cycles.record(homeID, time.Since(cycleStart))
	}
}

// simple string hash for seeding per-home RNGs
func hashStr(s string) int {
	h := 0
	for _, c := range s {
		h = h*31 + int(c)
	}
	return h
}

// sendOne sends a single message (no batching). Returns error for instrumentation.
// Old signature: func sendOne(ctx context.Context, w *kafka.Writer, key string, v any) error {
//   Key: []byte(key),  // converted string→[]byte on every call
func sendOne(ctx context.Context, w *kafka.Writer, key []byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: b,
	})
}

// --------------- main -------------------------------------------------------

func main() {
	kfkBootstrap := env("KFK_BOOTSTRAP_SERVERS", strPtr("kafka:9092"), "BOOTSTRAP")
	kfkTopicHome := env("KFK_TOPIC_HOME", strPtr("iot-sensor-general"), "TOPIC_GENERAL", "TOPIC")
	kfkTopicPlug := env("KFK_TOPIC_PLUG", strPtr("iot-sensor-plug"), "TOPIC_PLUG")
	producerStream := strings.ToLower(env("PRODUCER_STREAM", strPtr("both"), "PRODUCER_MODE"))
	simSleepSec := envFloat("SIM_SLEEP_SEC", 1.0, "SLEEP_SEC")

	simHomeIDs := env("SIM_HOME_IDS", strPtr(""), "HOME_IDS")
	simHomeCount := envInt("SIM_HOME_COUNT", 42, "HOME_COUNT")
	simPlugsPerHome := envInt("SIM_PLUGS_PER_HOME", 8, "PLUGS_PER_HOME")

	simHomeIDStart := envInt("SIM_HOME_ID_START", 0)
	simHomeIDEnd := envInt("SIM_HOME_ID_END", 0)

	if seedStr := env("SIM_RANDOM_SEED", nil, "RANDOM_SEED"); seedStr != "" {
		if seed, err := strconv.ParseInt(seedStr, 10, 64); err == nil {
			rand.Seed(seed)
		}
	}

	// --- build home IDs ---
	var baseHomeIDs []string
	if strings.TrimSpace(simHomeIDs) != "" {
		for _, p := range strings.Split(simHomeIDs, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				baseHomeIDs = append(baseHomeIDs, p)
			}
		}
	} else if simHomeIDStart > 0 && simHomeIDEnd > 0 && simHomeIDEnd >= simHomeIDStart {
		for i := simHomeIDStart; i <= simHomeIDEnd; i++ {
			baseHomeIDs = append(baseHomeIDs, strconv.Itoa(i))
		}
	} else {
		for i := 1; i <= simHomeCount; i++ {
			baseHomeIDs = append(baseHomeIDs, strconv.Itoa(i))
		}
	}

	homes := make([]string, len(baseHomeIDs))
	for i, hid := range baseHomeIDs {
		homes[i] = fmt.Sprintf("%s/%s", kfkTopicHome, hid)
	}

	plugsByHome := make(map[string][]string, len(homes))
	for _, h := range homes {
		plugs := make([]string, simPlugsPerHome)
		for i := 1; i <= simPlugsPerHome; i++ {
			plugs[i-1] = fmt.Sprintf("%s/%d", kfkTopicPlug, i)
		}
		plugsByHome[h] = plugs
	}

	// --- Kafka writers (leader-only ack, no client-side batching) ---
	bootstrapServers := strings.Split(kfkBootstrap, ",")
	for i := range bootstrapServers {
		bootstrapServers[i] = strings.TrimSpace(bootstrapServers[i])
	}

	writerHome := &kafka.Writer{
		Addr:                   kafka.TCP(bootstrapServers...),
		Topic:                  kfkTopicHome,
		Balancer:               &kafka.Hash{},
		// RequiredAcks:        kafka.RequireOne,  // original: wait for leader ACK
		RequiredAcks:           kafka.RequireNone, // fire-and-forget — avoids ACK round-trip
		BatchSize:              1,                 // send each message immediately (real-world 1:1)
		AllowAutoTopicCreation: true,
	}
	writerPlug := &kafka.Writer{
		Addr:                   kafka.TCP(bootstrapServers...),
		Topic:                  kfkTopicPlug,
		Balancer:               &kafka.Hash{},
		// RequiredAcks:        kafka.RequireOne,  // original: wait for leader ACK
		RequiredAcks:           kafka.RequireNone, // fire-and-forget
		BatchSize:              1,
		AllowAutoTopicCreation: true,
	}

	// --- launch one goroutine per home (simulates independent sites) ---
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for _, h := range homes {
		wg.Add(1)
		go func(homeID string) {
			defer wg.Done()
			homeSimulator(ctx, homeID, plugsByHome[homeID], simSleepSec, producerStream, writerHome, writerPlug)
		}(h)
	}

	fmt.Printf("Started %d home simulators (%d plugs each), stream=%s, interval=%.1fs\n",
		len(homes), simPlugsPerHome, producerStream, simSleepSec)

	// --- SLO + cycle-time stats reporter: log once per minute ---
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		last := time.Now()
		var lastTotal uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				total := sentTotal.Load()
				dt := time.Since(last).Seconds()
				rps := float64(total-lastTotal) / dt

				avg, max, count := cycles.snapshot()
				fmt.Printf("[stats] ok=%d err=%d total=%d rps=%.0f\n",
					sentOK.Load(), sentErr.Load(), total, rps)
				fmt.Printf("[cycle] homes=%d avg=%s max=%s interval=%.1fs saturated=%v\n",
					count, avg.Round(time.Millisecond), max.Round(time.Millisecond),
					simSleepSec, max.Seconds() > simSleepSec*0.8)

				lastTotal = total
				last = time.Now()
			}
		}
	}()

	// --- wait for SIGTERM/SIGINT then shut down gracefully ---
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	fmt.Println("Shutting down...")
	cancel()
	wg.Wait()

	// Final stats
	fmt.Printf("[final] ok=%d err=%d total=%d\n",
		sentOK.Load(), sentErr.Load(), sentTotal.Load())

	_ = writerHome.Close()
	_ = writerPlug.Close()
	fmt.Println("Done.")
}