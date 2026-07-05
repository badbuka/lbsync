// Command lbsync runs the load-balancer coordination agent: it joins an
// embedded Olric cluster with the other balancers and runs newest-wins modules
// (certs, blob, ...) that replicate resources across the fleet and apply the
// newest copy locally behind a verify-then-reload hook.
//
// Configuration is read from environment variables (via envconfig) and may be
// overridden by command-line flags.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/badbuka/lbsync/internal/cluster"
	"github.com/badbuka/lbsync/internal/engine"
	"github.com/badbuka/lbsync/internal/metrics"
	"github.com/badbuka/lbsync/internal/module"

	// Side-effect imports register the available modules.
	_ "github.com/badbuka/lbsync/internal/modules/blob"
	_ "github.com/badbuka/lbsync/internal/modules/certs"
	_ "github.com/badbuka/lbsync/internal/modules/stubs"
)

type config struct {
	LetsencryptPath string `envconfig:"LETSENCRYPT_PATH"   default:"/etc/letsencrypt"`
	ServingDir      string `envconfig:"SERVING_DIR"        default:"/etc/lbsync/live"`
	BlobResources   string `envconfig:"BLOB_RESOURCES"     default:""`
	Modules         string `envconfig:"MODULES"            default:"certs"`

	Peers          string `envconfig:"CLUSTER_PEERS"      default:""`
	ClusterEnv     string `envconfig:"CLUSTER_ENV"        default:"lan"`
	BindAddr       string `envconfig:"BIND_ADDR"          default:"0.0.0.0"`
	AdvertiseAddr  string `envconfig:"ADVERTISE_ADDR"     default:""`
	OlricPort      int    `envconfig:"OLRIC_PORT"         default:"3320"`
	MemberlistPort int    `envconfig:"MEMBERLIST_PORT"    default:"3322"`
	ReplicaCount   int    `envconfig:"REPLICA_COUNT"      default:"0"`

	Password  string `envconfig:"CLUSTER_PASSWORD"   default:""`
	GossipKey string `envconfig:"CLUSTER_GOSSIP_KEY" default:""`
	Insecure  bool   `envconfig:"INSECURE"           default:"false"`

	ReconcileInterval time.Duration `envconfig:"RECONCILE_INTERVAL" default:"30s"`
	VerifyCmd         string        `envconfig:"VERIFY_CMD"         default:""`
	ReloadCmd         string        `envconfig:"RELOAD_CMD"         default:""`
	ReloadTimeout     time.Duration `envconfig:"RELOAD_TIMEOUT"     default:"30s"`

	Port     int    `envconfig:"PORT"     default:"8623"`
	Hostname string `envconfig:"HOSTNAME" default:""`
}

func loadConfig(args []string) (config, error) {
	var cfg config
	if err := envconfig.Process("", &cfg); err != nil {
		return cfg, fmt.Errorf("envconfig: %w", err)
	}

	fs := flag.NewFlagSet("lbsync", flag.ContinueOnError)
	fs.StringVar(&cfg.LetsencryptPath, "letsencrypt-path", cfg.LetsencryptPath, "certbot root (env: LETSENCRYPT_PATH)")
	fs.StringVar(&cfg.ServingDir, "serving-dir", cfg.ServingDir, "directory the LB reads certs from (env: SERVING_DIR)")
	fs.StringVar(&cfg.BlobResources, "blob-resources", cfg.BlobResources, "blob module resources: name=src:dest[:strategy],... (env: BLOB_RESOURCES)")
	fs.StringVar(&cfg.Modules, "modules", cfg.Modules, "comma-separated enabled modules (env: MODULES)")
	fs.StringVar(&cfg.Peers, "peers", cfg.Peers, "comma-separated host:memberlistPort peers (env: CLUSTER_PEERS)")
	fs.StringVar(&cfg.ClusterEnv, "cluster-env", cfg.ClusterEnv, "memberlist env: local|lan|wan (env: CLUSTER_ENV)")
	fs.StringVar(&cfg.BindAddr, "bind-addr", cfg.BindAddr, "bind address (env: BIND_ADDR)")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", cfg.AdvertiseAddr, "advertised address for peers (env: ADVERTISE_ADDR)")
	fs.IntVar(&cfg.OlricPort, "olric-port", cfg.OlricPort, "Olric TCP port (env: OLRIC_PORT)")
	fs.IntVar(&cfg.MemberlistPort, "memberlist-port", cfg.MemberlistPort, "memberlist gossip port (env: MEMBERLIST_PORT)")
	fs.IntVar(&cfg.ReplicaCount, "replica-count", cfg.ReplicaCount, "replica count; 0 = len(peers)+1 (env: REPLICA_COUNT)")
	fs.StringVar(&cfg.Password, "cluster-password", cfg.Password, "Olric auth password (env: CLUSTER_PASSWORD)")
	fs.StringVar(&cfg.GossipKey, "cluster-gossip-key", cfg.GossipKey, "base64 16/24/32-byte memberlist encryption key (env: CLUSTER_GOSSIP_KEY)")
	fs.BoolVar(&cfg.Insecure, "insecure", cfg.Insecure, "allow running without auth/encryption (dev only) (env: INSECURE)")
	fs.DurationVar(&cfg.ReconcileInterval, "interval", cfg.ReconcileInterval, "reconcile cadence (env: RECONCILE_INTERVAL)")
	fs.StringVar(&cfg.VerifyCmd, "verify-cmd", cfg.VerifyCmd, "config verify command run before reload (env: VERIFY_CMD)")
	fs.StringVar(&cfg.ReloadCmd, "reload-cmd", cfg.ReloadCmd, "reload command run after verify passes (env: RELOAD_CMD)")
	fs.DurationVar(&cfg.ReloadTimeout, "reload-timeout", cfg.ReloadTimeout, "verify/reload exec timeout (env: RELOAD_TIMEOUT)")
	fs.IntVar(&cfg.Port, "port", cfg.Port, "metrics/health HTTP port (env: PORT)")
	fs.StringVar(&cfg.Hostname, "hostname", cfg.Hostname, "node id used as record source (env: HOSTNAME)")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func parseList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseBlobResources parses "name=src:dest[:strategy],name2=src2:dest2".
func parseBlobResources(s string) ([]module.BlobResource, error) {
	var out []module.BlobResource
	for _, item := range parseList(s) {
		name, spec, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("blob resource %q: expected name=src:dest[:strategy]", item)
		}
		fields := strings.Split(spec, ":")
		if len(fields) < 2 {
			return nil, fmt.Errorf("blob resource %q: expected src:dest[:strategy]", item)
		}
		r := module.BlobResource{Name: strings.TrimSpace(name), Source: fields[0], Dest: fields[1], Strategy: "mtime"}
		if len(fields) >= 3 && fields[2] != "" {
			r.Strategy = fields[2]
		}
		out = append(out, r)
	}
	return out, nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("lbsync: %v", err)
	}
}

func run(args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if cfg.Hostname == "" {
		cfg.Hostname = defaultHostname()
	}

	peers := parseList(cfg.Peers)
	clusterCfg, err := buildClusterConfig(cfg, peers)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("lbsync starting host=%s peers=%d replicas=%d", cfg.Hostname, len(peers), clusterCfg.ReplicaCount)
	cl, err := cluster.NewCluster(ctx, clusterCfg)
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if cerr := cl.Close(shutdownCtx); cerr != nil {
			log.Printf("cluster shutdown: %v", cerr)
		}
	}()

	eng := engine.New(cl, m, engine.Options{Interval: cfg.ReconcileInterval})
	if err := startModules(ctx, cfg, eng, cl, m); err != nil {
		return err
	}

	var ready atomic.Bool
	go func() {
		ready.Store(true)
		if rerr := eng.Run(ctx); rerr != nil && ctx.Err() == nil {
			log.Printf("engine stopped: %v", rerr)
		}
	}()

	srv := newHTTPServer(cfg.Port, reg, &ready)
	go func() {
		log.Printf("lbsync HTTP listening on :%d", cfg.Port) //nolint:gosec // port is operator config, not request data
		if serr := srv.ListenAndServe(); serr != nil && serr != http.ErrServerClosed {
			log.Printf("http server: %v", serr)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	return nil
}

func defaultHostname() string {
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "unknown"
}

func buildClusterConfig(cfg config, peers []string) (cluster.Config, error) {
	if !cfg.Insecure && cfg.Password == "" {
		return cluster.Config{}, fmt.Errorf("refusing to start without CLUSTER_PASSWORD: private keys and blobs " +
			"traverse the cluster network. Set CLUSTER_PASSWORD (and ideally CLUSTER_GOSSIP_KEY), or pass -insecure " +
			"for single-host development")
	}

	var gossipKey []byte
	if cfg.GossipKey != "" {
		decoded, err := base64.StdEncoding.DecodeString(cfg.GossipKey)
		if err != nil {
			return cluster.Config{}, fmt.Errorf("CLUSTER_GOSSIP_KEY: %w", err)
		}
		switch len(decoded) {
		case 16, 24, 32:
		default:
			return cluster.Config{}, fmt.Errorf("CLUSTER_GOSSIP_KEY must decode to 16, 24, or 32 bytes, got %d", len(decoded))
		}
		gossipKey = decoded
	}

	replicaCount := cfg.ReplicaCount
	if replicaCount <= 0 {
		replicaCount = len(peers) + 1
	}

	return cluster.Config{
		Env:            cfg.ClusterEnv,
		BindAddr:       cfg.BindAddr,
		BindPort:       cfg.OlricPort,
		MemberlistPort: cfg.MemberlistPort,
		AdvertiseAddr:  cfg.AdvertiseAddr,
		Peers:          peers,
		Password:       cfg.Password,
		GossipKey:      gossipKey,
		ReplicaCount:   replicaCount,
	}, nil
}

func startModules(ctx context.Context, cfg config, eng *engine.Engine, cl engine.ClusterKV, m *metrics.Metrics) error {
	blobResources, err := parseBlobResources(cfg.BlobResources)
	if err != nil {
		return fmt.Errorf("blob resources: %w", err)
	}
	moduleCfg := module.Config{
		LetsencryptPath: cfg.LetsencryptPath,
		ServingDir:      cfg.ServingDir,
		VerifyCmd:       strings.Fields(cfg.VerifyCmd),
		ReloadCmd:       strings.Fields(cfg.ReloadCmd),
		ReloadTimeout:   cfg.ReloadTimeout,
		BlobResources:   blobResources,
		Hostname:        cfg.Hostname,
	}
	mods, err := module.Build(parseList(cfg.Modules), moduleCfg)
	if err != nil {
		return fmt.Errorf("modules: %w", err)
	}
	deps := module.Deps{Cluster: cl, Engine: eng, Metrics: m, Logf: log.Printf}
	for _, mod := range mods {
		if err := mod.Start(ctx, deps); err != nil {
			return fmt.Errorf("start module %q: %w", mod.Name(), err)
		}
		m.ModuleEnabled.WithLabelValues(mod.Name()).Set(1)
		log.Printf("module %q started", mod.Name())
	}
	return nil
}

func newHTTPServer(port int, reg *prometheus.Registry, ready *atomic.Bool) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("lbsync\nGET /metrics\nGET /healthz\nGET /readyz\n"))
	})
	return &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
}
