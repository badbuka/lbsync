// Package discovery scans certificate locations and returns the set of
// certificates that should be monitored.
//
// Adapted from github.com/badbuka/letsencrypt-exporter (MIT licensed). It
// deliberately knows nothing about Prometheus or Olric so it can be reused by
// arbitrary tooling.
package discovery

import (
	"sort"
)

// Cert describes a single certificate discovered on disk.
type Cert struct {
	// CertPath is the absolute, symlink-resolved path to the PEM file.
	CertPath string
	// FallbackID is the certbot lineage directory name, PEM basename, or
	// another filesystem identifier used when the certificate carries no
	// DNS names or common name.
	FallbackID string
}

// DefaultRoot is the conventional Let's Encrypt directory on Linux.
const DefaultRoot = "/etc/letsencrypt"

// Config selects which discovery sources to run.
type Config struct {
	// CertbotRoot is the certbot root directory (<root>/live/*). Empty skips
	// the certbot scan.
	CertbotRoot string
}

// Entry is a single observation produced by ScanAllVerbose. If Error is nil
// the entry was usable; otherwise Cert.FallbackID still names the source but
// Cert.CertPath may be empty.
type Entry struct {
	Cert  Cert
	Error error
}

// ScanAll merges certificates from all enabled discovery sources.
func ScanAll(cfg Config) ([]Cert, error) {
	entries, err := ScanAllVerbose(cfg)
	if err != nil {
		return nil, err
	}
	out := make([]Cert, 0, len(entries))
	for _, e := range entries {
		if e.Error != nil {
			continue
		}
		out = append(out, e.Cert)
	}
	return out, nil
}

// ScanAllVerbose runs every enabled scanner and reports per-entry results.
func ScanAllVerbose(cfg Config) ([]Entry, error) {
	var out []Entry

	if cfg.CertbotRoot != "" {
		entries, err := scanCertbotVerbose(cfg.CertbotRoot)
		if err != nil {
			return nil, err
		}
		out = append(out, entries...)
	}

	return mergeEntries(out), nil
}

func mergeEntries(entries []Entry) []Entry {
	seen := make(map[string]struct{}, len(entries))
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if e.Error != nil || e.Cert.CertPath == "" {
			out = append(out, e)
			continue
		}
		if _, ok := seen[e.Cert.CertPath]; ok {
			continue
		}
		seen[e.Cert.CertPath] = struct{}{}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cert.FallbackID != out[j].Cert.FallbackID {
			return out[i].Cert.FallbackID < out[j].Cert.FallbackID
		}
		return out[i].Cert.CertPath < out[j].Cert.CertPath
	})
	return out
}
