package main

import (
	"testing"
	"time"

	"dash0.com/otlp-metric-store-backend/internal/telemetry"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestEnvName(t *testing.T) {
	for flagName, want := range map[string]string{
		"listenAddr":         "LISTEN_ADDR",
		"clickhouseAddr":     "CLICKHOUSE_ADDR",
		"batchMaxDataPoints": "BATCH_MAX_DATA_POINTS",
		"migrate":            "MIGRATE",
	} {
		if got := envName(flagName); got != want {
			t.Errorf("envName(%q) = %q, want %q", flagName, got, want)
		}
	}
}

func TestLoadConfigPrecedence(t *testing.T) {
	cfg, err := loadConfig(
		[]string{"-clickhouseAddr=flag:9000"},
		env(map[string]string{
			"CLICKHOUSE_ADDR":             "env:9000",
			"BATCH_MAX_DELAY":             "1s",
			"CLICKHOUSE_PASSWORD":         "secret",
			"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClickHouse.Addr != "flag:9000" {
		t.Errorf("flag must win over environment, got %q", cfg.ClickHouse.Addr)
	}
	if cfg.Writer.MaxBatchDelay != time.Second {
		t.Errorf("environment must override defaults, got %v", cfg.Writer.MaxBatchDelay)
	}
	if cfg.ClickHouse.Password != "secret" {
		t.Error("password not read from CLICKHOUSE_PASSWORD")
	}
	if cfg.TimeLimits.RetentionDays != cfg.RetentionDays || cfg.TimeLimits.MaxAge != 7*24*time.Hour {
		t.Errorf("time limits must default to 7 days and follow the retention, got %+v", cfg.TimeLimits)
	}
	if cfg.Telemetry.Exporter != telemetry.ExporterOTLP {
		t.Errorf("an OTLP endpoint must select the OTLP exporter, got %q", cfg.Telemetry.Exporter)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		env  map[string]string
	}{
		"bad env duration": {nil, map[string]string{"BATCH_MAX_DELAY": "soon"}},
		"bad log level":    {[]string{"-logLevel=loud"}, nil},
		"negative TTL":     {[]string{"-retentionDays=-1"}, nil},
		"zero max age":     {[]string{"-maxDataPointAge=0"}, nil},
		"window too long":  {[]string{"-maxDataPointAge=2160h", "-maxClockSkew=1h"}, nil},
	} {
		if _, err := loadConfig(tc.args, env(tc.env)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
