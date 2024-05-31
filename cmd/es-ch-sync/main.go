package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/es-ch-sync/internal/config"
	"github.com/DIMO-Network/es-ch-sync/internal/service/clickhouse"
	"github.com/DIMO-Network/es-ch-sync/internal/service/deviceapi"
	"github.com/DIMO-Network/es-ch-sync/internal/service/elastic"
	"github.com/DIMO-Network/es-ch-sync/internal/sync"
	"github.com/DIMO-Network/shared"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("app", "es-ch-sync").Logger()
	// create a flag for the settings file
	settingsFile := flag.String("settings", "settings.yaml", "settings file")
	flag.Parse()
	settings, err := shared.LoadConfig[config.Settings](*settingsFile)
	if err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}

	serveMonitoring(strconv.Itoa(settings.MonPort), &logger)
	syncer, err := createSychronizer(logger, settings)
	if err != nil {
		return fmt.Errorf("failed to create synchronizer: %w", err)
	}
	opts, err := settingsToOpttions(settings)
	if err != nil {
		return fmt.Errorf("failed to parse settings: %w", err)
	}
	if err := syncer.Start(context.Background(), opts); err != nil {
		return fmt.Errorf("failed to start synchronizer: %w", err)
	}

	// stop linkerd
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:4191/shutdown", nil)
	if err != nil {
		return fmt.Errorf("failed to create shutdown request: %w", err)
	}
	if _, err := http.DefaultClient.Do(req); err != nil {
		return fmt.Errorf("failed to send shutdown request: %w", err)
	}

	return nil
}

func settingsToOpttions(settings config.Settings) (sync.Options, error) {
	var stopTime, startTime time.Time
	var err error
	if settings.StopTime != "" {
		stopTime, err = time.Parse(time.RFC3339, settings.StopTime)
		if err != nil {
			return sync.Options{}, fmt.Errorf("failed to parse stop time: %w", err)
		}
	}
	if settings.StartTime != "" {
		startTime, err = time.Parse(time.RFC3339, settings.StartTime)
		if err != nil {
			return sync.Options{}, fmt.Errorf("failed to parse start time: %w", err)
		}
	}
	batchSize := settings.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	var tokenIDs []string
	if settings.TokenIDs != "" {
		tokenIDs = strings.Split(settings.TokenIDs, ",")
	}
	var signals []string
	if settings.Signals != "" {
		signals = strings.Split(settings.Signals, ",")
	}
	return sync.Options{
		StartTime: startTime,
		StopTime:  stopTime,
		BatchSize: batchSize,
		TokenIDs:  tokenIDs,
		Signals:   signals,
		Parallel:  settings.Parallel,
	}, nil
}

func serveMonitoring(port string, logger *zerolog.Logger) *fiber.App {
	logger.Info().Str("port", port).Msg("Starting monitoring web server.")

	monApp := fiber.New(fiber.Config{DisableStartupMessage: true})

	monApp.Get("/", func(c *fiber.Ctx) error { return nil })
	monApp.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	go func() {
		if err := monApp.Listen(":" + port); err != nil {
			logger.Fatal().Err(err).Str("port", port).Msg("Failed to start monitoring web server.")
		}
	}()

	return monApp
}

func createSychronizer(logger zerolog.Logger, settings config.Settings) (*sync.Synchronizer, error) {
	chService, err := clickhouse.New(settings)
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse service: %w", err)
	}
	esService, err := elastic.New(settings)
	if err != nil {
		return nil, fmt.Errorf("failed to create elastic service: %w", err)
	}
	devicesConn, err := grpc.Dial(settings.DeviceAPIAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial devices api: %w", err)
	}
	deviceAPI := deviceapi.NewService(devicesConn)
	syncer, err := sync.NewSychronizer(logger, esService, chService, deviceAPI)
	if err != nil {
		return nil, fmt.Errorf("failed to create synchronizer: %w", err)
	}
	return syncer, nil
}
