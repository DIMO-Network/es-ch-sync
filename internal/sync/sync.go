// Package sync is the entrypoint for starting an elastic clickhouse sync
package sync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/es-ch-sync/internal/service/clickhouse"
	"github.com/DIMO-Network/es-ch-sync/internal/service/elastic"
	"github.com/DIMO-Network/model-garage/pkg/schema"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

var errNoRows = errors.New("no more rows")

type idResolver interface {
	convert.TokenIDGetter
	SubjectFromTokenID(ctx context.Context, tokenID uint32) (string, error)
	PrimeTokenIDCache(ctx context.Context, tokendID uint32, subject string)
}

// Synchronizer is the main struct for syncing data between ElasticSearch and ClickHouse
type Synchronizer struct {
	esService *elastic.Service
	chService *clickhouse.Service
	idGetter  idResolver
	log       zerolog.Logger
}

func NewSychronizer(logger zerolog.Logger, esService *elastic.Service, chService *clickhouse.Service, idGetter idResolver) (*Synchronizer, error) {
	syncer := Synchronizer{
		esService: esService,
		chService: chService,
		idGetter:  idGetter,
		log:       logger,
	}
	return &syncer, nil
}

// Start begins the synchronization process from ElasticSearch to ClickHouse.
// It takes a context and stop time as parameters and returns an error if any.
func (s *Synchronizer) Start(ctx context.Context, opts Options) error {
	if opts.StopTime.IsZero() {
		opts.StopTime = time.Now()
	}
	if opts.StartTime.IsZero() {
		// one month before stop time
		opts.StartTime = opts.StopTime.AddDate(0, -1, 0)
	}
	tokenIDs, err := s.getTokenIDs(ctx, opts.TokenIDs)
	if err != nil {
		return fmt.Errorf("failed to get tokenIDs: %w", err)
	}

	requiredFields, err := getRequiredEsFields(opts)
	if err != nil {
		return fmt.Errorf("failed to get required elastic fields: %w", err)
	}
	if opts.Parallel == 0 {
		opts.Parallel = defaultParallel
	}
	s.log.Info().Time("StartTime", opts.StartTime).Time("StopTime", opts.StopTime).Interface("TokenIDs", tokenIDs).Interface("Signals", opts.Signals).Int("Parallel", opts.Parallel).Msg("Starting sync for all tokenIDs")

	group, _ := errgroup.WithContext(ctx)
	group.SetLimit(opts.Parallel)
	for _, tokenID := range tokenIDs {
		tokenID := tokenID
		group.Go(func() error {
			s.sync(ctx, tokenID, &opts, requiredFields)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	s.log.Info().Msg("Finished syncing all tokenIDs")
	return nil
}

func (s *Synchronizer) sync(ctx context.Context, tokenID uint32, opts *Options, requiredFields []string) {
	logger := s.log
	logger.Info().Msg("Starting new token sync")
	// adjust stop time if there are already signals synced in clickhouse.
	stopTime, err := s.getStopTime(ctx, tokenID, opts)
	if err != nil {
		logger.Error().Err(err).Msg("failed to get next timestamp")
		return
	}
	logger = s.log.With().Time("stopTime", stopTime).Int("tokenID", int(tokenID)).Logger()
	startTime, err := s.getStartTime(ctx, tokenID, opts)
	if err != nil {
		logger.Error().Err(err).Msg("failed to get start time")
		return
	}
	subject, err := s.idGetter.SubjectFromTokenID(ctx, tokenID)
	if err != nil {
		logger.Error().Err(err).Msg("failed to get subject from tokenID")
		return
	}
	logger = logger.With().Str("subject", subject).Logger()
	s.idGetter.PrimeTokenIDCache(ctx, tokenID, subject)
	for startTime.Before(stopTime) {
		logger = logger.With().Time("date", startTime).Logger()
		err = s.processRecords(logger.WithContext(ctx), opts.BatchSize, startTime, stopTime, subject, requiredFields)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to process Records")
		}
		startTime = startTime.Add(time.Hour * 24).Truncate(time.Hour * 24)
	}
	logger.Info().Msg("Finished token sync")
}

// getStopTime returns the stop time for the given tokenID based on the latest signal if any.
func (s *Synchronizer) getStopTime(ctx context.Context, tokenID uint32, opts *Options) (time.Time, error) {
	oldestSignal, err := s.chService.QueryOldestSignal(ctx, tokenID, opts.Signals)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, fmt.Errorf("failed to get latest signal: %w", err)
		}
		return opts.StopTime, nil
	}
	return oldestSignal.Timestamp, nil
}

// getStopTime returns the stop time for the given tokenID based on the latest signal if any.
func (s *Synchronizer) getStartTime(ctx context.Context, tokenID uint32, opts *Options) (time.Time, error) {
	// TODO
	return time.Time{}, nil
}

// processRecords processes the records retrieved from Elasticsearch.
// It takes a context, batch size, oldest timestamp, stop time, and oldest subject as parameters.
// It converts the records to Clickhouse signals and inserts them into Clickhouse.
// The function returns the oldest timestamp and subject from the processed records, and an error if any.
func (s *Synchronizer) processRecords(ctx context.Context, batchSize int, startTime time.Time, stopTime time.Time, subject string, requiredFields []string) error {
	esRecords, err := s.getEsRecords(ctx, s.esService, batchSize, startTime, stopTime, subject, requiredFields)
	if err != nil {
		return err
	}
	signals := s.convertToClickhouseSignals(ctx, esRecords)
	err = s.chService.InsertIntoClickhouse(ctx, signals)
	if err != nil {
		return fmt.Errorf("failed to insert into clickhouse: %w", err)
	}
	return nil
}

// getEsRecords retrieves records from Elasticsearch. If the batch size is too large, it reduces the batch size and retries.
func (s *Synchronizer) getEsRecords(ctx context.Context, esService *elastic.Service, batchSize int, startTime, stopTime time.Time, subject string, requiredFields []string) ([][]byte, error) {
	logger := log.Ctx(ctx)
	var err error
	var esRecords [][]byte
	currentBatchSize := batchSize
	for {
		esRecords, err = s.esService.GetRecordsSince(ctx, currentBatchSize, startTime, stopTime, subject, requiredFields)
		if err != nil {
			if currentBatchSize > 1 && strings.Contains(err.Error(), "Data too large") {
				currentBatchSize /= 2
				logger.Warn().Int("batchSize", currentBatchSize).Msg("Data too large, reducing batch size")
				continue
			}
			return nil, fmt.Errorf("failed to get records elatic records: %w", err)
		}
		if len(esRecords) == 0 {
			// no more records to sync
			return nil, errNoRows
		}
		return esRecords, nil
	}
}

// convertToClickhouseValues converts the elastic search records to clickhouse signals.
func (s *Synchronizer) convertToClickhouseSignals(ctx context.Context, records [][]byte) []vss.Signal {
	signals := make([]vss.Signal, 0, len(records)*20)
	for _, record := range records {
		sigs, err := convert.SignalsFromPayload(ctx, s.idGetter, record)
		if err != nil {
			var versionError convert.VersionError
			if !errors.As(err, &versionError) || versionError.Version != "" {
				id := gjson.GetBytes(record, "id").String()
				s.log.Error().Err(err).Str("ID", id).Msg("failed to convert signals from v1 payload")
				continue
			}
			sigs, err = convert.SignalsFromV1Payload(ctx, s.idGetter, record)
			if err != nil {
				id := gjson.GetBytes(record, "id").String()
				s.log.Error().Err(err).Str("ID", id).Msg("failed to convert signal from v1 payload")
				continue
			}
		}
		signals = append(signals, sigs...)
	}
	return signals
}

// getTokenIDs returns the tokenIDs from the given tokenID strings or from clickhouse if none are provided.
func (s *Synchronizer) getTokenIDs(ctx context.Context, tokenIDStrs []string) ([]uint32, error) {
	if len(tokenIDStrs) == 0 {
		// get tokenIDs from clickhouse
		var err error
		tokenIDs, err := s.chService.QueryTokenIDs(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get tokenIDs: %w", err)
		}
		slices.Sort(tokenIDs)
		slices.Reverse(tokenIDs)
		return tokenIDs, nil
	}
	tokenIDs := []uint32{}
	for _, tokenIDStr := range tokenIDStrs {
		tokenID, err := strconv.Atoi(strings.TrimSpace(tokenIDStr))
		if err != nil {
			s.log.Error().Err(err).Msg("failed to convert tokenID to int")
			continue
		}
		tokenIDs = append(tokenIDs, uint32(tokenID))
	}

	return tokenIDs, nil
}

func getRequiredEsFields(opts Options) ([]string, error) {
	if len(opts.Signals) == 0 {
		return nil, nil
	}
	signalDefs, err := schema.LoadDefinitionFile(strings.NewReader(schema.DefinitionsYAML()))
	if err != nil {
		return nil, fmt.Errorf("failed to load schema definitions: %w", err)
	}
	var elasticFields []string
	for _, signal := range opts.Signals {
		signal = strings.TrimSpace(signal)
		defInfo, ok := signalDefs.FromName[signal]
		if !ok {
			continue
		}
		for _, conv := range defInfo.Conversions {
			elasticFields = append(elasticFields, "data."+conv.OriginalName)
		}
	}
	return elasticFields, nil
}
