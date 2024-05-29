// Package sync is the entrypoint for starting an elastic clickhouse sync
package sync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/es-ch-sync/internal/service/clickhouse"
	"github.com/DIMO-Network/es-ch-sync/internal/service/elastic"
	"github.com/DIMO-Network/model-garage/pkg/schema"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
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
	s.log.Info().Time("StartTime", opts.StartTime).Time("StopTime", opts.StopTime).Interface("TokenIDs", tokenIDs).Interface("Signals", opts.Signals).Msg("Starting sync...")

	for _, tokenID := range tokenIDs {
		s.log.Info().Msg(fmt.Sprintf("Syncing tokenID: %d", tokenID))
		// adjust stop time if there are already signals synced in clickhouse.
		stopTime, err := s.getStopTime(ctx, tokenID, &opts)
		if err != nil {
			s.log.Error().Err(err).Msg("failed to get next timestamp")
			continue
		}
		subject, err := s.idGetter.SubjectFromTokenID(ctx, tokenID)
		if err != nil {
			s.log.Error().Err(err).Msg("failed to get subject from tokenID")
			continue
		}
		s.idGetter.PrimeTokenIDCache(ctx, tokenID, subject)
		for {
			stopTime, err = s.processRecords(ctx, opts.BatchSize, opts.StartTime, stopTime, subject, requiredFields)
			if err != nil {
				if errors.Is(err, errNoRows) {
					break
				}
				return err
			}
		}
		s.log.Info().Msg(fmt.Sprintf("Finished syncing tokenID: %d", tokenID))
	}

	s.log.Info().Msg("Finished syncing all tokenIDs")
	return nil
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

// processRecords processes the records retrieved from Elasticsearch.
// It takes a context, batch size, oldest timestamp, stop time, and oldest subject as parameters.
// It converts the records to Clickhouse signals and inserts them into Clickhouse.
// The function returns the oldest timestamp and subject from the processed records, and an error if any.
func (s *Synchronizer) processRecords(ctx context.Context, batchSize int, startTime time.Time, stopTime time.Time, subject string, requiredFields []string) (time.Time, error) {
	var lastTimestamp time.Time
	var err error
	var esRecords [][]byte
	var signals []vss.Signal
	var tokenID uint32
	defer func() {
		s.log.Info().Int("TokenID", int(tokenID)).Str("subject", subject).Time("lastTimestamp", lastTimestamp).Int("numSignals", len(signals)).Int("numRecords", len(esRecords)).Msg("Batch processed successfully")
	}()
	esRecords, err = s.esService.GetRecordsSince(ctx, batchSize, startTime, stopTime, subject, requiredFields)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get records elatic records: %w", err)
	}
	if len(esRecords) == 0 {
		// no more records to sync
		return time.Time{}, errNoRows
	}
	signals = s.convertToClickhouseSignals(ctx, esRecords)
	if len(signals) != 0 {
		tokenID = signals[0].TokenID
	}
	err = s.chService.InsertIntoClickhouse(ctx, signals)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to insert into clickhouse: %w", err)
	}
	lastRecord := esRecords[len(esRecords)-1]
	lastTimestamp, _ = convert.TimestampFromV1Data(lastRecord)
	return lastTimestamp, nil
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
		return tokenIDs, nil
	}
	tokenIDs := []uint32{}
	for _, tokenIDStr := range tokenIDStrs {
		tokenID, err := strconv.Atoi(tokenIDStr)
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
