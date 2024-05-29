package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/es-ch-sync/internal/config"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

var dialect = drivers.Dialect{
	LQ: '`',
	RQ: '`',

	UseIndexPlaceholders:    false,
	UseLastInsertID:         false,
	UseSchema:               false,
	UseDefaultKeyword:       false,
	UseAutoColumns:          false,
	UseTopClause:            false,
	UseOutputClause:         false,
	UseCaseWhenExistsClause: false,
}

// Service is responsible for handling ClickHouse database operations.
type Service struct {
	ChConn clickhouse.Conn
}

// New creates a new ClickHouse service using the provided settings.
func New(settings config.Settings) (*Service, error) {
	addr := fmt.Sprintf("%s:%d", settings.ClickHouseHost, settings.ClickHouseTCPPort)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Username: settings.ClickHouseUser,
			Password: settings.ClickHousePassword,
			Database: settings.ClickHouseDatabase,
		},
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}
	return &Service{ChConn: conn}, nil
}

// NewQuery initializes a new Query using the passed in QueryMods.
func NewQuery(mods ...qm.QueryMod) (string, []any) {
	q := &queries.Query{}
	queries.SetDialect(q, &dialect)
	qm.Apply(q, mods...)
	return queries.BuildQuery(q)
}

// QueryOldestSignal returns the oldest signal from the ClickHouse database with the given tokenID and signal names.
// if signalNames is empty, it returns the earliest signal for the given tokenID.
func (s *Service) QueryOldestSignal(ctx context.Context, tokenID uint32, signalNames []string) (*vss.Signal, error) {
	mods := []qm.QueryMod{
		qm.Select(vss.TimestampCol),
		qm.From(vss.TableName),
		qm.Where(vss.TokenIDCol+" = ?", tokenID),
		qm.OrderBy(vss.TimestampCol + " ASC"),
		qm.Limit(1),
	}
	if len(signalNames) > 0 {
		mods = append(mods, qm.WhereIn(vss.NameCol+" IN ?", signalNames))
	}
	stmt, args := NewQuery(mods...)
	row := s.ChConn.QueryRow(ctx, stmt, args...)
	if row.Err() != nil {
		return nil, fmt.Errorf("failed to query clickhouse: %w", row.Err())
	}
	latestSignal := vss.Signal{}
	if err := row.ScanStruct(&latestSignal); err != nil {
		return nil, fmt.Errorf("failed to scan clickhouse row: %w", err)
	}

	return &latestSignal, nil
}

func (s *Service) QueryTokenIDs(ctx context.Context) ([]uint32, error) {
	mods := []qm.QueryMod{
		qm.Select("DISTINCT " + vss.TokenIDCol),
		qm.From(vss.TableName),
	}
	stmt, args := NewQuery(mods...)
	rows, err := s.ChConn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query clickhouse: %w", err)
	}
	defer rows.Close()

	var tokenIDs []uint32
	for rows.Next() {
		var tokenID uint32
		if err := rows.Scan(&tokenID); err != nil {
			return nil, fmt.Errorf("failed to scan clickhouse row: %w", err)
		}
		tokenIDs = append(tokenIDs, tokenID)
	}
	return tokenIDs, nil
}

// InsertIntoClickhouse inserts the values into the clickhouse database
func (s *Service) InsertIntoClickhouse(ctx context.Context, signals []vss.Signal) error {
	query := "INSERT INTO " + vss.TableName
	batch, err := s.ChConn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare clickhouse statement: %w", err)
	}
	for i := range signals {
		err := batch.Append(vss.SignalToSlice(signals[i])...)
		if err != nil {
			return fmt.Errorf("failed to append clickhouse struct: %w", err)
		}
	}
	err = batch.Send()
	if err != nil {
		return fmt.Errorf("failed to execute clickhouse batch: %w", err)
	}
	return nil
}
