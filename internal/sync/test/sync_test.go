package sync_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chconfig "github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
	"github.com/DIMO-Network/clickhouse-infra/pkg/container"
	clickhouseservice "github.com/DIMO-Network/es-ch-sync/internal/service/clickhouse"
	"github.com/DIMO-Network/es-ch-sync/internal/service/elastic"
	"github.com/DIMO-Network/es-ch-sync/internal/sync"
	"github.com/DIMO-Network/model-garage/pkg/migrations"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/bulk"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	esmodule "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
)

const deviceIndex = "device-status"

var insertBatchSize = 100
var (
	testFirstTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	//go:embed static_vehicle_data_test.json
	staticVehicleData []byte

	//go:embed status_mapping.json
	statusMapping []byte

	badData = map[string]any{
		"time": testFirstTime,
		"data": map[string]any{
			"bad": "format",
		},
		"source": "bad",
	}
)

type tokenGetter struct{}

func (t tokenGetter) TokenIDFromSubject(_ context.Context, subject string) (uint32, error) {
	tok, err := strconv.Atoi(subject)
	return uint32(tok), err
}
func (t tokenGetter) SubjectFromTokenID(_ context.Context, tokenID uint32) (string, error) {
	return strconv.Itoa(int(tokenID)), nil
}
func (t tokenGetter) PrimeTokenIDCache(context.Context, uint32, string) {}

func TestSync(t *testing.T) {
	sigsInRecord := 15       // each status has 15 signals
	recordsPerTimestamp := 8 // each timestamp has 8 records with different subjects
	recordsLoaded := recordsPerTimestamp * insertBatchSize
	totalSignals := (recordsLoaded * sigsInRecord)
	expectedSigs := (totalSignals / 2) + (sigsInRecord * recordsPerTimestamp) // we expect to see half of the signals + 1 for inclusive time range

	ctx := context.Background()
	syncer, chConn, cleanup := setupService(ctx, t)
	t.Cleanup(cleanup)

	// startTime is set to the get half of the inserted records
	startTime := testFirstTime.Add(time.Millisecond * time.Duration(insertBatchSize) / 2)
	opts := sync.Options{
		StartTime: startTime,
		BatchSize: insertBatchSize,
		TokenIDs:  []string{"1", "2 ", "3", "4	", " 5 ", "	6", "7", "8"},
	}
	err := syncer.Start(context.TODO(), opts)
	require.NoError(t, err)
	sigs := []vss.Signal{}
	rows, err := chConn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE %s != ''", vss.TableName, vss.NameCol))
	require.NoError(t, err)
	for rows.Next() {
		sig := vss.Signal{}
		err = rows.ScanStruct(&sig)
		require.NoError(t, err)
		sigs = append(sigs, sig)
	}
	require.Equal(t, expectedSigs, len(sigs), "expected number of signals")
}

func TestSyncWithTokenIDFromCH(t *testing.T) {
	sigsInRecord := 15       // each status has 15 signals
	recordsPerTimestamp := 8 // each timestamp has 8 records with different subjects
	recordsLoaded := recordsPerTimestamp * insertBatchSize
	totalSignals := (recordsLoaded * sigsInRecord)
	expectedSigs := (totalSignals / 2) + (sigsInRecord * recordsPerTimestamp) // we expect to see half of the signals + 1 for inclusive time range

	ctx := context.Background()
	syncer, chConn, cleanup := setupService(ctx, t)
	t.Cleanup(cleanup)

	// startTime is set to the get half of the inserted records
	startTime := testFirstTime.Add(time.Millisecond * time.Duration(insertBatchSize) / 2)
	opts := sync.Options{
		StartTime: startTime,
		BatchSize: insertBatchSize,
	}
	for i := 0; i < recordsPerTimestamp; i++ {
		// insert tokenID into clickhouse
		err := chConn.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (?, ?)", vss.TableName, vss.TokenIDCol, vss.TimestampCol), i+1, time.Now())
		require.NoError(t, err)
	}
	err := syncer.Start(context.TODO(), opts)
	require.NoError(t, err)
	sigs := []vss.Signal{}
	rows, err := chConn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE %s != ''", vss.TableName, vss.NameCol))
	require.NoError(t, err)
	for rows.Next() {
		sig := vss.Signal{}
		err = rows.ScanStruct(&sig)
		require.NoError(t, err)
		sigs = append(sigs, sig)
	}

	require.Equal(t, expectedSigs, len(sigs), "expected number of signals")
}

func TestSyncWithFieldFilter(t *testing.T) {
	sigsInRecord := 2        // each status has 2 signals
	recordsPerTimestamp := 8 // each timestamp has 8 records with different subjects
	recordsLoaded := recordsPerTimestamp * insertBatchSize
	totalSignals := (recordsLoaded * sigsInRecord)
	expectedSigs := (totalSignals / 2) + (sigsInRecord * recordsPerTimestamp) // we expect to see half of the signals + 1 for inclusive time range

	ctx := context.Background()
	syncer, chConn, cleanup := setupService(ctx, t)
	t.Cleanup(cleanup)

	// startTime is set to the get half of the inserted records
	startTime := testFirstTime.Add(time.Millisecond * time.Duration(insertBatchSize) / 2)
	opts := sync.Options{
		StartTime: startTime,
		BatchSize: insertBatchSize,
		TokenIDs:  []string{"1", "2", "3", "4", "5", "6", "7", "8"},
		Signals:   []string{"Vehicle.Speed", "Vehicle.Powertrain.FuelSystem.RelativeLevel"},
	}
	err := syncer.Start(context.TODO(), opts)
	require.NoError(t, err)
	sigs := []vss.Signal{}
	rows, err := chConn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE %s != ''", vss.TableName, vss.NameCol))
	require.NoError(t, err)
	for rows.Next() {
		sig := vss.Signal{}
		err = rows.ScanStruct(&sig)
		require.NoError(t, err)
		sigs = append(sigs, sig)
	}

	require.Equal(t, expectedSigs, len(sigs), "expected number of signals")
}

func TestSyncWithParrallel(t *testing.T) {
	sigsInRecord := 15       // each status has 15 signals
	recordsPerTimestamp := 8 // each timestamp has 7 records with different subjects
	recordsLoaded := recordsPerTimestamp * insertBatchSize
	totalSignals := (recordsLoaded * sigsInRecord)
	expectedSigs := (totalSignals / 2) + (sigsInRecord * recordsPerTimestamp) // we expect to see half of the signals + 1 for inclusive time range

	ctx := context.Background()
	syncer, chConn, cleanup := setupService(ctx, t)
	t.Cleanup(cleanup)

	// startTime is set to the get half of the inserted records
	startTime := testFirstTime.Add(time.Millisecond * time.Duration(insertBatchSize) / 2)
	opts := sync.Options{
		StartTime: startTime,
		BatchSize: insertBatchSize,
		Parallel:  2,
		TokenIDs:  []string{"1", "2", "3", "4", "5", "6", "7", "8"},
	}
	err := syncer.Start(context.TODO(), opts)
	require.NoError(t, err)
	sigs := []vss.Signal{}
	rows, err := chConn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE %s != ''", vss.TableName, vss.NameCol))
	require.NoError(t, err)
	for rows.Next() {
		sig := vss.Signal{}
		err = rows.ScanStruct(&sig)
		require.NoError(t, err)
		sigs = append(sigs, sig)
	}

	require.Equal(t, expectedSigs, len(sigs), "expected number of signals")
}

func TestSyncWithDataTooLarge(t *testing.T) {
	sigsInRecord := 15       // each status has 15 signals
	recordsPerTimestamp := 8 // each timestamp has 8 records with different subjects
	recordsLoaded := recordsPerTimestamp * insertBatchSize
	totalSignals := (recordsLoaded * sigsInRecord)
	expectedSigs := (totalSignals / 2) + (sigsInRecord * recordsPerTimestamp) // we expect to see half of the signals + 1 for inclusive time range

	ctx := context.Background()
	syncer, chConn, cleanup := setupService(ctx, t)
	t.Cleanup(cleanup)

	// startTime is set to the get half of the inserted records
	startTime := testFirstTime.Add(time.Millisecond * time.Duration(insertBatchSize) / 2)
	opts := sync.Options{
		StartTime: startTime,
		BatchSize: 10000,
		TokenIDs:  []string{"1", "2", "3", "4", "5", "6", "7", "8"},
	}
	err := syncer.Start(context.TODO(), opts)
	require.NoError(t, err)
	sigs := []vss.Signal{}
	rows, err := chConn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE %s != ''", vss.TableName, vss.NameCol))
	require.NoError(t, err)
	for rows.Next() {
		sig := vss.Signal{}
		err = rows.ScanStruct(&sig)
		require.NoError(t, err)
		sigs = append(sigs, sig)
	}

	require.Equal(t, expectedSigs, len(sigs), "expected number of signals")
}

// setupService creates a new elastic container and a new elastic service with the client and the logger.
func setupService(ctx context.Context, t *testing.T) (*sync.Synchronizer, clickhouse.Conn, func()) {
	t.Helper()
	chConn, es8Client, cleanup, err := Create(ctx)
	require.NoErrorf(t, err, "could not create containers: %v", err)

	logger := zerolog.New(os.Stdout).With().Timestamp().Str("app", "es-ch-sync-test").Logger()

	err = addStatusMapping(ctx, es8Client, deviceIndex)
	require.NoErrorf(t, err, "could not add status mapping: %v", err)
	chService := clickhouseservice.Service{
		ChConn: chConn,
	}
	esService := elastic.Service{
		EsClient:    es8Client,
		StatusIndex: deviceIndex,
	}
	service, err := sync.NewSychronizer(logger, &esService, &chService, &tokenGetter{})
	require.NoErrorf(t, err, "could not create synchronizer: %v", err)

	// insert mapping into Elasticsearch using a typed client
	loadStaticVehicleData(t, es8Client)

	return service, chConn, cleanup
}

// loadStaticVehicleData marshals staticVehicleData into a slice of byte slices.
// and then inserts each byte slice into the elastic index.
func loadStaticVehicleData(t *testing.T, client *elasticsearch.TypedClient) {
	t.Helper()

	// masrshal staticVehicleData into a slice of byte slices
	data := []map[string]any{}
	if err := json.Unmarshal(staticVehicleData, &data); err != nil {
		t.Fatalf("could not unmarshal static vehicle data: %v", err)
	}

	// loop through the data and add override the time field and subject for each object so we have a consistent state for the test.
	// The time field is incremented by 1 millisecond for each object.
	total := 0
	bulk := client.Bulk().Index(deviceIndex).Refresh(refresh.True)
	for i := range insertBatchSize {
		for j, obj := range data {
			obj["time"] = testFirstTime.Add(time.Millisecond * time.Duration(i+1))
			obj["subject"] = strconv.Itoa(j + 1)
			err := bulk.CreateOp(types.CreateOperation{}, obj)
			if err != nil {
				t.Fatalf("could not insert static vehicle data: %v", err)
			}
			total++
		}
		if total%10000 == 0 {
			fmt.Printf("inserted %d records\n", total)
			err := bulk.CreateOp(types.CreateOperation{}, badData)
			if err != nil {
				t.Fatalf("could not insert static vehicle data: %v", err)
			}
			resp, err := bulk.Do(context.Background())
			if err != nil {
				t.Fatalf("could not insert static vehicle data: %v", err)
			}
			if resp.Errors {
				t.Fatalf("bulk insert failed: %v", firstError(resp))
			}
			bulk = client.Bulk().Index(deviceIndex).Refresh(refresh.True)
		}
	}
	if total%10000 != 0 {
		resp, err := bulk.Do(context.Background())
		if err != nil {
			t.Fatalf("could not insert static vehicle data: %v", err)
		}
		if resp.Errors {
			t.Fatalf("bulk insert failed: %v", firstError(resp))
		}
	}
}

func Create(ctx context.Context) (clickhouse.Conn, *elasticsearch.TypedClient, func(), error) {
	esContainer, err := esmodule.RunContainer(
		ctx,
		testcontainers.WithImage("docker.elastic.co/elasticsearch/elasticsearch:8.3.0"),
		esmodule.WithPassword("testpassword"),
	)
	cleanup := func() {
		if err := esContainer.Terminate(ctx); err != nil {
			log.Fatalf("could not terminate Elasticsearch container: %v", err)
		}
	}
	if err != nil {
		return nil, nil, cleanup, fmt.Errorf("could not start Elasticsearch container: %v", err)
	}

	esConfig := elasticsearch.Config{
		Addresses: []string{esContainer.Settings.Address},
		Username:  "elastic",
		Password:  esContainer.Settings.Password,
		CACert:    esContainer.Settings.CACert,
	}
	es8Client, err := elasticsearch.NewTypedClient(esConfig)
	if err != nil {
		return nil, nil, cleanup, fmt.Errorf("failed to create elastic client: %w", err)
	}

	container, err := container.CreateClickHouseContainer(ctx, chconfig.Settings{})
	if err != nil {
		return nil, nil, cleanup, fmt.Errorf("could not start ClickHouse container: %v", err)
	}
	cleanup = func() {
		container.Terminate(ctx)
		if err := esContainer.Terminate(ctx); err != nil {
			log.Fatalf("could not terminate ClickHouse container: %v", err)
		}
	}

	db, err := container.GetClickhouseAsDB()
	if err != nil {
		return nil, nil, cleanup, fmt.Errorf("could not get ClickHouse database: %v", err)
	}

	err = migrations.RunGoose(ctx, []string{"up", "-v"}, db)
	if err != nil {
		return nil, nil, cleanup, fmt.Errorf("could not run migrations: %v", err)
	}

	chConn, err := container.GetClickHouseAsConn()
	if err != nil {
		return nil, nil, cleanup, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	return chConn, es8Client, cleanup, nil
}

func addStatusMapping(ctx context.Context, client *elasticsearch.TypedClient, index string) error {
	_, err := client.Indices.Create(index).WaitForActiveShards("1").Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	_, err = client.Indices.PutMapping(index).Raw(bytes.NewReader(statusMapping)).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to insert mapping: %w", err)
	}

	return nil
}

func firstError(resp *bulk.Response) string {
	if resp.Errors {
		for _, item := range resp.Items {
			for _, resp := range item {
				if resp.Error != nil {
					return *resp.Error.Reason
				}
			}
		}
	}
	return ""
}
