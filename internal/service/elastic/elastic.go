package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/DIMO-Network/es-ch-sync/internal/config"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

// Service is responsible for interacting with an Elasticsearch instance.
type Service struct {
	EsClient    *elasticsearch.TypedClient
	StatusIndex string
}

func New(settings config.Settings) (*Service, error) {
	esConfig := elasticsearch.Config{
		Addresses: []string{settings.ElasticSearchAnalyticsHost},
		Username:  settings.ElasticSearchAnalyticsUsername,
		Password:  settings.ElasticSearchAnalyticsPassword,
	}
	es8Client, err := elasticsearch.NewTypedClient(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create elastic client: %w", err)
	}
	return &Service{
		EsClient:    es8Client,
		StatusIndex: settings.DeviceDataIndexName,
	}, nil
}

// GetRecordsSince is a method of the Synchronizer struct. It retrieves records from an Elasticsearch instance within a specified time range.
// It constructs a search request with a boolean filter query to Elasticsearch, where the time range is between the start and end times.
// The batch size determines the number of records to return in the response.
// The lastSubject and startTime is used to paginate the results.
func (s *Service) GetRecordsSince(ctx context.Context, batchSize int, startTime, endTime time.Time, subject string, requiredFields []string) ([][]byte, error) {
	startArg := strconv.Itoa(int(startTime.UnixMilli()))
	endArg := strconv.Itoa(int(endTime.UnixMilli()))
	query := &search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Filter: []types.Query{
					{Match: map[string]types.MatchQuery{"subject": {Query: subject}}},
					{Range: map[string]types.RangeQuery{"time": types.DateRangeQuery{Gte: &startArg, Lt: &endArg}}},
				},
			},
		},
		Size: some.Int(batchSize),
		Sort: []types.SortCombinations{
			types.SortOptions{
				SortOptions: map[string]types.FieldSort{
					"time": {Order: &sortorder.Desc},
				},
			},
		},
	}
	if len(requiredFields) > 0 {
		source := &types.SourceFilter{
			// get root fields
			Includes: []string{"subject", "dataschema", "id", "source", "time", "type"},
		}
		for _, field := range requiredFields {
			// filter for desired fields
			query.Query.Bool.Should = append(query.Query.Bool.Should, types.Query{Exists: &types.ExistsQuery{Field: field}})
			source.Includes = append(source.Includes, field)
		}
		query.Source_ = source
		query.Query.Bool.MinimumShouldMatch = 1
	}
	if queryBytes, err := json.Marshal(query); err != nil {
		log.Error().Err(err).Interface("query", query).Msg("failed to marshal query")
	} else {
		log.Info().Str("query", string(queryBytes)).Msg("Elastic Query")
	}
	res, err := s.EsClient.Search().Index(s.StatusIndex).Request(query).Perform(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to search elastic search: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	docs := [][]byte{}
	gjson.GetBytes(body, "hits.hits.#._source").ForEach(func(_, value gjson.Result) bool {
		data := resultToBytes(body, value)
		docs = append(docs, data)
		return true
	})
	log.Info().Str("response", string(body)).Msg("Elastic Response")
	return docs, nil
}

// resultToBytes converts the result to a byte slice.
// this logic is the recommended way to extract the raw bytes from a gjson result.
// more info: https://github.com/tidwall/gjson/blob/6ee9f877d683381343bc998c137339c7ae908b86/README.md#working-with-bytes
func resultToBytes(body []byte, result gjson.Result) []byte {
	var raw []byte
	if result.Index > 0 {
		raw = body[result.Index : result.Index+len(result.Raw)]
	} else {
		raw = []byte(result.Raw)
	}
	return raw
}
