package elastic

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/DIMO-Network/es-ch-sync/internal/config"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// Service is responsible for interacting with an Elasticsearch instance.
type Service struct {
	EsClient    *elasticsearch.TypedClient
	StatusIndex string
}

func New(settings config.Settings) (*Service, error) {
	esConfig := elasticsearch.Config{
		Addresses:     []string{settings.ElasticSearchAnalyticsHost},
		Username:      settings.ElasticSearchAnalyticsUsername,
		Password:      settings.ElasticSearchAnalyticsPassword,
		RetryOnStatus: []int{429, 502, 503, 504}, // Add 429 to the default list.
		MaxRetries:    5,
		RetryBackoff: func(attempt int) time.Duration {
			// Back off 5, 10, 20, 40, 80 seconds.
			return 5 * time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
		},
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
	docs := [][]byte{}
	startArg := strconv.Itoa(int(startTime.UnixMilli()))
	endArg := strconv.Itoa(int(endTime.UnixMilli()))
	index := s.StatusIndex + startTime.Format(time.DateOnly)
	var searchAfter []types.FieldValue
	for {
		req := &search.Request{
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Filter: []types.Query{
						{Match: map[string]types.MatchQuery{"subject": {Query: subject}}},
						{Range: map[string]types.RangeQuery{"time": types.DateRangeQuery{Gte: &startArg, Lte: &endArg}}},
					},
				},
			},
			Size:        some.Int(batchSize),
			Sort:        []types.SortCombinations{"id"},
			SearchAfter: searchAfter,
		}
		if len(requiredFields) > 0 {
			source := &types.SourceFilter{
				// get root fields
				Includes: []string{"subject", "dataschema", "id", "source", "time", "type"},
			}
			query := &types.Query{Bool: &types.BoolQuery{}}
			for _, field := range requiredFields {
				// filter for desired fields
				query.Bool.Should = append(query.Bool.Should, types.Query{Exists: &types.ExistsQuery{Field: field}})
				source.Includes = append(source.Includes, field)
			}
			req.Query = query
			req.Source_ = source
			req.Query.Bool.MinimumShouldMatch = 1
		}
		res, err := s.EsClient.Search().Index(index).Request(req).Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to search elastic search: %w", err)
		}
		if res.Shards_.Failed > 0 {
			return nil, fmt.Errorf("search request on index %s had %d failed shards", index, res.Shards_.Failed)
		}

		numHits := len(res.Hits.Hits)
		if numHits == 0 {
			// Done
			break
		}
		for i := range res.Hits.Hits {
			docs = append(docs, res.Hits.Hits[i].Source_)
		}
		searchAfter = res.Hits.Hits[numHits-1].Sort
	}

	return docs, nil
}
