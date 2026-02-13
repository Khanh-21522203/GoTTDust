package query

import (
	"fmt"
	"time"

	"GoTTDust/internal/common"
	"GoTTDust/internal/storage"
)

const (
	maxTimeRangeDays      = 7
	maxRecordsPerPage     = 10000
	defaultRecordsPerPage = 1000
	queryTimeout          = 30 * time.Second
	maxConcurrentFetch    = 10
)

// QueryPlan describes how a query will be executed.
type QueryPlan struct {
	Partitions      []common.PartitionKey
	Files           []storage.FileEntry
	CachePartitions []common.PartitionKey // Partitions to check cache first
	Limit           int
	Cursor          string
	Strategy        ExecutionStrategy
}

// ExecutionStrategy describes how to execute the query.
type ExecutionStrategy int

const (
	StrategyCacheLookup   ExecutionStrategy = iota // Check cache first
	StrategyIndexLookup                            // Use index
	StrategyPartitionScan                          // Scan partitions from S3
)

func (s ExecutionStrategy) String() string {
	switch s {
	case StrategyCacheLookup:
		return "CACHE_LOOKUP"
	case StrategyIndexLookup:
		return "INDEX_LOOKUP"
	case StrategyPartitionScan:
		return "PARTITION_SCAN"
	default:
		return "UNKNOWN"
	}
}

// Planner creates query execution plans.
type Planner struct {
	storageManager *storage.Manager
}

// NewPlanner creates a new query planner.
func NewPlanner(storageManager *storage.Manager) *Planner {
	return &Planner{
		storageManager: storageManager,
	}
}

// PlanTimeRange creates an execution plan for a time-range query.
func (p *Planner) PlanTimeRange(streamID common.StreamID, start, end time.Time, limit int, cursor string) (*QueryPlan, error) {
	// Validate time range
	if end.Before(start) {
		return nil, fmt.Errorf("%w: end time must be after start time", common.ErrValidation)
	}

	duration := end.Sub(start)
	if duration > maxTimeRangeDays*24*time.Hour {
		return nil, fmt.Errorf("%w: time range exceeds maximum of %d days", common.ErrValidation, maxTimeRangeDays)
	}

	// Normalize limit
	if limit <= 0 {
		limit = defaultRecordsPerPage
	}
	if limit > maxRecordsPerPage {
		limit = maxRecordsPerPage
	}

	// Step 1: Partition pruning - only scan partitions in the time range
	partitions := storage.GetPartitionsForTimeRange(streamID, start, end)

	// Step 2: File selection - get files from partition index
	var allFiles []storage.FileEntry
	for _, part := range partitions {
		files := p.storageManager.GetPartitionFiles(part)
		allFiles = append(allFiles, files...)
	}

	// Step 3: Determine cache-eligible partitions (recent data)
	var cachePartitions []common.PartitionKey
	recentThreshold := time.Now().Add(-1 * time.Hour)
	for _, part := range partitions {
		partTime := time.Date(part.Year, time.Month(part.Month), part.Day, part.Hour, 0, 0, 0, time.UTC)
		if partTime.After(recentThreshold) {
			cachePartitions = append(cachePartitions, part)
		}
	}

	// Step 4: Choose execution strategy
	strategy := StrategyPartitionScan
	if len(cachePartitions) == len(partitions) {
		strategy = StrategyCacheLookup
	}

	return &QueryPlan{
		Partitions:      partitions,
		Files:           allFiles,
		CachePartitions: cachePartitions,
		Limit:           limit,
		Cursor:          cursor,
		Strategy:        strategy,
	}, nil
}

// PlanKeyLookup creates an execution plan for a key-based lookup.
func (p *Planner) PlanKeyLookup(streamID common.StreamID, keyField, keyValue string) (*QueryPlan, error) {
	if keyField == "" || keyValue == "" {
		return nil, fmt.Errorf("%w: key_field and key_value are required", common.ErrValidation)
	}

	// For MVP, key lookup scans recent partitions
	// Only _record_id is indexed by default
	strategy := StrategyCacheLookup
	if keyField != "_record_id" {
		strategy = StrategyPartitionScan
	}

	return &QueryPlan{
		Limit:    1,
		Strategy: strategy,
	}, nil
}
