package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"GoTTDust/internal/common"
	pb "GoTTDust/internal/genproto/querypb"
	"GoTTDust/internal/ingestion"
	"GoTTDust/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// QueryHandler implements the gRPC QueryService.
type QueryHandler struct {
	pb.UnimplementedQueryServiceServer

	planner        *Planner
	storageManager *storage.Manager
	cache          Cache
	subManager     *ingestion.SubscriptionManager
}

// Cache is the interface for the query cache (L1 in-process + L2 Redis).
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration)
}

// NewQueryHandler creates a new query handler.
func NewQueryHandler(planner *Planner, storageManager *storage.Manager, cache Cache, subManager *ingestion.SubscriptionManager) *QueryHandler {
	return &QueryHandler{
		planner:        planner,
		storageManager: storageManager,
		cache:          cache,
		subManager:     subManager,
	}
}

// QueryTimeRange handles time-range queries with pagination.
func (h *QueryHandler) QueryTimeRange(ctx context.Context, req *pb.TimeRangeQueryRequest) (*pb.TimeRangeQueryResponse, error) {
	startTime := time.Now()

	if req.StreamId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "stream_id is required")
	}

	streamID := common.StreamID(req.StreamId)
	start := common.TimeFromMicros(req.StartTimeUnixMicros)
	end := common.TimeFromMicros(req.EndTimeUnixMicros)

	limit := int(defaultRecordsPerPage)
	if req.Limit != nil {
		limit = int(*req.Limit)
	}

	var cursor string
	if req.Cursor != nil {
		cursor = *req.Cursor
	}

	// Create query plan
	plan, err := h.planner.PlanTimeRange(streamID, start, end, limit, cursor)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan query: %v", err)
	}

	// Execute query
	records, nextCursor, metadata, err := h.executeTimeRange(ctx, plan, streamID, start, end)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "execute query: %v", err)
	}

	// Build response
	resp := &pb.TimeRangeQueryResponse{
		Records: records,
		Metadata: &pb.QueryMetadata{
			TotalRecordsScanned: metadata.TotalRecordsScanned,
			PartitionsScanned:   metadata.PartitionsScanned,
			CacheHits:           metadata.CacheHits,
			ExecutionTimeMicros: time.Since(startTime).Microseconds(),
		},
	}

	if nextCursor != "" {
		resp.NextCursor = &nextCursor
	}

	return resp, nil
}

// LookupByKey handles key-based lookups.
func (h *QueryHandler) LookupByKey(ctx context.Context, req *pb.KeyLookupRequest) (*pb.KeyLookupResponse, error) {
	if req.StreamId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "stream_id is required")
	}
	if req.KeyField == "" || req.KeyValue == "" {
		return nil, status.Errorf(codes.InvalidArgument, "key_field and key_value are required")
	}

	streamID := common.StreamID(req.StreamId)

	// Check cache first
	if h.cache != nil {
		cacheKey := fmt.Sprintf("key:%s:%s:%s", streamID, req.KeyField, req.KeyValue)
		if data, found := h.cache.Get(ctx, cacheKey); found {
			var record pb.Record
			if err := json.Unmarshal(data, &record); err == nil {
				return &pb.KeyLookupResponse{
					Record: &record,
					Found:  true,
				}, nil
			}
		}
	}

	// Scan recent partitions for the key
	record, found, err := h.scanForKey(ctx, streamID, req.KeyField, req.KeyValue)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "key lookup: %v", err)
	}

	resp := &pb.KeyLookupResponse{
		Found: found,
	}
	if found {
		resp.Record = record

		// Cache the result
		if h.cache != nil {
			cacheKey := fmt.Sprintf("key:%s:%s:%s", streamID, req.KeyField, req.KeyValue)
			if data, err := json.Marshal(record); err == nil {
				h.cache.Set(ctx, cacheKey, data, 5*time.Minute)
			}
		}
	}

	return resp, nil
}

// StreamingRead handles streaming read requests (server-streaming).
func (h *QueryHandler) StreamingRead(req *pb.StreamingReadRequest, stream pb.QueryService_StreamingReadServer) error {
	if req.StreamId == "" {
		return status.Errorf(codes.InvalidArgument, "stream_id is required")
	}

	streamID := common.StreamID(req.StreamId)
	ctx := stream.Context()

	// Determine start position
	var startSeq int64
	if req.StartSequence != nil {
		startSeq = *req.StartSequence
	} else if req.StartTime != nil {
		startSeq = 0 // Will filter by time
	}

	// Stream historical records from storage
	now := time.Now()
	lookback := 24 * time.Hour // Default lookback for historical
	startTime := now.Add(-lookback)
	if req.StartTime != nil {
		startTime = common.TimeFromMicros(*req.StartTime)
	}

	partitions := storage.GetPartitionsForTimeRange(streamID, startTime, now)

	for _, part := range partitions {
		files := h.storageManager.GetPartitionFiles(part)
		for _, file := range files {
			if file.MaxSequence < startSeq {
				continue // Skip files before start sequence
			}

			records, err := h.readFileRecords(ctx, file.Key)
			if err != nil {
				continue // Skip unreadable files
			}

			for _, rec := range records {
				if rec.SequenceNumber < startSeq {
					continue
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if err := stream.Send(&pb.StreamingReadResponse{
					Record:       rec,
					IsHistorical: true,
				}); err != nil {
					return err
				}
			}
		}
	}

	// After historical records, subscribe to live data
	if h.subManager == nil {
		return nil
	}

	sub := h.subManager.Subscribe(streamID)
	defer h.subManager.Unsubscribe(sub)

	for {
		select {
		case <-ctx.Done():
			return nil
		case record, ok := <-sub.Ch():
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.StreamingReadResponse{
				Record: &pb.Record{
					RecordId:            string(record.RecordID),
					StreamId:            string(record.StreamID),
					SequenceNumber:      int64(record.SequenceNumber),
					TimestampUnixMicros: record.IngestedAt.UnixMicro(),
					Payload:             record.Payload,
				},
				IsHistorical: false,
			}); err != nil {
				return err
			}
		}
	}
}

// --- internal helpers ---

type queryMetadata struct {
	TotalRecordsScanned int64
	PartitionsScanned   int64
	CacheHits           int64
}

func (h *QueryHandler) executeTimeRange(
	ctx context.Context,
	plan *QueryPlan,
	streamID common.StreamID,
	start, end time.Time,
) ([]*pb.Record, string, queryMetadata, error) {
	var (
		mu       sync.Mutex
		allRecs  []*pb.Record
		metadata queryMetadata
	)

	metadata.PartitionsScanned = int64(len(plan.Partitions))

	// Parallel fetch from S3 (up to maxConcurrentFetch)
	sem := make(chan struct{}, maxConcurrentFetch)
	var wg sync.WaitGroup
	var fetchErr error

	for _, file := range plan.Files {
		select {
		case <-ctx.Done():
			return nil, "", metadata, ctx.Err()
		default:
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(f storage.FileEntry) {
			defer wg.Done()
			defer func() { <-sem }()

			records, err := h.readFileRecords(ctx, f.Key)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = err
				}
				mu.Unlock()
				return
			}

			// Filter by time range
			var filtered []*pb.Record
			for _, rec := range records {
				recTime := common.TimeFromMicros(rec.TimestampUnixMicros)
				if !recTime.Before(start) && !recTime.After(end) {
					filtered = append(filtered, rec)
				}
			}

			mu.Lock()
			allRecs = append(allRecs, filtered...)
			metadata.TotalRecordsScanned += int64(len(records))
			mu.Unlock()
		}(file)
	}

	wg.Wait()

	if fetchErr != nil {
		return nil, "", metadata, fetchErr
	}

	// Sort by timestamp
	sort.Slice(allRecs, func(i, j int) bool {
		return allRecs[i].TimestampUnixMicros < allRecs[j].TimestampUnixMicros
	})

	// Apply cursor (skip records before cursor position)
	if plan.Cursor != "" {
		cursorSeq, err := decodeCursor(plan.Cursor)
		if err == nil {
			idx := 0
			for idx < len(allRecs) && allRecs[idx].SequenceNumber <= cursorSeq {
				idx++
			}
			allRecs = allRecs[idx:]
		}
	}

	// Apply limit
	var nextCursor string
	if len(allRecs) > plan.Limit {
		lastRec := allRecs[plan.Limit-1]
		nextCursor = encodeCursor(lastRec.SequenceNumber)
		allRecs = allRecs[:plan.Limit]
	}

	return allRecs, nextCursor, metadata, nil
}

func (h *QueryHandler) readFileRecords(ctx context.Context, key string) ([]*pb.Record, error) {
	data, err := h.storageManager.ReadFile(ctx, key)
	if err != nil {
		return nil, err
	}

	var batch struct {
		Records []storage.ParquetRecord `json:"records"`
	}
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("unmarshal file %s: %w", key, err)
	}

	records := make([]*pb.Record, 0, len(batch.Records))
	for _, pr := range batch.Records {
		records = append(records, &pb.Record{
			RecordId:            pr.RecordID,
			StreamId:            pr.StreamID,
			SequenceNumber:      pr.Sequence,
			TimestampUnixMicros: pr.IngestedAt,
			Payload:             pr.Payload,
		})
	}

	return records, nil
}

func (h *QueryHandler) scanForKey(ctx context.Context, streamID common.StreamID, keyField, keyValue string) (*pb.Record, bool, error) {
	// Scan recent partitions (last 24h) for the key
	now := time.Now()
	start := now.Add(-24 * time.Hour)
	partitions := storage.GetPartitionsForTimeRange(streamID, start, now)

	// Scan in reverse chronological order to find most recent match
	for i := len(partitions) - 1; i >= 0; i-- {
		files := h.storageManager.GetPartitionFiles(partitions[i])
		for j := len(files) - 1; j >= 0; j-- {
			records, err := h.readFileRecords(ctx, files[j].Key)
			if err != nil {
				continue
			}

			// Scan records in reverse for most recent match
			for k := len(records) - 1; k >= 0; k-- {
				rec := records[k]
				if keyField == "_record_id" && rec.RecordId == keyValue {
					return rec, true, nil
				}

				// For other fields, parse payload and check
				if keyField != "_record_id" {
					var payload map[string]interface{}
					if err := json.Unmarshal(rec.Payload, &payload); err != nil {
						continue
					}
					if fmt.Sprintf("%v", payload[keyField]) == keyValue {
						return rec, true, nil
					}
				}
			}
		}
	}

	return nil, false, nil
}

// Cursor encoding/decoding
func encodeCursor(seq int64) string {
	data, _ := json.Marshal(map[string]int64{"seq": seq})
	return base64.URLEncoding.EncodeToString(data)
}

func decodeCursor(cursor string) (int64, error) {
	data, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	var c map[string]int64
	if err := json.Unmarshal(data, &c); err != nil {
		return 0, err
	}
	seq, ok := c["seq"]
	if !ok {
		return 0, fmt.Errorf("invalid cursor")
	}
	return seq, nil
}
