package processing

import (
	"GoTTDust/internal/common"
	"GoTTDust/internal/processing/transforms"
)

// NewFieldProjection creates a FieldProjection transform from a stage config.
func NewFieldProjection(config map[string]interface{}) (Transform, error) {
	return transforms.NewFieldProjectionFromConfig(config)
}

// NewTimestampNormalization creates a TimestampNormalization transform from a stage config.
func NewTimestampNormalization(config map[string]interface{}) (Transform, error) {
	return transforms.NewTimestampNormalizationFromConfig(config)
}

// NewTypeCoercion creates a TypeCoercion transform from a stage config.
func NewTypeCoercion(config map[string]interface{}) (Transform, error) {
	return transforms.NewTypeCoercionFromConfig(config)
}

// PipelineManager manages processing pipelines for streams.
type PipelineManager struct {
	pipelines map[common.StreamID]*Pipeline
}

// NewPipelineManager creates a new pipeline manager.
func NewPipelineManager() *PipelineManager {
	return &PipelineManager{
		pipelines: make(map[common.StreamID]*Pipeline),
	}
}

// Register registers a pipeline for a stream.
func (pm *PipelineManager) Register(config common.PipelineConfig) error {
	var dlqWriter *DLQWriter
	if config.DLQConfig.Enabled {
		dlqWriter = NewDLQWriter(config.DLQConfig.StreamID, config.DLQConfig.RetentionDays)
	}

	pipeline, err := NewPipeline(config, dlqWriter)
	if err != nil {
		return err
	}

	pm.pipelines[config.StreamID] = pipeline
	return nil
}

// Get returns the pipeline for a stream.
func (pm *PipelineManager) Get(streamID common.StreamID) *Pipeline {
	return pm.pipelines[streamID]
}

// ProcessRecords processes records through the pipeline for a stream.
// If no pipeline is registered, records pass through unchanged.
func (pm *PipelineManager) ProcessRecords(streamID common.StreamID, records []*common.ValidatedRecord) []*PipelineResult {
	pipeline := pm.pipelines[streamID]
	if pipeline == nil {
		// No pipeline: pass through
		results := make([]*PipelineResult, len(records))
		for i, r := range records {
			results[i] = &PipelineResult{
				Record:  r,
				Payload: r.Payload,
				Failed:  false,
			}
		}
		return results
	}

	return pipeline.ProcessBatch(records)
}
