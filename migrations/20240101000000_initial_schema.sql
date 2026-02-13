-- TTDust Control Plane Initial Schema
-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Schemas table
CREATE TABLE schemas (
    schema_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL DEFAULT 'default',
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255) NOT NULL,
    UNIQUE(namespace, name)
);

CREATE INDEX idx_schemas_namespace ON schemas(namespace);

-- Schema versions table
CREATE TABLE schema_versions (
    version_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schema_id UUID NOT NULL REFERENCES schemas(schema_id),
    version INTEGER NOT NULL,
    definition JSONB NOT NULL,
    fingerprint VARCHAR(64) NOT NULL,
    compatibility_mode VARCHAR(32) NOT NULL DEFAULT 'BACKWARD',
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255) NOT NULL,
    UNIQUE(schema_id, version)
);

CREATE INDEX idx_schema_versions_schema ON schema_versions(schema_id);
CREATE INDEX idx_schema_versions_fingerprint ON schema_versions(fingerprint);

-- Streams table
CREATE TABLE streams (
    stream_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    schema_id UUID NOT NULL REFERENCES schemas(schema_id),
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    retention_policy JSONB NOT NULL DEFAULT '{"type": "time_based", "duration_days": 365}',
    partition_config JSONB NOT NULL DEFAULT '{"partition_by": "time", "granularity": "hourly"}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255) NOT NULL
);

CREATE INDEX idx_streams_status ON streams(status);
CREATE INDEX idx_streams_schema ON streams(schema_id);

-- Stream statistics table
CREATE TABLE stream_stats (
    stream_id UUID NOT NULL REFERENCES streams(stream_id),
    stat_date DATE NOT NULL,
    records_ingested BIGINT NOT NULL DEFAULT 0,
    bytes_ingested BIGINT NOT NULL DEFAULT 0,
    records_queried BIGINT NOT NULL DEFAULT 0,
    storage_bytes BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (stream_id, stat_date)
);

-- Pipelines table
CREATE TABLE pipelines (
    pipeline_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    stream_id UUID NOT NULL REFERENCES streams(stream_id),
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    dlq_stream_id UUID REFERENCES streams(stream_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pipelines_stream ON pipelines(stream_id);

-- Pipeline stages table
CREATE TABLE pipeline_stages (
    stage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    stage_order INTEGER NOT NULL,
    stage_type VARCHAR(64) NOT NULL,
    config JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    UNIQUE(pipeline_id, stage_order)
);

CREATE INDEX idx_pipeline_stages_pipeline ON pipeline_stages(pipeline_id);

-- Component health table
CREATE TABLE component_health (
    component_id VARCHAR(128) PRIMARY KEY,
    node_id VARCHAR(128) NOT NULL,
    component_type VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details JSONB
);

CREATE INDEX idx_component_health_node ON component_health(node_id);
CREATE INDEX idx_component_health_status ON component_health(status);

-- System configuration table
CREATE TABLE system_config (
    config_key VARCHAR(255) PRIMARY KEY,
    config_value JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(255) NOT NULL
);

-- API keys table
CREATE TABLE api_keys (
    key_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_hash VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(32) NOT NULL,
    stream_permissions JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    created_by VARCHAR(255) NOT NULL
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);

-- Audit log table
CREATE TABLE audit_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor VARCHAR(255) NOT NULL,
    action VARCHAR(64) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(255),
    details JSONB,
    ip_address INET,
    trace_id VARCHAR(64)
);

CREATE INDEX idx_audit_log_timestamp ON audit_log(timestamp);
CREATE INDEX idx_audit_log_actor ON audit_log(actor);
CREATE INDEX idx_audit_log_resource ON audit_log(resource_type, resource_id);
