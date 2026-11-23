CREATE TABLE executions (
    pipeline_id text NOT NULL,
    run_id uuid NOT NULL,
    batch_id text,
    status text NOT NULL CHECK (status IN ('running', 'completed', 'completed_with_warnings', 'failed', 'reused')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    event jsonb NOT NULL CHECK (jsonb_typeof(event) = 'object'),
    received_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, run_id),
    CHECK ((status = 'running') = (finished_at IS NULL)),
    CHECK (finished_at >= started_at)
);
CREATE INDEX executions_recent ON executions (started_at DESC, pipeline_id, run_id);
CREATE INDEX executions_pipeline_recent ON executions (pipeline_id, started_at DESC, run_id);
CREATE TABLE row_counts (
    pipeline_id text NOT NULL,
    run_id uuid NOT NULL,
    name text NOT NULL,
    count bigint CHECK (count >= 0),
    PRIMARY KEY (pipeline_id, run_id, name),
    FOREIGN KEY (pipeline_id, run_id) REFERENCES executions ON DELETE CASCADE
);
CREATE TABLE quality_metrics (
    pipeline_id text NOT NULL,
    run_id uuid NOT NULL,
    name text NOT NULL,
    count bigint CHECK (count >= 0),
    denominator bigint CHECK (denominator >= 0),
    rate double precision,
    PRIMARY KEY (pipeline_id, run_id, name),
    FOREIGN KEY (pipeline_id, run_id) REFERENCES executions ON DELETE CASCADE,
    CHECK (count <= denominator),
    CHECK (
        CASE WHEN count IS NULL OR denominator IS NULL OR denominator = 0
            THEN rate IS NULL
            ELSE rate IS NOT NULL AND rate BETWEEN 0 AND 1
                AND abs(rate - count::double precision / denominator) <= 1e-12
        END
    )
);
