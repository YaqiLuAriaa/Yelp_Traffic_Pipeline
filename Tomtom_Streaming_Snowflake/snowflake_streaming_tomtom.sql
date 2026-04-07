-- =============================================================================
-- 1. Description: Setup Infrastructure-level Integrations (GCS & Pub/Sub)
-- =============================================================================

-- 1. Cloud Storage Integration (GCS)
CREATE OR REPLACE STORAGE INTEGRATION gcs_role_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://traffic-data-lake-us/');

-- Retrieve service account for GCS IAM binding
DESC STORAGE INTEGRATION gcs_role_integration;

-- 2. Notification Integration (GCP Pub/Sub)
CREATE OR REPLACE NOTIFICATION INTEGRATION gcp_pubsub_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/fluted-lambda-489221-b8/subscriptions/snowflake-traffic-sub';

-- Retrieve service account for Pub/Sub Publisher role binding
DESC NOTIFICATION INTEGRATION GCP_PUBSUB_INT;



-- =============================================================================
-- 2. Define Stage and Landing Table for TomTom Traffic Data
-- Database: MSBA405 | Schema: RAW
-- =============================================================================

-- 1. Define Reusable File Format
CREATE OR REPLACE FILE FORMAT MSBA405.RAW.PARQUET_FORMAT
  TYPE = PARQUET;

-- 2. Define External Stage
CREATE OR REPLACE STAGE MSBA405.RAW.GCS_STAGE
  URL = 'gcs://traffic-data-lake-us/tomtom/'
  STORAGE_INTEGRATION = gcs_role_integration
  FILE_FORMAT = MSBA405.RAW.PARQUET_FORMAT;

-- Verification
LIST @MSBA405.RAW.GCS_STAGE;

-- 3. Create Raw Landing Table
CREATE OR REPLACE TABLE MSBA405.RAW.STREAMING_RAW (
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_data VARIANT
);

-- 4. Data Discovery (Debug use)
-- 1/ Check final data format
-- SELECT raw_data
-- FROM MSBA405.RAW.STREAMING_RAW
-- WHERE raw_data:timestamp::STRING LIKE '2026-%'
-- ORDER BY raw_data:timestamp::TIMESTAMP_NTZ DESC
-- LIMIT 10;

-- 2/ Check copy history
-- SELECT *
-- FROM TABLE(MSBA405.INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'MSBA405.RAW.STREAMING_RAW',
--     START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
-- ));

-- =============================================================================
-- 3. Automation via Snowpipe and Downstream Analytics View
-- Database: MSBA405 | Schema: RAW / ANALYTICS
-- =============================================================================

-- 1. Snowpipe for Auto-Ingestion
CREATE OR REPLACE PIPE MSBA405.RAW.STREAMING_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = 'GCP_PUBSUB_INT'
AS
COPY INTO MSBA405.RAW.STREAMING_RAW(raw_data)
FROM (SELECT $1 FROM @MSBA405.RAW.GCS_STAGE);

-- Pause and Resume the Pipe
-- ALTER PIPE MSBA405.RAW.STREAMING_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
-- ALTER PIPE MSBA405.RAW.STREAMING_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;

-- Pipe Management (Maintenance)
-- ALTER PIPE MSBA405.RAW.STREAMING_PIPE REFRESH;
-- SELECT SYSTEM$PIPE_STATUS('MSBA405.RAW.STREAMING_PIPE');


-- 2. Analytics View (All final records)
CREATE OR REPLACE VIEW MSBA405.ANALYTICS.TRAFFIC_FINAL AS
SELECT 
    -- 1/ Get business info
    raw_data:BUSINESS_ID::STRING AS business_id,
    SPLIT_PART(raw_data:coord::STRING, ',', 1)::FLOAT AS lat,
    SPLIT_PART(raw_data:coord::STRING, ',', 2)::FLOAT AS lon,
    
    -- 2/ Metadata
    raw_data:city::STRING AS city,
    raw_data:frc::STRING AS road_class,
    raw_data:confidence::FLOAT AS data_confidence,
    raw_data:roadClosure::BOOLEAN AS is_closed,
    
    -- 3/ Speed & Time
    raw_data:currentSpeed::INT AS current_speed,
    raw_data:freeFlowSpeed::INT AS free_flow_speed,
    raw_data:currentTravelTime::INT AS current_travel_time,
    raw_data:freeFlowTravelTime::INT AS free_flow_travel_time,
    
    -- 4/ Speed ratio
    (raw_data:currentSpeed::FLOAT / NULLIF(raw_data:freeFlowSpeed::FLOAT, 0)) AS speed_ratio,
       
    -- 5/ timestamp
    CONVERT_TIMEZONE('UTC', 'America/New_York', raw_data:timestamp::TIMESTAMP_NTZ) AS event_time

-- Use only final pipeline data
FROM MSBA405.RAW.STREAMING_RAW
WHERE raw_data:timestamp::STRING LIKE '2026-%'
    AND raw_data:BUSINESS_ID::STRING IS NOT NULL;


-- 3. Analytics View (Only latest records)
CREATE OR REPLACE VIEW MSBA405.ANALYTICS.TRAFFIC_LATEST AS
SELECT 
    business_id,
    lat,
    lon,
    city,
    road_class,
    data_confidence,
    is_closed,
    current_speed,
    free_flow_speed,
    current_travel_time,
    free_flow_travel_time,
    speed_ratio,
    event_time
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY event_time DESC) AS rn
    FROM MSBA405.ANALYTICS.TRAFFIC_FINAL
)
WHERE rn = 1;

-- 3. Data Discovery (Check time bucket)
-- SELECT 
--     DATE_TRUNC('minute', event_time) AS time_bucket,
--     COUNT(*) AS row_count
-- FROM MSBA405.ANALYTICS.TRAFFIC_FINAL
-- GROUP BY 1
-- ORDER BY 1 DESC
-- LIMIT 20;


-- =============================================================================
-- 4. Verification of the pipeline and downstrewam view
-- Database: MSBA405 | Schema: RAW / ANALYTICS
-- =============================================================================

-- Pipe Status Verification
SELECT SYSTEM$PIPE_STATUS('MSBA405.RAW.STREAMING_PIPE');

-- View Update Verification
SELECT CONVERT_TIMEZONE('America/New_York', 'America/Los_Angeles', event_time) AS last_ingestion_pst, *
FROM MSBA405.ANALYTICS.TRAFFIC_LATEST;
