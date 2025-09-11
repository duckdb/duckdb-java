CREATE OR REPLACE TEMPORARY SECRET secret (
    TYPE s3,
    ENDPOINT '$ENV{MINIO_HOST}:$ENV{MINIO_PORT}',
    PROVIDER config,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    URL_STYLE 'path',
    USE_SSL false
)
