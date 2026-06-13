import duckdb

ACCESS_KEY = "2YU33V3SVWX748T166D3"
SECRET_KEY = "TwHZI75hDf5dhve+gmqIbDY3d8PKlGRjojw+NVrg"

# Порт MinIO API (не UI). Обычно 9000, проверь в docker-compose.yml:
# ports:
#   - "9000:9000"  <- API
#   - "9001:9001"  <- UI
MINIO_ENDPOINT = "localhost:9000"

con = duckdb.connect()

con.sql(
    f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_url_style = 'path';
    SET s3_endpoint = '{MINIO_ENDPOINT}';
    SET s3_access_key_id = '{ACCESS_KEY}';
    SET s3_secret_access_key = '{SECRET_KEY}';
    SET s3_use_ssl = FALSE;
    """
)

con.sql("""
    DESCRIBE SELECT * FROM read_parquet('s3://raw/github/users/2026-06-13/2026-06-13.parquet');
""").show()

con.close()