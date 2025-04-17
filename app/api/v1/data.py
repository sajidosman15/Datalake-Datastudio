
import structlog
from fastapi import APIRouter, status, HTTPException
from clickhouse_connect import get_client
from app.config import get_clickhouse_env

logger = structlog.get_logger()
router = APIRouter(prefix="/data", tags=["Get Data"])


@router.get("/{database}/{table}",status_code=status.HTTP_200_OK,)
def get_table_data(database: str, table: str):
    try:
        env = get_clickhouse_env()
        client = get_client (host = env["host"], port = env["port"], username = env["username"], password = env["password"], database = database)

        query = f"SELECT * FROM {database}.{table}"
        result = client.query(query)
        
        if not result.result_rows:
            logger.warning(f"No data returned from {database}.{table}")
            return []

        # Convert to array of objects (row-wise dicts)
        records = [dict(zip(result.column_names, row)) for row in result.result_rows]
        logger.info(f"Retrieved {len(records)} records from {database}.{table}")

        return records

    except Exception as e:
        logger.error(f"Error fetching data from {database}.{table}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while retrieving data")