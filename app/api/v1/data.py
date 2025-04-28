
import structlog
from fastapi import APIRouter, status, HTTPException
from app.config import config

logger = structlog.get_logger()
router = APIRouter(prefix="/data", tags=["Get Data"])

@router.get("/{database}/{table}",status_code=status.HTTP_200_OK,)
def get_table_data(database: str, table: str):
    try:
        client = config.clickhouse.get_connection(database)

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