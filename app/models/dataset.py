import psycopg2
from datetime import datetime, timezone
from psycopg2.extras import Json
from typing import Optional, Dict

from dataclasses import dataclass, field
from structlog import get_logger

from app.config import get_db_env

logger = get_logger()

@dataclass
class Dataset:
    id: Optional[int] = None
    dataset_name: str = ""
    api_version: str = ""
    dataset_owner: str = ""
    visibility: str = ""
    table_name: str = ""
    dashboard_url: str = ""
    connection_id: Optional[int] = None
    create_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @staticmethod
    def return_dataset(record : Dict[str, str]):
        return Dataset (
                    id = str(record["id"]),
                    dataset_name = record["dataset_name"],
                    api_version = record["api_version"],
                    dataset_owner = record["dataset_owner"],
                    visibility = record["visibility"],
                    table_name = record["table_name"],
                    dashboard_url = record["dashboard_url"],
                    connection_id = record["connection_id"],
                    create_date = record["create_date"].strftime("%d %b %Y, %H:%M %p")
                )

    @staticmethod
    def list_all():
        """
        Retrieves all records from the Datasets table.
        """
        logger.info("Module:DatasetModels. Started retrieving data from Dataset table.")
        env = get_db_env()
        try:
            conn = psycopg2.connect(
                dbname=env["dbname"], host=env["host"], user=env["user"], 
                password=env["password"], port=env["port"]
            )
            with conn.cursor() as cursor:
                select_query = """
                    SELECT 
                        id, dataset_name, api_version, dataset_owner, visibility, table_name, dashboard_url, connection_id, create_date
                    FROM Datasets;
                """
                cursor.execute(select_query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                results = []
                for row in rows:
                    record = dict(zip(columns, row))
                    results.append(Dataset.return_dataset(record))

                logger.info(f"Module:DatasetModels. Retrieved {len(results)} record(s) from Dataset table.")
                return results

        except Exception as e:
            logger.error(f"Module:DatasetModels. Failed to retrieve data from Dataset table: {e}")
            return []
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()



