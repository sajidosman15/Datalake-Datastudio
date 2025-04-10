import psycopg2
from datetime import datetime, timezone
from psycopg2.extras import Json
from typing import Optional, Dict

from dataclasses import dataclass, field
from structlog import get_logger

from app.config import get_db_env

logger = get_logger()

@dataclass
class Connection:
    connection_name: str
    source_type: str
    state: Optional[str] = None
    connection_properties: Dict[str, str] = field(default_factory=dict)
    nifi_process_id: Optional[str] = None
    create_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def save(self):
        """
        Inserts data into the Connections table.
        """
        logger.info(f"Module:ConnectionModels. Started inserting data into Connection table.")
        env = get_db_env()
        try:
            conn = psycopg2.connect(
                dbname=env["dbname"], host=env["host"], user=env["user"], 
                password=env["password"], port=env["port"]
            )
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO Connections (
                        connection_name, source_type, connection_properties, state, nifi_process_id
                    ) VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """
                cursor.execute(insert_query, (
                    self.connection_name,
                    self.source_type,
                    Json(self.connection_properties),
                    self.state,
                    self.nifi_process_id
                ))
                inserted_id = cursor.fetchone()[0]
                conn.commit()

                cursor.close()
                conn.close()
                logger.info(f"Module:ConnectionModels. Insert into Connection table is successful. Record ID: {inserted_id}")
                return inserted_id
        except Exception as e:
            conn.rollback()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.error(f"Module:ConnectionModels. Insert into Connection table is failed: {e}")

