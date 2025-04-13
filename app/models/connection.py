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
    id: Optional[int] = None
    connection_name: str = ""
    source_type: str = ""
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
                    RETURNING id, connection_name, source_type, state, nifi_process_id, create_date;
                """
                cursor.execute(insert_query, (
                    self.connection_name,
                    self.source_type,
                    Json(self.connection_properties),
                    self.state,
                    self.nifi_process_id
                ))
                inserted_record = cursor.fetchone()
                columns = [desc[0] for desc in cursor.description]
                inserted_record = dict(zip(columns, inserted_record))
                new_connection = Connection(
                    id=inserted_record["id"],
                    connection_name=inserted_record["connection_name"],
                    source_type=inserted_record["source_type"],
                    state=inserted_record["state"],
                    nifi_process_id=inserted_record["nifi_process_id"],
                    create_date=inserted_record["create_date"]
                )

                conn.commit()
                logger.info(f"Module:ConnectionModels. Insert into Connection table is successful. Record ID: {new_connection.id}")
                return new_connection
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Module:ConnectionModels. Insert into Connection table is failed: {e}")
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()

    @staticmethod
    def list_all():
        """
        Retrieves all records from the Connections table.
        """
        logger.info("Module:ConnectionModels. Started retrieving data from Connection table.")
        env = get_db_env()
        try:
            conn = psycopg2.connect(
                dbname=env["dbname"], host=env["host"], user=env["user"], 
                password=env["password"], port=env["port"]
            )
            with conn.cursor() as cursor:
                select_query = """
                    SELECT 
                        id, connection_name, source_type, connection_properties, state, nifi_process_id, create_date
                    FROM Connections;
                """
                cursor.execute(select_query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                results = []
                for row in rows:
                    record = dict(zip(columns, row))
                    record["id"] = str(record["id"])
                    if record["create_date"]:
                        record["create_date"] = record["create_date"].strftime("%d %b %Y, %H:%M %p")
                    results.append(record)

                logger.info(f"Module:ConnectionModels. Retrieved {len(results)} record(s) from Connection table.")
                return results

        except Exception as e:
            logger.error(f"Module:ConnectionModels. Failed to retrieve data from Connection table: {e}")
            return []
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()

    def update_state(self, new_state: str) -> bool:
        """
        Updates the state of a connection record.

        Parameters:
        - new_state: str — the new state value to set

        Returns:
        - bool: True if the update was successful, False otherwise
        """
        logger.info(f"Module:ConnectionModels. Attempting to update state for Connection ID {self.id} to '{new_state}'.")
        env = get_db_env()
        try:
            conn = psycopg2.connect(
                dbname=env["dbname"], host=env["host"], user=env["user"], 
                password=env["password"], port=env["port"]
            )
            with conn.cursor() as cursor:
                update_query = """
                    UPDATE Connections
                    SET state = %s
                    WHERE id = %s;
                """
                cursor.execute(update_query, (new_state, self.id))
                if cursor.rowcount == 0:
                    logger.warning(f"Module:ConnectionModels. No records updated. ID {self.id} may not exist.")
                    return False

                conn.commit()
                logger.info(f"Module:ConnectionModels. State updated successfully for Connection ID {self.id}.")
                return True

        except Exception as e:
            logger.error(f"Module:ConnectionModels. Failed to update state for Connection ID {self.id}: {e}")
            return False
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()




