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

    @staticmethod
    def return_connection(record : Dict[str, str]):
        return Connection (
                    id = str(record["id"]),
                    connection_name = record["connection_name"],
                    source_type = record["source_type"],
                    state = record["state"],
                    connection_properties = record["connection_properties"],
                    nifi_process_id = record["nifi_process_id"],
                    create_date = record["create_date"].strftime("%d %b %Y, %H:%M %p")
                )

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
                    RETURNING id, connection_name, source_type, state, connection_properties, nifi_process_id, create_date;
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
                new_connection = Connection.return_connection(inserted_record)

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
                        id, connection_name, source_type, state, connection_properties, nifi_process_id, create_date
                    FROM Connections;
                """
                cursor.execute(select_query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                results = []
                for row in rows:
                    record = dict(zip(columns, row))
                    results.append(Connection.return_connection(record))

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

    def delete(self) -> bool:
        """
        Deletes the connection record from the database.

        Returns:
        - bool: True if the deletion was successful, False otherwise
        """
        logger.info(f"Module:ConnectionModels. Attempting to delete Connection ID {self.id}.")
        env = get_db_env()
        try:
            conn = psycopg2.connect(
                dbname=env["dbname"], host=env["host"], user=env["user"], 
                password=env["password"], port=env["port"]
            )
            with conn.cursor() as cursor:
                delete_query = """
                    DELETE FROM Connections
                    WHERE id = %s;
                """
                cursor.execute(delete_query, (self.id,))
                if cursor.rowcount == 0:
                    logger.warning(f"Module:ConnectionModels. No records deleted. ID {self.id} may not exist.")
                    return False

                conn.commit()
                logger.info(f"Module:ConnectionModels. Connection ID {self.id} deleted successfully.")
                return True

        except Exception as e:
            logger.error(f"Module:ConnectionModels. Failed to delete Connection ID {self.id}: {e}")
            return False
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()





