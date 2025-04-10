import pyodbc
from dataclasses import dataclass, field
from typing import Dict, List

from structlog import get_logger

logger = get_logger()

@dataclass
class SQLServer:
    db_url: str
    db_name: str
    db_username: str
    db_password: str
    tables: List[str] = field(default_factory=list)

    def to_connection_properties(self) -> Dict:
        return {
            "db_url": self.db_url,
            "db_name": self.db_name,
            "db_username": self.db_username,
            "db_password": self.db_password,
            "tables": self.tables
        }
    
    def get_table_names(self) -> List:
        """
            Retrieve all table names from the SQL Server.
        """
        try:
            logger.info(f"Module:SQLServerModels. Started retrieving the table names.")

            conn_str = f"DRIVER={{SQL Server}};SERVER={self.db_url};DATABASE={self.db_name};UID={self.db_username};PWD={self.db_password}"
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

            logger.info(f"Module:SQLServerModels. Successfully retrieved the table names.")
            return tables
        except Exception as e:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.error(f"Module:SQLServerModels. Failed to retrieve the table names: {e}")
            return False

