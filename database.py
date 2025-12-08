"""
Database operations for the DuckDB application.
Provides connection management and data access layer.
"""
import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple

import duckdb
import pandas as pd

from config import config

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass


class DatabaseConnection:
    """Manages DuckDB database connections with proper error handling."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.get_database_path()
        self._connection = None

    @contextmanager
    def get_connection(self, read_only: bool = False):
        """Context manager for database connections."""
        conn = None
        try:
            conn = duckdb.connect(database=self.db_path, read_only=read_only)
            logger.debug(f"Connected to database: {self.db_path}")
            yield conn
        except duckdb.Error as e:
            logger.error(f"Database error: {e}")
            raise DatabaseError(f"Failed to connect to database: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise DatabaseError(f"Unexpected database error: {e}") from e
        finally:
            if conn:
                try:
                    conn.close()
                    logger.debug("Database connection closed")
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> None:
        """Execute a query without returning results."""
        with self.get_connection() as conn:
            try:
                if params:
                    conn.execute(query, params)
                else:
                    conn.execute(query)
                logger.debug("Query executed successfully")
            except duckdb.Error as e:
                logger.error(f"Query execution failed: {query}")
                raise DatabaseError(f"Query execution failed: {e}") from e

    def fetch_dataframe(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """Execute a query and return results as a DataFrame."""
        with self.get_connection(read_only=True) as conn:
            try:
                if params:
                    result = pd.read_sql(query, conn, params=params)
                else:
                    result = pd.read_sql(query, conn)
                logger.debug(f"Query returned {len(result)} rows")
                return result
            except duckdb.Error as e:
                logger.error(f"Query failed: {query}")
                raise DatabaseError(f"Query failed: {e}") from e


class FlightRepository:
    """Repository for flight-related database operations."""

    def __init__(self, db_connection: Optional[DatabaseConnection] = None):
        self.db = db_connection or DatabaseConnection()

    def get_all_flights(self) -> pd.DataFrame:
        """Get all flight records excluding specified journey type."""
        query = f"""
            SELECT * FROM {config.MAIN_TABLE}        
        """
        return self.db.fetch_dataframe(query)

    def insert_flight(self, row_data: Dict[str, Any]) -> None:
        """Insert a new flight record into the main table."""
        columns = [
            'PaxName', 'BookingRef', 'ETicketNo', 'ClientCode', 'Airline', 'JourneyType',
            'FlightNumber1', 'FlightNumber2', 'FlightNumber3', 'FlightNumber4',
            'FlightNumber5', 'FlightNumber6', 'FlightNumber7',
            'DepartureDateLocal1', 'DepartureDateLocal2', 'DepartureDateLocal3',
            'DepartureDateLocal4', 'DepartureDateLocal5', 'DepartureDateLocal6',
            'DepartureDateLocal7', 'Airport1', 'Airport2', 'Airport3', 'Airport4',
            'Airport5', 'Airport6', 'Airport7', 'Airport8'
        ]

        placeholders = ', '.join(['?' for _ in columns])
        query = f"""
            INSERT INTO {config.MAIN_TABLE} ({', '.join(columns)})
            VALUES ({placeholders})
        """

        # Convert dict to list in the correct order, then modify ETicketNo
        params = [row_data.get(col, None) for col in columns]

        # Set ETicketNo to INSERT
        params[2] = config.INSERTED_TICKET_NO  # ETicketNo position

        # Convert to tuple for query execution
        params = tuple(params)

        self.db.execute_query(query, params)
        logger.info(f"INSERTED ROW FOR BOOKING {row_data.get('BookingRef', 'Unknown')}")

    def update_flight(self, row_data: Dict[str, Any]) -> None:
        """Update an existing flight record."""
        query = f"""
            UPDATE {config.MAIN_TABLE}
            SET ETicketNo = ?,
                FlightNumber1 = ?, FlightNumber2 = ?, FlightNumber3 = ?,
                FlightNumber4 = ?, FlightNumber5 = ?, FlightNumber6 = ?,
                FlightNumber7 = ?, DepartureDateLocal1 = ?, DepartureDateLocal2 = ?,
                DepartureDateLocal3 = ?, DepartureDateLocal4 = ?, DepartureDateLocal5 = ?,
                DepartureDateLocal6 = ?, DepartureDateLocal7 = ?
            WHERE PaxName = ? AND BookingRef = ?
        """

        params = (
            config.UPDATED_TICKET_NO,
            row_data.get('FlightNumber1'), row_data.get('FlightNumber2'),
            row_data.get('FlightNumber3'), row_data.get('FlightNumber4'),
            row_data.get('FlightNumber5'), row_data.get('FlightNumber6'),
            row_data.get('FlightNumber7'),
            row_data.get('DepartureDateLocal1'), row_data.get('DepartureDateLocal2'),
            row_data.get('DepartureDateLocal3'), row_data.get('DepartureDateLocal4'),
            row_data.get('DepartureDateLocal5'), row_data.get('DepartureDateLocal6'),
            row_data.get('DepartureDateLocal7'),
            row_data.get('PaxName'), row_data.get('BookingRef')
        )

        self.db.execute_query(query, params)
        logger.info(f"UPDATED ROW FOR BOOKING {row_data.get('BookingRef', 'Unknown')}")

# Global instances
db_connection = DatabaseConnection()
flight_repo = FlightRepository(db_connection)
