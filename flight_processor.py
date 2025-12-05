"""
Flight processing utilities for grouping and manipulating flight data.
"""
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from config import config
from models import FlightRow, FlightEntry, FlightGroup, ProcessingResult
from database import flight_repo

logger = logging.getLogger(__name__)


class FlightProcessorError(Exception):
    """Custom exception for flight processing operations."""
    pass


class FlightProcessor:
    """Handles flight data processing operations."""

    @staticmethod
    def has_bus_transition(row: Dict[str, Any]) -> bool:
        """
        Check if any flight number in the row ends with three zeros.

        Args:
            row: Database row containing flight data

        Returns:
            True if any flight number ends with '000', False otherwise
        """
        for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
            flight_number = row.get(f"{config.FLIGHT_NUMBER_PREFIX}{i}")
            if flight_number and str(flight_number).endswith('000'):
                return True
        return False

    @staticmethod
    def calculate_hours_difference(first_date: datetime, next_date: datetime) -> float:
        """Calculate the difference in hours between two dates."""
        if not first_date or not next_date:
            return 0.0
        diff = next_date - first_date
        return diff.total_seconds() / 3600

    @staticmethod
    def extract_flight_data_from_row(row: Dict[str, Any]) -> Tuple[List[datetime], List[str]]:
        """
        Extract departure dates and flight numbers from a database row.

        Returns:
            Tuple of (departure_dates, flight_numbers) lists
        """
        departure_dates = []
        flight_numbers = []

        for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
            flight_num_key = f"{config.FLIGHT_NUMBER_PREFIX}{i}"
            date_key = f"{config.DEPARTURE_DATE_PREFIX}{i}"

            flight_number = row.get(flight_num_key)
            departure_date_str = row.get(date_key)

            # Add flight number if valid
            if pd.notnull(flight_number) and flight_number != 'NULL':
                flight_numbers.append(flight_number)

            # Add departure date if valid
            if pd.notnull(departure_date_str) and departure_date_str != 'NULL':
                try:
                    departure_date = pd.to_datetime(
                        departure_date_str,
                        format="%Y-%m-%d %H:%M:%S.%f"
                    )
                    departure_dates.append(departure_date)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse date {departure_date_str}: {e}")

        return departure_dates, flight_numbers

    @staticmethod
    def group_flights_by_departure_date(
        departure_dates: List[datetime],
        flight_numbers: List[str]
    ) -> List[FlightGroup]:
        """
        Group flights based on departure date proximity (within 24 hours).

        Args:
            departure_dates: List of departure dates
            flight_numbers: List of corresponding flight numbers

        Returns:
            List of FlightGroup objects
        """
        if len(departure_dates) != len(flight_numbers):
            raise FlightProcessorError(
                f"Mismatched lengths: {len(departure_dates)} dates, {len(flight_numbers)} flights"
            )

        if not departure_dates:
            return []

        groups = []
        current_group = FlightGroup()

        # Start with first entry
        first_entry = FlightEntry(flight_numbers[0], departure_dates[0])
        current_group.add_entry(first_entry)

        # Process remaining entries
        for i in range(1, len(departure_dates)):
            current_date = departure_dates[i]
            prev_date = departure_dates[i - 1]

            hours_diff = FlightProcessor.calculate_hours_difference(prev_date, current_date)

            if hours_diff < config.HOURS_THRESHOLD:
                # Add to current group
                entry = FlightEntry(flight_numbers[i], current_date)
                current_group.add_entry(entry)
            else:
                # Start new group
                groups.append(current_group)
                current_group = FlightGroup()
                entry = FlightEntry(flight_numbers[i], current_date)
                current_group.add_entry(entry)

        # Add final group
        if not current_group.is_empty():
            groups.append(current_group)

        logger.debug(f"Created {len(groups)} flight groups")
        return groups

    @staticmethod
    def create_insert_row_data(original_row: Dict[str, Any], groups: List[FlightGroup]) -> Optional[Dict[str, Any]]:
        """
        Create data for inserting a new row based on flight groups.

        Args:
            original_row: Original row data
            groups: List of flight groups

        Returns:
            Dictionary with insert data or None if no processing needed
        """
        if len(groups) <= 1:
            logger.info("No processing needed - single or no groups")
            return None

        insert_data = original_row.copy()
        group_idx = 1

        for group in groups:
            if group_idx >= config.MAX_FLIGHT_ENTRIES:
                break

            group_entries = group.entries
            for entry_idx, entry in enumerate(group_entries):
                if group_idx + entry_idx >= config.MAX_FLIGHT_ENTRIES:
                    break

                target_idx = group_idx + entry_idx
                insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{target_idx}'] = (
                    original_row.get(f'{config.FLIGHT_NUMBER_PREFIX}{group_idx + entry_idx + 1}')
                )
                insert_data[f'{config.DEPARTURE_DATE_PREFIX}{target_idx}'] = (
                    original_row.get(f'{config.DEPARTURE_DATE_PREFIX}{group_idx + entry_idx + 1}')
                )

                # Clear the original positions
                insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{group_idx + entry_idx + 1}'] = None
                insert_data[f'{config.DEPARTURE_DATE_PREFIX}{group_idx + entry_idx + 1}'] = None

            group_idx += 1

        return insert_data

    @staticmethod
    def create_update_row_data(original_row: Dict[str, Any], groups: List[FlightGroup]) -> Dict[str, Any]:
        """
        Create data for updating the original row.

        Args:
            original_row: Original row data
            groups: List of flight groups

        Returns:
            Dictionary with update data
        """
        update_data = original_row.copy()

        # Clear flight data positions that were moved to insert
        for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
            flight_key = f'{config.FLIGHT_NUMBER_PREFIX}{i}'
            date_key = f'{config.DEPARTURE_DATE_PREFIX}{i}'

            if update_data.get(flight_key) == 'NULL':
                update_data[flight_key] = None
            if update_data.get(date_key) == 'NULL':
                update_data[date_key] = None

        return update_data

    @staticmethod
    def process_flight_row(row_data: Dict[str, Any]) -> ProcessingResult:
        """
        Process a single flight row - main entry point for flight processing.

        Args:
            row_data: Row data from database

        Returns:
            ProcessingResult with the outcome
        """
        try:
            # Extract flight data
            departure_dates, flight_numbers = FlightProcessor.extract_flight_data_from_row(row_data)

            if not departure_dates or not flight_numbers:
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=[],
                    success=True,
                    message="No flight data to process"
                )

            # Group flights
            groups = FlightProcessor.group_flights_by_departure_date(departure_dates, flight_numbers)

            if len(groups) <= 1:
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=groups,
                    success=True,
                    message="Single group - no processing needed"
                )

            # Create insert and update data
            insert_data = FlightProcessor.create_insert_row_data(row_data, groups)
            update_data = FlightProcessor.create_update_row_data(row_data, groups)

            if insert_data:
                # Perform database operations
                flight_repo.insert_flight(insert_data)
                flight_repo.update_flight(update_data)

                logger.info(f"Processed row for booking {row_data.get('BookingRef', 'Unknown')}")

                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=groups,
                    success=True,
                    message="Row processed and database updated"
                )
            else:
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=groups,
                    success=True,
                    message="No database changes needed"
                )

        except Exception as e:
            logger.error(f"Failed to process flight row: {e}")
            return ProcessingResult(
                original_row=FlightRow.from_dataframe_row(row_data),
                groups=[],
                success=False,
                message=f"Processing failed: {str(e)}"
            )

def has_three_zeros_at_the_end(flight_number: str) -> bool:
    """
    Check if a flight number ends with three zeros.
    """
    if not flight_number or flight_number == 'Unknown':
        return False
    return str(flight_number).endswith('000')

def has_three_zeros_at_the_end_in_flight_numbers(row: Dict[str, Any]) -> bool:
    """
    Check if a row has a flight number that ends with three zeros.
    """
    for i in range(1, 8):
        flight_number = row.get(f'FlightNumber{i}', 'Unknown')
        if has_three_zeros_at_the_end(flight_number):
            return True
    return False
# Global processor instance
flight_processor = FlightProcessor()

