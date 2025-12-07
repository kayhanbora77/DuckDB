"""
Flight processing utilities for grouping and manipulating flight data.
"""
from itertools import pairwise
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from config import config
from models import FlightRow, ProcessingResult
from database import flight_repo

logger = logging.getLogger(__name__)


class FlightProcessorError(Exception):
    """Custom exception for flight processing operations."""
    pass


class FlightProcessor:
    """Handles flight data processing operations."""

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
    ) -> List[List[datetime]]:
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
        current_group = []
        #current_group = [departure_dates[0]]
       
        #for prev, curr in zip(departure_dates, departure_dates[1:]):
        for prev_date, curr_date in pairwise(departure_dates):
            current_group.append(prev_date)   
            hours_diff = FlightProcessor.calculate_hours_difference(prev_date, curr_date)

            if hours_diff <= config.HOURS_THRESHOLD:
                current_group.append(curr_date)
            else:
                groups.append(current_group)
                current_group = [curr_date]

        # Add final group
        groups.append(current_group)
        
        logger.info(f"Created {len(groups)} flight groups")
        
        return groups

    @staticmethod
    def get_insert_and_update_rows(original_row: Dict[str, Any], groups: List[List[datetime]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Create data for inserting a new row based on flight groups.

        Args:
            original_row: Original row data
            groups: List of flight groups (each group is a list of datetime objects)

        Returns:
            Dictionary with insert data or None if no processing needed
        """
        if len(groups) <= 1:
            logger.info("No processing needed - single or no groups")
            return {}, {}

        insert_data = original_row.copy()
        update_data = original_row.copy()

        group_idx = 1
        for group in groups:
            if original_row.get("BookingRef") == "27967777" :
                logger.info("EACH_GROUP: %s", group)
            if len(group) > 1:
                item_idx = 1
                for item in group:
                    target_idx = group_idx + item_idx - 1
                    source_idx = group_idx + item_idx + 1

                    insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{target_idx}'] = (
                        original_row.get(f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}')
                    )
                    insert_data[f'{config.DEPARTURE_DATE_PREFIX}{target_idx}'] = (
                        original_row.get(f'{config.DEPARTURE_DATE_PREFIX}{source_idx}')
                    )
                    # Clear the original positions
                    insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}'] = None
                    insert_data[f'{config.DEPARTURE_DATE_PREFIX}{source_idx}'] = None

                    update_data[f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}'] = None
                    update_data[f'{config.DEPARTURE_DATE_PREFIX}{source_idx}'] = None
                    item_idx += 1
            elif len(group) == 1:
                target_idx = 1
                source_idx = 2
                insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{target_idx}'] = (
                    original_row.get(f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}')
                )
                insert_data[f'{config.DEPARTURE_DATE_PREFIX}{target_idx}'] = (
                    original_row.get(f'{config.DEPARTURE_DATE_PREFIX}{source_idx}')
                )
                # Clear the original positions
                insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}'] = None
                insert_data[f'{config.DEPARTURE_DATE_PREFIX}{source_idx}'] = None

                update_data[f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}'] = None
                update_data[f'{config.DEPARTURE_DATE_PREFIX}{source_idx}'] = None

            group_idx += 1

            for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
                flight_key = f'{config.FLIGHT_NUMBER_PREFIX}{i}'
                date_key = f'{config.DEPARTURE_DATE_PREFIX}{i}'
                if insert_data.get(flight_key) == 'NULL':
                    insert_data[flight_key] = None
                if insert_data.get(date_key) == 'NULL':
                    insert_data[date_key] = None
                if update_data.get(flight_key) == 'NULL':
                    update_data[flight_key] = None
                if update_data.get(date_key) == 'NULL':
                    update_data[date_key] = None

        if original_row.get("BookingRef") == "27967777" :
            logger.info("INSERT_DATA: %s", insert_data)
            logger.info("UPDATE_DATA: %s", update_data)

        return insert_data, update_data

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
            insert_data, update_data = FlightProcessor.get_insert_and_update_rows(row_data, groups)

            logger.info(f"Insert data: {insert_data}")
            logger.info(f"Update data: {update_data}")

            if insert_data:
                # Perform database operations
                flight_repo.update_flight(update_data)
                flight_repo.insert_flight(insert_data)                

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

def has_bus_transition(row: Dict[str, Any]) -> bool:
    """
    Check if a flight number ends with three zeros.
    """
    for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
        flight_number = row.get(f'FlightNumber{i}')
        if not flight_number or flight_number == 'Unknown':
            return False
        if str(flight_number).endswith('000'):
            return True
    return False

# Global processor instance
flight_processor = FlightProcessor()

