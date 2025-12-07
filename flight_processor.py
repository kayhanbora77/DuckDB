"""
Flight processing utilities for grouping and manipulating flight data.
"""
from itertools import pairwise
import logging
from datetime import datetime
from re import A
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
    def extract_flight_data_from_row(row: Dict[str, Any]) -> Tuple[List[datetime], List[str]]:

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
    def group_by_24h(datetimes, flight_numbers):
        # Make sure the list is sorted
        datetimes = sorted(datetimes)

        flight_groups = []
        current_flight_group = [flight_numbers[0]]
        date_groups = []
        current_date_group = [datetimes[0]]

        for i in range(1, len(datetimes)):
            diff_hours = (datetimes[i] - datetimes[i-1]).total_seconds() / 3600
            
            if diff_hours <= 24:
                # Same group
                current_date_group.append(datetimes[i])
                current_flight_group.append(flight_numbers[i])
            else:
                # Start new group
                date_groups.append(current_date_group)
                flight_groups.append(current_flight_group)
                
                current_date_group = [datetimes[i]]
                current_flight_group = [flight_numbers[i]]
        # Append last group
        date_groups.append(current_date_group)
        flight_groups.append(current_flight_group)
        # for grp_date in date_groups:
        #     logger.info(f"DATE_GROUP==> {grp_date}")
        # for grp_flight in flight_groups:
        #     logger.info(f"FLIGHT_GROUP==>{grp_flight}")    


        return date_groups, flight_groups

    @staticmethod
    def get_insert_list(original_row: Dict[str, Any], date_groups: List[List[datetime]], flight_groups: List[List[datetime]]) -> List[Dict[str, Any]]:

        insert_list = []        
        group_idx = 1
        for date_group, flight_group in zip(date_groups, flight_groups):
            # logger.info("FLIGHT_NUMBER_GRP==>: %s", flight_group)
            # logger.info("DATE_GRP==>: %s", date_group)

            insert_data = original_row.copy() 
            for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
                flight_key = f'{config.FLIGHT_NUMBER_PREFIX}{i}'
                date_key = f'{config.DEPARTURE_DATE_PREFIX}{i}'
                insert_data[flight_key] = None
                insert_data[date_key] = None
                # if insert_data.get(flight_key) == 'NULL':
                #     insert_data[flight_key] = None
                # if insert_data.get(date_key) == 'NULL':
                #     insert_data[date_key] = None
                                    
            item_idx = 1
            for date_item, flight_item in zip(date_group, flight_group):
                # logger.info("FLIGHT_NUMBER==>: %s", flight_item)
                # logger.info("DEPARTURE_DATE==>: %s", date_item)
                insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{item_idx}'] = (flight_item)
                insert_data[f'{config.DEPARTURE_DATE_PREFIX}{item_idx}'] = (date_item)
                item_idx += 1
                # target_idx = group_idx + item_idx - 1#1
                # source_idx = group_idx + item_idx + 1#3

                # insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{target_idx}'] = (
                #     original_row.get(f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}')
                # )
                # insert_data[f'{config.DEPARTURE_DATE_PREFIX}{target_idx}'] = (
                #     original_row.get(f'{config.DEPARTURE_DATE_PREFIX}{source_idx}')
                # )
                # # Clear the original positions
                # insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{source_idx}'] = None
                # insert_data[f'{config.DEPARTURE_DATE_PREFIX}{source_idx}'] = None
                # item_idx += 1
            insert_list.append(insert_data)
                
            group_idx += 1
        
        # logger.info("INSERT_DATA_LIST==>: %s", insert_list)
        return insert_list

    @staticmethod
    def process_flight_row(row_data: Dict[str, Any]) -> ProcessingResult:

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
            #groups = FlightProcessor.group_flights_by_departure_date(departure_dates, flight_numbers)
            date_groups, flight_groups = FlightProcessor.group_by_24h(departure_dates, flight_numbers)
            if len(date_groups) <= 1:
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=date_groups,
                    success=True,
                    message="Single group - no processing needed"
                )

            # Create insert and update data
            insert_list = FlightProcessor.get_insert_list(row_data, date_groups, flight_groups)

            #logger.info(f"Insert data: {insert_list}")            

            if insert_list:
                # Perform database operations
                #flight_repo.update_flight(update_data)
                for insert_data in insert_list:
                    flight_repo.insert_flight(insert_data)                

                logger.info(f"Processed row for booking {row_data.get('BookingRef', 'Unknown')}")

                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=date_groups,
                    success=True,
                    message="Row processed and database updated"
                )
            else:
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=date_groups,
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

