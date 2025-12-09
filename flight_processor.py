"""
Flight processing utilities for grouping and manipulating flight data.
"""
from collections import Counter
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
            if flight_number is not None and flight_number != 'NULL':
                if str(flight_number).endswith('000'):
                    logger.info(f"DELETED FLIGHT_NUMBER and DEPARTURELOCALDATE {row.get("PaxName")}")
                    continue                
                flight_numbers.append(flight_number)
            # Add departure date if valid
            if departure_date_str is not None and departure_date_str != 'NULL':
                try:
                    departure_date = pd.to_datetime(departure_date_str)
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

        count_dates = Counter(current_date_group)
        count_flights = Counter(current_flight_group)       
        duplicate_dates = []
        duplicate_flights = []
        for (date, count_date), (flight, count_flight) in zip(count_dates.items(), count_flights.items()):
            if count_date > 1:
                duplicate_dates.append(date)
                print(f"Duplicate date: {date} → {count_date} times")
            if count_flight > 1:
                duplicate_flights.append(flight)
                print(f"Duplicate flight: {flight} → {count_flight} times")
        
        return date_groups, flight_groups, duplicate_dates,duplicate_flights

    @staticmethod
    def get_duplicate_insert(original_row: Dict[str, Any], dates: List[datetime], flights: List[str]) -> List[Dict[str, Any]]:
        duplicate_list = []
        duplicate_data = original_row.copy()

        # Clear all flight entries first
        for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
            flight_key = f'{config.FLIGHT_NUMBER_PREFIX}{i}'
            date_key = f'{config.DEPARTURE_DATE_PREFIX}{i}'
            duplicate_data[flight_key] = None
            duplicate_data[date_key] = None

        # Set the duplicate values
        for item_idx, (date_item, flight_item) in enumerate(zip(dates, flights), 1):
            duplicate_data[f'{config.FLIGHT_NUMBER_PREFIX}{item_idx}'] = flight_item
            duplicate_data[f'{config.DEPARTURE_DATE_PREFIX}{item_idx}'] = date_item

        duplicate_list.append(duplicate_data)
        return duplicate_list
                
    @staticmethod
    def get_insert_list(original_row: Dict[str, Any], date_groups: List[List[datetime]], flight_groups: List[List[datetime]]) -> List[Dict[str, Any]]:

        insert_list = []        
        group_idx = 1
        for date_group, flight_group in zip(date_groups, flight_groups):
        
            insert_data = original_row.copy() 
            for i in range(1, config.MAX_FLIGHT_ENTRIES + 1):
                flight_key = f'{config.FLIGHT_NUMBER_PREFIX}{i}'
                date_key = f'{config.DEPARTURE_DATE_PREFIX}{i}'
                insert_data[flight_key] = None
                insert_data[date_key] = None
                                    
            item_idx = 1
            for date_item, flight_item in zip(date_group, flight_group):
                insert_data[f'{config.FLIGHT_NUMBER_PREFIX}{item_idx}'] = (flight_item)
                insert_data[f'{config.DEPARTURE_DATE_PREFIX}{item_idx}'] = (date_item)
                item_idx += 1
            insert_list.append(insert_data)                
            group_idx += 1        

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
            date_groups, flight_groups, duplicate_dates, duplicate_flights = FlightProcessor.group_by_24h(departure_dates, flight_numbers)
            if len(duplicate_dates) > 0:
                insert_duplicate_list = FlightProcessor.get_duplicate_insert(row_data, duplicate_dates, duplicate_flights)    
                if insert_duplicate_list:
                    # Perform database operations                
                    for duplicate_data in insert_duplicate_list:
                        flight_repo.insert_flight(duplicate_data)                
                    flight_repo.delete_flight(duplicate_data)
            
            if len(date_groups) <= 1:
                return ProcessingResult(
                    original_row=FlightRow.from_dataframe_row(row_data),
                    groups=date_groups,
                    success=True,
                    message="Single group - no processing needed"
                )
            # Create insert and update data
            insert_list = FlightProcessor.get_insert_list(row_data, date_groups, flight_groups)
            if insert_list:
                # Perform database operations                
                for insert_data in insert_list:
                    flight_repo.insert_flight(insert_data)                
                flight_repo.delete_flight(insert_data)
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

# Global processor instance
flight_processor = FlightProcessor()

