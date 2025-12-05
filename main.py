#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flight data processing application.
Processes flight booking data and groups related flights.

Created on Wed Dec  3 16:24:46 2025
@author: kayhan
"""
import logging
import sys
from typing import NoReturn

from database import flight_repo, DatabaseError
from flight_processor import flight_processor, FlightProcessorError
from logging_config import setup_logging
from models import ProcessingResult


logger = logging.getLogger(__name__)


def process_all_flights() -> None:
    """
    Process all flight records from the database.
    Retrieves flight data and processes each row for grouping and updates.
    """
    try:
        logger.info("Starting flight data processing")

        # Retrieve all flight data
        df = flight_repo.get_all_flights()
        total_rows = len(df)
        logger.info(f"Retrieved {total_rows} flight records for processing")

        if total_rows == 0:
            logger.warning("No flight records found to process")
            return

        processed_count = 0
        success_count = 0
        error_count = 0

        # Process each row
        for index, row in df.iterrows():
            try:
                booking_ref = row.get('BookingRef', 'Unknown')
                pax_name = row.get('PaxName', 'Unknown')

                logger.info(f"Processing row {index + 1}/{total_rows}: "
                           f"BookingRef={booking_ref}, PaxName={pax_name}")

                # Process the flight row
                result = flight_processor.process_flight_row(row.to_dict())

                processed_count += 1

                if result.success:
                    success_count += 1
                    logger.info(f"Successfully processed: {result.message}")
                else:
                    error_count += 1
                    logger.error(f"Failed to process row {index + 1}: {result.message}")

                # Log progress every 10 rows
                if (index + 1) % 10 == 0:
                    logger.info(f"Progress: {index + 1}/{total_rows} rows processed")

            except Exception as e:
                error_count += 1
                logger.error(f"Unexpected error processing row {index + 1}: {e}")
                continue

        # Final summary
        logger.info("Processing complete!")
        logger.info(f"Total rows: {total_rows}")
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Errors: {error_count}")

    except DatabaseError as e:
        logger.error(f"Database error during processing: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        raise


def main() -> NoReturn:
    """
    Main entry point for the flight processing application.
    """
    # Setup logging
    setup_logging(log_level="INFO")

    logger.info("Starting Flight Processing Application")

    try:
        process_all_flights()
        logger.info("Application completed successfully")
        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        sys.exit(130)
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except FlightProcessorError as e:
        logger.error(f"Flight processing error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    
