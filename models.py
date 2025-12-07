"""
Data models and type definitions for the flight booking application.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum


class JourneyType(Enum):
    """Enumeration for journey types."""
    ONE_WAY = "OneWay"
    RETURN = "Return"    


# @dataclass
# class FlightEntry:
#     """Represents a single flight entry with flight number and departure date."""
#     flight_number: Optional[str]
#     departure_date: Optional[datetime]

#     @classmethod
#     def from_row(cls, row: Dict[str, Any], index: int) -> 'FlightEntry':
#         """Create a FlightEntry from a database row at the specified index."""
#         flight_key = f"FlightNumber{index}"
#         date_key = f"DepartureDateLocal{index}"

#         flight_number = row.get(flight_key)
#         departure_date_str = row.get(date_key)

#         # Parse departure date if present
#         departure_date = None
#         if departure_date_str and departure_date_str != 'NULL':
#             try:
#                 departure_date = datetime.strptime(
#                     str(departure_date_str),
#                     "%Y-%m-%d %H:%M:%S.%f"
#                 )
#             except ValueError:
#                 # If parsing fails, keep as None
#                 pass

#         return cls(
#             flight_number=flight_number if flight_number != 'NULL' else None,
#             departure_date=departure_date
#         )


# @dataclass
# class FlightGroup:
#     """Represents a group of related flight entries."""
#     entries: List[FlightEntry] = field(default_factory=list)

#     def add_entry(self, entry: FlightEntry) -> None:
#         """Add a flight entry to this group."""
#         self.entries.append(entry)

#     def is_empty(self) -> bool:
#         """Check if the group has no entries."""
#         return len(self.entries) == 0

#     def get_departure_dates(self) -> List[datetime]:
#         """Get all departure dates in this group."""
#         return [entry.departure_date for entry in self.entries if entry.departure_date]

#     def get_flight_numbers(self) -> List[str]:
#         """Get all flight numbers in this group."""
#         return [entry.flight_number for entry in self.entries if entry.flight_number]


@dataclass
class FlightRow:
    """Represents a complete flight booking row with all flight entries."""
    pax_name: str
    booking_ref: str
    client_code: Optional[str] = None
    airline: Optional[str] = None
    journey_type: Optional[str] = None
    e_ticket_no: Optional[str] = None
    #flight_entries: List[FlightEntry] = field(default_factory=list)
    airports: List[Optional[str]] = field(default_factory=list)

    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any]) -> 'FlightRow':
        """Create a FlightRow from a pandas DataFrame row."""
        flight_entries = []
        airports = []

        # Extract flight entries (1-7)
        #for i in range(1, 8):
        #    flight_entries.append(FlightEntry.from_row(row, i))

        # Extract airports (1-8)
        for i in range(1, 9):
            airport_key = f"Airport{i}"
            airports.append(row.get(airport_key))

        return cls(
            pax_name=row.get('PaxName', ''),
            booking_ref=row.get('BookingRef', ''),
            client_code=row.get('ClientCode'),
            airline=row.get('Airline'),
            journey_type=row.get('JourneyType'),
            e_ticket_no=row.get('ETicketNo'),
            #flight_entries=flight_entries,
            airports=airports
        )

    # def to_dict(self) -> Dict[str, Any]:
    #     """Convert the FlightRow to a dictionary for database operations."""
    #     result = {
    #         'PaxName': self.pax_name,
    #         'BookingRef': self.booking_ref,
    #         'ClientCode': self.client_code,
    #         'Airline': self.airline,
    #         'JourneyType': self.journey_type,
    #         'ETicketNo': self.e_ticket_no,
    #     }

    #     # Add flight entries
    #     for i, entry in enumerate(self.flight_entries, 1):
    #         result[f'FlightNumber{i}'] = entry.flight_number
    #         result[f'DepartureDateLocal{i}'] = (
    #             entry.departure_date.isoformat() if entry.departure_date else None
    #         )

    #     # Add airports
    #     for i, airport in enumerate(self.airports, 1):
    #         result[f'Airport{i}'] = airport

    #     return result

    # def get_valid_flight_entries(self) -> List[FlightEntry]:
    #     """Get only flight entries that have valid data."""
    #     return [
    #         entry for entry in self.flight_entries
    #         if entry.flight_number and entry.flight_number != 'NULL'
    #     ]


@dataclass
class ProcessingResult:
    """Result of processing a flight row."""
    original_row: FlightRow
    groups: List[List[datetime]]
    success: bool
    message: Optional[str] = None

    def __post_init__(self):
        """Validate the processing result."""
        if not isinstance(self.groups, list):
            self.groups = []

