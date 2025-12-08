"""
Data models and type definitions for the flight booking application.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class FlightRow:
    """Represents a complete flight booking row with all flight entries."""
    pax_name: str
    booking_ref: str
    client_code: Optional[str] = None
    airline: Optional[str] = None
    journey_type: Optional[str] = None
    e_ticket_no: Optional[str] = None    
    airports: List[Optional[str]] = field(default_factory=list)

    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any]) -> 'FlightRow':
        """Create a FlightRow from a pandas DataFrame row."""        
        airports = []

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
            airports=airports
        )

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

