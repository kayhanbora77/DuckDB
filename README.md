# Flight Data Processing Application v-1

A Python application for processing and grouping flight booking data using DuckDB.

## Overview

This application processes flight booking records from a DuckDB database, groups related flights based on departure date proximity (within 24 hours), and updates the database accordingly.

## Features

- **Database Integration**: Uses DuckDB for efficient data storage and querying
- **Flight Grouping**: Automatically groups flights based on departure date proximity
- **Data Processing**: Processes flight records and creates insert/update operations
- **Error Handling**: Comprehensive error handling and logging
- **Type Safety**: Full type hints throughout the codebase
- **Modular Design**: Clean separation of concerns

## Project Structure

```
├── config.py              # Configuration management
├── database.py            # Database connection and operations
├── models.py              # Data models and type definitions
├── flight_processor.py    # Business logic for flight processing
├── logging_config.py      # Logging configuration
├── main.py                # Main application entry point
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

### Key Modules

- **`config.py`**: Centralized configuration including database paths and constants
- **`database.py`**: Database connection management and CRUD operations
- **`models.py`**: Data structures for flights, groups, and processing results
- **`flight_processor.py`**: Core business logic for processing flight data
- **`main.py`**: Application entry point with proper error handling

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd DuckDBUpdate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the application:
```bash
python main.py
```

The application will:
1. Connect to the DuckDB database
2. Retrieve all flight records
3. Process each record to group related flights
4. Update the database with grouped flight data
5. Provide detailed logging of the process

## Configuration

Database settings can be modified in `config.py`:

```python
class Config:
    DATABASE_DIR = Path.home() / "my_database"
    DATABASE_NAME = "my_db.duckdb"
    HOURS_THRESHOLD = 24  # Hours for flight grouping
    MAX_FLIGHT_ENTRIES = 7  # Maximum flight entries per row
```

## Logging

The application uses Python's logging module with configurable log levels. Logs are output to console by default. To enable file logging, modify the logging setup in `main.py`.

## Error Handling

The application includes comprehensive error handling for:
- Database connection issues
- Data parsing errors
- Processing failures
- Unexpected exceptions

All errors are logged with appropriate severity levels.

## Data Flow

1. **Data Retrieval**: Fetch flight records from the main table
2. **Flight Extraction**: Extract departure dates and flight numbers from each row
3. **Grouping Logic**: Group flights within 24-hour windows
4. **Database Updates**: Create insert records for grouped flights and update original records
## Dependencies

- `duckdb`: High-performance analytical database
- `pandas`: Data manipulation and analysis

## Development

The codebase follows these principles:
- **Type Hints**: Full type annotations for better IDE support
- **Error Handling**: Comprehensive exception handling
- **Logging**: Proper logging instead of print statements
- **Modularity**: Separation of concerns across modules
- **Documentation**: Docstrings and comments throughout

## Testing

To run the application in a test environment, ensure the DuckDB database file exists at the configured path.

