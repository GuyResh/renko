# Renko Bar Generator

A Rust application that generates Renko bars from IQFeed-sourced tick data stored in SQLite databases. Renko bars are price-based charts that filter out time and focus on price movements, making them useful for technical analysis.

## Features

- Reads tick data from SQLite databases
- Generates Renko bars with configurable bar intervals
- Supports volume tracking (bid/ask volume separation)
- Handles phantom bars for large price movements
- Stores results back to SQLite with JSON-serialized volume data

## Prerequisites

- Rust (latest stable version)
- SQLite database with tick data in the following format:
  ```sql
  CREATE TABLE ticks (
    date TEXT,
    time TEXT,
    last REAL,
    last_sz INTEGER,
    bid REAL,
    ask REAL
  );
  ```

## Installation

1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd renko
   ```

2. Build the project:
   ```bash
   cargo build --release
   ```

## Usage

### Command Line Arguments

```bash
cargo run --release -- <database_path> <bar_interval> <tick_size>
```

### Parameters

- `database_path`: Path to your SQLite database file
- `bar_interval`: Number of ticks per bar (e.g., 8)
- `tick_size`: Minimum price movement for one tick (e.g., 0.25)

### Examples

```bash
# Generate 8-tick Renko bars with 0.25 tick size
cargo run --release -- "path/to/your/database.db" 8 0.25

# Generate 16-tick Renko bars with 0.5 tick size
cargo run --release -- "path/to/your/database.db" 16 0.5
```

### Default Values

If no arguments are provided, the program uses these defaults:
- Database: `D:\db\IQFeed\SQLite\NQM5\NQM5_20250514.db`
- Bar interval: 8
- Tick size: 0.25

## Output

The program creates a new table in your database with the following structure:

```sql
CREATE TABLE bars_renko_<interval> (
  date_time TEXT,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  up INTEGER,
  down INTEGER,
  tick INTEGER,
  volume INTEGER,
  volume_bid TEXT,  -- JSON array of bid volumes
  volume_ask TEXT   -- JSON array of ask volumes
);
```

## How Renko Bars Work

1. **Bar Formation**: A new bar is created when price moves by the specified bar interval
2. **Direction**: Bars are colored based on whether the close is higher (up) or lower (down) than the open
3. **Phantom Bars**: Large price movements may generate multiple bars at once
4. **Volume Tracking**: Bid and ask volumes are tracked separately and stored as JSON arrays

## Dependencies

- `chrono`: Date/time handling
- `rusqlite`: SQLite database operations
- `rust_decimal`: High-precision decimal arithmetic
- `serde_json`: JSON serialization for volume data

## Performance

The application is optimized for large datasets with:
- WAL mode for better concurrent access
- Efficient memory usage with VecDeque for volume tracking
- Progress tracking for long-running operations

## License

MIT