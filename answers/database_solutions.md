# Database Solutions

Quick answers to common database challenges in our Weather API project.

## Parallel Write Operations

**Question**: How do we handle multiple threads writing to the database safely?

**Answer**: SQLAlchemy's Engine is already built for this! It manages a connection pool, so each thread gets its own database connection automatically.

```python
from sqlalchemy.orm import sessionmaker
from threading import Thread

# Each thread gets its own session and connection
def process_weather_data(data_batch):
    db = SessionLocal()  # New session for this thread
    try:
        db.bulk_insert_mappings(WeatherRecord, data_batch)
        db.commit()
    finally:
        db.close()  # Returns connection to pool

# Safe to run in parallel
threads = []
for batch in data_batches:
    thread = Thread(target=process_weather_data, args=(batch,))
    threads.append(thread)
    thread.start()
```

**Why it works**: The connection pool handles everything - no conflicts, no data corruption, just clean parallel processing.

---

## Only Committed Data in APIs

**Question**: How do we make sure our API never returns uncommitted/dirty data?

**Answer**: Set the database isolation level to `READ COMMITTED`. This ensures we only see data that's been properly committed.

```python
# Option 1: Set it globally when creating the engine
engine = create_engine(
    "postgresql://user:pass@localhost/db",
    isolation_level="READ_COMMITTED"
)

# Option 2: Set it per API call
@router.get("/weather")
async def get_weather_data(db: Session = Depends(get_db)):
    db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    
    # Now all queries only see committed data
    weather_data = db.query(WeatherRecord).all()
    return weather_data
```

**What this prevents**: 
- Dirty reads (seeing uncommitted changes from other transactions)
- Getting inconsistent data during ongoing database operations

---

## Fewer Database Round Trips

**Question**: How do we insert thousands of records efficiently without killing performance?

**Answer**: Use bulk operations instead of inserting one record at a time.

```python
for record in weather_data:
    db.add(WeatherRecord(**record))
    db.commit()  # 1000 records = 1000 database calls

# Better approach
def bulk_insert_weather_data(db: Session, weather_data):
    # Single database call for all records
    db.bulk_insert_mappings(WeatherRecord, weather_data)
    db.commit()  # 1000 records = 1 database call
    
# Even better for PostgreSQL: Handle duplicates efficiently
from sqlalchemy.dialects.postgresql import insert

def upsert_weather_data(db: Session, weather_data):
    stmt = insert(WeatherRecord).values(weather_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['station_id', 'date'],
        set_=dict(
            max_temperature_celsius=stmt.excluded.max_temperature_celsius,
            min_temperature_celsius=stmt.excluded.min_temperature_celsius,
            precipitation_mm=stmt.excluded.precipitation_mm
        )
    )
    db.execute(stmt)
    db.commit()
```

**Performance Impact**:
- Single inserts: 1000 records = 1000 database calls ≈ 10+ seconds
- Bulk inserts: 1000 records = 1 database call ≈ 0.1 seconds

**Pro tip**: For really large datasets, batch them into chunks of 1000-5000 records to balance memory usage with performance.

---

These solutions make our weather API both faster and more reliable when dealing with large amounts of data and concurrent users.