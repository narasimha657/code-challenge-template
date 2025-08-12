# Weather Data API

A FastAPI-based REST API for ingesting, storing, and analyzing weather data from text files. This project processes historical weather data (1985-2014) from weather stations across Nebraska, Iowa, Illinois, Indiana, and Ohio.

## 🌟 Features

- **Data Ingestion**: Automatically parse and import weather data from text files
- **Data Storage**: Store weather records in PostgreSQL with proper data modeling
- **Statistics Calculation**: Calculate yearly statistics (avg temperatures, total precipitation)
- **REST API**: Query weather data and statistics with filtering and pagination
- **Data Validation**: Handle missing values and prevent duplicate records
- **Interactive Documentation**: Swagger/OpenAPI documentation for easy API exploration

## 📋 What This App Does

### 1. Data Ingestion
- Reads weather data files from the `wx_data` folder
- Each file contains daily weather records for a specific weather station
- Parses tab-separated values: date, max temperature, min temperature, precipitation
- Handles missing values (indicated by -9999)
- Prevents duplicate records on re-ingestion

### 2. Data Processing
- Stores raw data in tenths of units (as provided in source files)
- Converts and stores values in standard units (Celsius, millimeters)
- Creates weather station records automatically

### 3. Statistics Calculation
- Calculates yearly statistics for each weather station:
  - Average maximum temperature (°C)
  - Average minimum temperature (°C) 
  - Total accumulated precipitation (cm)
- Ignores missing values in calculations
- Stores NULL for statistics that cannot be calculated

### 4. API Access
- RESTful endpoints to query weather data and statistics
- Filtering by station ID, date ranges, and years
- Pagination support for large datasets
- JSON responses with proper data formatting

## 🛠️ Technology Stack

- **Backend**: FastAPI (Python)
- **Database**: PostgreSQL
- **ORM**: SQLAlchemy
- **Migration**: Alembic
- **Validation**: Pydantic
- **Documentation**: Swagger/OpenAPI

## 📦 Installation & Setup

### Prerequisites
- Python 3.8+
- PostgreSQL database
- Git

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd weather-api
```

### 2. Set Up Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Database Configuration

#### Create PostgreSQL Database
```sql
CREATE DATABASE weather_db;
CREATE USER postgres WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO postgres;
```

#### Configure Environment Variables
Create a `.env` file in the project root:
```env
DEBUG=True
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5437
```
> **Note**: Replace `5437` with your actual PostgreSQL port number

### 5. Get Weather Data
```bash
# Clone the data repository
git clone https://github.com/corteva/code-challenge-template
# Copy the wx_data folder to your project root
cp -r code-challenge-template/wx_data ./
```

Your project structure should look like:
```
weather-api/
├── wx_data/           # Weather data files (*.txt)
├── app/
├── requirements.txt
├── .env
└── README.md
```

### 6. Test Database Connection
```bash
python db_test.py
```

## 🚀 Running the Application

### Start the API Server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

### Verify the API is Running
Visit: `http://localhost:8000` 
You should see: `{"message": "Weather API is running", "version": "1.0.0"}`

## 📖 API Documentation

### Interactive Documentation (Swagger UI)
Visit: **http://localhost:8000/api/v1/docs#/**

This provides a complete interactive interface to:
- Explore all available endpoints
- Test API calls directly from the browser
- View request/response schemas
- Understand parameter requirements

### Alternative Documentation
- ReDoc format: `http://localhost:8000/api/v1/redoc`

## 🔄 Getting Started - Step by Step

### Step 1: Ingest Weather Data
**Endpoint**: `POST /api/v1/weather/ingestion/ingest`

```bash
curl -X POST "http://localhost:8000/api/v1/weather/ingestion/ingest"
```

This will:
- Process all `.txt` files in the `wx_data` folder
- Create weather station records
- Import daily weather records
- Run in the background

**Check ingestion status**:
```bash
curl -X GET "http://localhost:8000/api/v1/weather/ingestion/status"
```

### Step 2: Calculate Statistics
**Endpoint**: `POST /api/v1/weather/statistics/calculate`

```bash
curl -X POST "http://localhost:8000/api/v1/weather/statistics/calculate"
```

This will:
- Calculate yearly statistics for all stations
- Store results in the database
- Run in the background

**Check calculation status**:
```bash
curl -X GET "http://localhost:8000/api/v1/weather/statistics/status"
```

### Step 3: Query the Data

#### Get Weather Records
```bash
# Get all weather records (paginated)
curl -X GET "http://localhost:8000/api/v1/weather"

# Filter by station
curl -X GET "http://localhost:8000/api/v1/weather?station_id=USC00110072"

# Filter by date range
curl -X GET "http://localhost:8000/api/v1/weather?start_date=2010-01-01&end_date=2010-12-31"
```

#### Get Weather Statistics
```bash
# Get all statistics
curl -X GET "http://localhost:8000/api/v1/weather/stats"

# Filter by station and year
curl -X GET "http://localhost:8000/api/v1/weather/stats?station_id=USC00110072&year=2010"

# Filter by year range
curl -X GET "http://localhost:8000/api/v1/weather/stats?start_year=2010&end_year=2014"
```

## 📊 API Endpoints Overview

### Weather Data Ingestion
- `POST /api/v1/weather/ingestion/ingest` - Start background ingestion
- `POST /api/v1/weather/ingestion/ingest/sync` - Synchronous ingestion
- `GET /api/v1/weather/ingestion/status` - Check ingestion status
- `GET /api/v1/weather/ingestion/config` - View ingestion configuration

### Weather Statistics
- `POST /api/v1/weather/statistics/calculate` - Calculate all statistics
- `POST /api/v1/weather/statistics/calculate/sync` - Synchronous calculation
- `POST /api/v1/weather/statistics/calculate/station/{station_id}` - Calculate for specific station
- `GET /api/v1/weather/statistics/status` - Check calculation status

### Data Retrieval
- `GET /api/v1/weather` - Get weather records with filtering/pagination
- `GET /api/v1/weather/stats` - Get statistics with filtering/pagination

## 🔍 Query Parameters

### Weather Data Filtering
- `station_id` - Filter by weather station ID
- `start_date` - Start date (YYYY-MM-DD format)
- `end_date` - End date (YYYY-MM-DD format)
- `page` - Page number (default: 1)
- `size` - Records per page (default: 50, max: 1000)

### Statistics Filtering
- `station_id` - Filter by weather station ID
- `year` - Specific year
- `start_year` - Start year (inclusive)
- `end_year` - End year (inclusive)
- `page` - Page number (default: 1)
- `size` - Records per page (default: 50, max: 1000)

## 📁 Data Format

### Input Data Format (wx_data files)
Each line contains tab-separated values:
```
YYYYMMDD    max_temp_tenths    min_temp_tenths    precipitation_tenths
19850101    -128               -183               0
```
- Date in YYYYMMDD format
- Temperatures in tenths of degrees Celsius
- Precipitation in tenths of millimeters
- Missing values indicated by -9999

### API Response Format
```json
{
  "total": 100,
  "page": 1,
  "size": 50,
  "data": [
    {
      "id": 1,
      "station_id": "USC00110072",
      "date": "1985-01-01",
      "max_temperature_celsius": -12.8,
      "min_temperature_celsius": -18.3,
      "precipitation_mm": 0.0
    }
  ]
}
```

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_weather_statistics.py -v

# Run with coverage
pytest --cov=app tests/
```

### Test Database Connection
```bash
python db_test.py
```

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check PostgreSQL is running
   - Verify database credentials in `.env`
   - Ensure database and user exist
   - Check the port number (common ports: 5432, 5433, 5437)

2. **Import Errors**
   - Activate virtual environment: `source venv/bin/activate`
   - Install requirements: `pip install -r requirements.txt`

3. **No wx_data folder**
   - Clone data repository: `git clone https://github.com/corteva/code-challenge-template`
   - Copy wx_data folder to project root

4. **Port Already in Use**
   - Change port: `uvicorn app.main:app --reload --port 8001`
   - Or kill existing process using port 8000

5. **Permission Denied on Database**
   - Ensure PostgreSQL user has proper permissions
   - Grant privileges: `GRANT ALL PRIVILEGES ON DATABASE weather_db TO postgres;`

### Logging
The application provides detailed logging. Check console output for:
- Ingestion progress and statistics
- Error messages and stack traces
- API request/response information

## 🏗️ Project Structure

```
weather-api/
├── app/
│   ├── api/
│   │   └── routes/           # API route definitions
│   ├── core/
│   │   └── config.py         # Configuration settings
│   ├── db/
│   │   ├── base.py          # Database base class
│   │   ├── session.py       # Database session management
│   │   └── repositories/    # Data access layer
│   ├── models/
│   │   └── weather.py       # SQLAlchemy models
│   ├── schemas/
│   │   └── weather.py       # Pydantic schemas
│   ├── services/
│   │   ├── weather_ingestion.py    # Data ingestion logic
│   │   └── weather_statistics.py   # Statistics calculation
│   └── main.py              # FastAPI application entry point
├── tests/                   # Test files
├── wx_data/                 # Weather data files
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables
└── README.md               # This file
```

## 📝 Data Schema

### Weather Stations
- Station ID (unique identifier)
- Optional name and state information

### Weather Records
- Station ID and date (unique constraint)
- Maximum/minimum temperatures (stored in both tenths and Celsius)
- Precipitation (stored in both tenths and millimeters)
- Creation and update timestamps

### Weather Statistics
- Station ID and year (unique constraint)
- Calculated averages and totals
- Metadata about calculation (record counts)
- Creation and update timestamps

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request