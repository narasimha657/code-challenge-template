# app/services/weather_ingestion.py
import os
import logging
from datetime import datetime, date
from typing import Dict, List, Tuple
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db.repositories.weather_repo import (
    WeatherStationRepository, 
    WeatherRecordRepository
)
from app.schemas.weather import WeatherRecordCreate

logger = logging.getLogger(__name__)


class WeatherIngestionService:
    """Service to handle weather data ingestion from text files"""
    
    def __init__(self, db: Session):
        self.db = db
        self.station_repo = WeatherStationRepository(db)
        self.record_repo = WeatherRecordRepository(db)
        
    def ingest_weather_data(self, data_folder_path: str) -> Dict[str, any]:
        """
        Ingest weather data from text files in the specified folder
        
        Args:
            data_folder_path: Path to the wx_data folder containing weather files
            
        Returns:
            Dictionary with ingestion results and statistics
        """
        start_time = datetime.now()
        logger.info(f"Starting weather data ingestion at {start_time}")
        logger.info(f"Data folder path: {data_folder_path}")
        
        results = {
            "start_time": start_time,
            "end_time": None,
            "files_processed": 0,
            "total_records_processed": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "stations_created": 0,
            "errors": [],
            "duration_seconds": 0
        }
        
        try:
            # Validate folder exists
            if not os.path.exists(data_folder_path):
                raise FileNotFoundError(f"Data folder not found: {data_folder_path}")
            
            # Get all .txt files in the folder
            data_folder = Path(data_folder_path)
            weather_files = list(data_folder.glob("*.txt"))
            
            if not weather_files:
                logger.warning(f"No .txt files found in {data_folder_path}")
                results["errors"].append("No .txt files found in data folder")
                return results
            
            logger.info(f"Found {len(weather_files)} weather data files")
            
            # Process each file
            for file_path in weather_files:
                try:
                    file_results = self._process_weather_file(file_path)
                    results["files_processed"] += 1
                    results["total_records_processed"] += file_results["records_processed"]
                    results["records_inserted"] += file_results["records_inserted"]
                    results["records_updated"] += file_results["records_updated"]
                    results["stations_created"] += file_results["stations_created"]
                    
                    logger.info(
                        f"Processed {file_path.name}: "
                        f"{file_results['records_processed']} records, "
                        f"{file_results['records_inserted']} inserted, "
                        f"{file_results['records_updated']} updated"
                    )
                    
                except Exception as e:
                    error_msg = f"Error processing file {file_path.name}: {str(e)}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
            
            # Commit all changes
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Error during ingestion: {str(e)}")
            results["errors"].append(f"Ingestion error: {str(e)}")
            self.db.rollback()
        
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            results["end_time"] = end_time
            results["duration_seconds"] = duration
            
            logger.info(f"Weather data ingestion completed at {end_time}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Files processed: {results['files_processed']}")
            logger.info(f"Total records processed: {results['total_records_processed']}")
            logger.info(f"Records inserted: {results['records_inserted']}")
            logger.info(f"Records updated: {results['records_updated']}")
            logger.info(f"Stations created: {results['stations_created']}")
            
            if results["errors"]:
                logger.warning(f"Errors encountered: {len(results['errors'])}")
                for error in results["errors"]:
                    logger.warning(f"  - {error}")
        
        return results
    
    def _process_weather_file(self, file_path: Path) -> Dict[str, int]:
        """
        Process a single weather data file
        
        Args:
            file_path: Path to the weather data file
            
        Returns:
            Dictionary with processing statistics for this file
        """
        # Extract station ID from filename (e.g., USC00110072.txt -> USC00110072)
        station_id = file_path.stem
        
        # Determine state from station ID pattern or filename
        state = self._determine_state_from_station(station_id)
        
        # Ensure station exists
        station = self.station_repo.get_or_create_station(station_id, state)
        stations_created = 1 if station.id else 0
        
        results = {
            "records_processed": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "stations_created": stations_created
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    try:
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Parse the tab-separated values
                        parts = line.split('\t')
                        if len(parts) != 4:
                            logger.warning(
                                f"Invalid line format in {file_path.name}:{line_num}: "
                                f"Expected 4 fields, got {len(parts)}"
                            )
                            continue
                        
                        date_str, max_temp_str, min_temp_str, precip_str = parts
                        
                        # Parse date (YYYYMMDD format)
                        record_date = self._parse_date(date_str)
                        if not record_date:
                            logger.warning(
                                f"Invalid date format in {file_path.name}:{line_num}: {date_str}"
                            )
                            continue
                        
                        # Parse temperature and precipitation values
                        max_temp = self._parse_value(max_temp_str)
                        min_temp = self._parse_value(min_temp_str)
                        precipitation = self._parse_value(precip_str)
                        
                        # Create weather record
                        record_data = WeatherRecordCreate(
                            station_id=station_id,
                            date=record_date,
                            max_temperature_tenths=max_temp,
                            min_temperature_tenths=min_temp,
                            precipitation_tenths=precipitation
                        )
                        
                        # Check if record already exists
                        existing_record = self.record_repo.get_by_station_and_date(
                            station_id, record_date
                        )
                        
                        if existing_record:
                            # Update existing record
                            updated_record = self.record_repo.create_or_update_record(record_data)
                            results["records_updated"] += 1
                        else:
                            # Create new record
                            new_record = self.record_repo.create_or_update_record(record_data)
                            results["records_inserted"] += 1
                        
                        results["records_processed"] += 1
                        
                        # Commit every 1000 records to avoid large transactions
                        if results["records_processed"] % 1000 == 0:
                            self.db.commit()
                            logger.debug(f"Committed {results['records_processed']} records from {file_path.name}")
                        
                    except Exception as e:
                        logger.error(
                            f"Error processing line {line_num} in {file_path.name}: {str(e)}"
                        )
                        continue
        
        except Exception as e:
            logger.error(f"Error reading file {file_path.name}: {str(e)}")
            raise
        
        return results
    
    def _parse_date(self, date_str: str) -> date:
        """Parse date string in YYYYMMDD format"""
        try:
            return datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            return None
    
    def _parse_value(self, value_str: str) -> int:
        """Parse numeric value, returning None for missing values (-9999)"""
        try:
            value = int(value_str.strip())
            return None if value == -9999 else value
        except (ValueError, AttributeError):
            return None
    
    def _determine_state_from_station(self, station_id: str) -> str:
        """
        Determine state from station ID pattern
        This is a simplified approach - in reality you might have a lookup table
        """
        # This is a placeholder - you might want to implement actual logic
        # based on station ID patterns or maintain a mapping
        if station_id.startswith("USC001"):
            return "Nebraska"
        elif station_id.startswith("USC002"):
            return "Iowa"
        elif station_id.startswith("USC003"):
            return "Illinois"
        elif station_id.startswith("USC004"):
            return "Indiana"
        elif station_id.startswith("USC005"):
            return "Ohio"
        else:
            return "Unknown"
    
    def get_ingestion_status(self) -> Dict[str, any]:
        """Get current ingestion status and database statistics"""
        try:
            # Get database statistics
            stations_count = self.db.execute(
                text("SELECT COUNT(*) FROM weather_stations")
            ).scalar()
            
            records_count = self.db.execute(
                text("SELECT COUNT(*) FROM weather_records")
            ).scalar()
            
            date_range = self.db.execute(
                text("SELECT MIN(date), MAX(date) FROM weather_records")
            ).fetchone()
            
            return {
                "total_stations": stations_count,
                "total_records": records_count,
                "date_range": {
                    "start_date": str(date_range[0]) if date_range[0] else None,
                    "end_date": str(date_range[1]) if date_range[1] else None
                },
                "database_status": "connected"
            }
        except Exception as e:
            logger.error(f"Error getting ingestion status: {str(e)}")
            return {
                "error": str(e),
                "database_status": "error"
            }