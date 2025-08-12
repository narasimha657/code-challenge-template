# app/services/weather_statistics.py
import logging
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text, extract

from app.db.repositories.weather_repo import (
    WeatherRecordRepository,
    WeatherStatisticsRepository,
    WeatherStationRepository
)
from app.schemas.weather import WeatherStatisticsCreate

logger = logging.getLogger(__name__)


class WeatherStatisticsService:
    """Service to calculate and store weather statistics"""
    
    def __init__(self, db: Session):
        self.db = db
        self.record_repo = WeatherRecordRepository(db)
        self.stats_repo = WeatherStatisticsRepository(db)
        self.station_repo = WeatherStationRepository(db)
    
    def calculate_all_statistics(self) -> Dict[str, any]:
        """
        Calculate statistics for all stations and all years in the database
        
        Returns:
            Dictionary with calculation results and statistics
        """
        start_time = datetime.now()
        logger.info(f"Starting weather statistics calculation at {start_time}")
        
        results = {
            "start_time": start_time,
            "end_time": None,
            "stations_processed": 0,
            "years_processed": 0,
            "statistics_created": 0,
            "statistics_updated": 0,
            "errors": [],
            "duration_seconds": 0
        }
        
        try:
            # Get all unique station-year combinations from weather records
            station_years = self._get_all_station_years()
            logger.info(f"Found {len(station_years)} station-year combinations to process")
            
            processed_stations = set()
            
            # Process each station-year combination
            for station_id, year in station_years:
                try:
                    # Calculate statistics for this station-year
                    stats_result = self._calculate_station_year_statistics(station_id, year)
                    
                    if stats_result["created"]:
                        results["statistics_created"] += 1
                    else:
                        results["statistics_updated"] += 1
                    
                    results["years_processed"] += 1
                    processed_stations.add(station_id)
                    
                    # Log progress every 100 records
                    if results["years_processed"] % 100 == 0:
                        logger.info(f"Processed {results['years_processed']} station-year combinations")
                        self.db.commit()  # Commit periodically
                    
                except Exception as e:
                    error_msg = f"Error calculating statistics for station {station_id}, year {year}: {str(e)}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
            
            results["stations_processed"] = len(processed_stations)
            
            # Final commit
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Error during statistics calculation: {str(e)}")
            results["errors"].append(f"Statistics calculation error: {str(e)}")
            self.db.rollback()
        
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            results["end_time"] = end_time
            results["duration_seconds"] = duration
            
            logger.info(f"Weather statistics calculation completed at {end_time}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Stations processed: {results['stations_processed']}")
            logger.info(f"Years processed: {results['years_processed']}")
            logger.info(f"Statistics created: {results['statistics_created']}")
            logger.info(f"Statistics updated: {results['statistics_updated']}")
            
            if results["errors"]:
                logger.warning(f"Errors encountered: {len(results['errors'])}")
        
        return results
    
    def calculate_station_statistics(self, station_id: str, year: Optional[int] = None) -> Dict[str, any]:
        """
        Calculate statistics for a specific station and optionally a specific year
        
        Args:
            station_id: The weather station ID
            year: Optional specific year to calculate. If None, calculates for all years
            
        Returns:
            Dictionary with calculation results
        """
        start_time = datetime.now()
        logger.info(f"Calculating statistics for station {station_id}, year: {year or 'all'}")
        
        results = {
            "station_id": station_id,
            "year": year,
            "start_time": start_time,
            "end_time": None,
            "years_processed": 0,
            "statistics_created": 0,
            "statistics_updated": 0,
            "errors": [],
            "duration_seconds": 0
        }
        
        try:
            if year:
                # Calculate for specific year
                stats_result = self._calculate_station_year_statistics(station_id, year)
                results["years_processed"] = 1
                if stats_result["created"]:
                    results["statistics_created"] = 1
                else:
                    results["statistics_updated"] = 1
            else:
                # Calculate for all years for this station
                years = self._get_years_for_station(station_id)
                for year_to_process in years:
                    try:
                        stats_result = self._calculate_station_year_statistics(station_id, year_to_process)
                        results["years_processed"] += 1
                        if stats_result["created"]:
                            results["statistics_created"] += 1
                        else:
                            results["statistics_updated"] += 1
                    except Exception as e:
                        error_msg = f"Error calculating statistics for year {year_to_process}: {str(e)}"
                        logger.error(error_msg)
                        results["errors"].append(error_msg)
            
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Error during station statistics calculation: {str(e)}")
            results["errors"].append(f"Station statistics calculation error: {str(e)}")
            self.db.rollback()
        
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            results["end_time"] = end_time
            results["duration_seconds"] = duration
        
        return results
    
    def _calculate_station_year_statistics(self, station_id: str, year: int) -> Dict[str, any]:
        """
        Calculate statistics for a specific station and year
        
        Args:
            station_id: The weather station ID
            year: The year to calculate statistics for
            
        Returns:
            Dictionary with calculation result and whether record was created or updated
        """
        # Get statistical data from repository
        stats_data = self.record_repo.get_yearly_statistics_data(station_id, year)
        
        # Create or update statistics record
        existing_stats = self.stats_repo.get_by_station_and_year(station_id, year)
        
        if existing_stats:
            # Update existing record
            updated_stats = self.stats_repo.update(
                db_obj=existing_stats,
                obj_in=stats_data
            )
            return {"statistics": updated_stats, "created": False}
        else:
            # Create new record
            new_stats = self.stats_repo.create_or_update_statistics(
                station_id=station_id,
                year=year,
                stats_data=stats_data
            )
            return {"statistics": new_stats, "created": True}
    
    def _get_all_station_years(self) -> List[tuple]:
        """Get all unique station-year combinations from weather records"""
        query = text("""
            SELECT DISTINCT station_id, EXTRACT(YEAR FROM date) as year 
            FROM weather_records 
            ORDER BY station_id, year
        """)
        result = self.db.execute(query).fetchall()
        return [(row[0], int(row[1])) for row in result]
    
    def _get_years_for_station(self, station_id: str) -> List[int]:
        """Get all years with data for a specific station"""
        query = text("""
            SELECT DISTINCT EXTRACT(YEAR FROM date) as year 
            FROM weather_records 
            WHERE station_id = :station_id
            ORDER BY year
        """)
        result = self.db.execute(query, {"station_id": station_id}).fetchall()
        return [int(row[0]) for row in result]
    
    def get_statistics_summary(self) -> Dict[str, any]:
        """Get summary of calculated statistics"""
        try:
            # Count total statistics records
            total_stats = self.db.execute(
                text("SELECT COUNT(*) FROM weather_statistics")
            ).scalar()
            
            # Get date range of statistics
            stats_range = self.db.execute(
                text("SELECT MIN(year), MAX(year) FROM weather_statistics")
            ).fetchone()
            
            # Count stations with statistics
            stations_with_stats = self.db.execute(
                text("SELECT COUNT(DISTINCT station_id) FROM weather_statistics")
            ).scalar()
            
            # Get some sample statistics
            sample_stats = self.db.execute(
                text("""
                    SELECT station_id, year, avg_max_temperature_celsius, 
                           avg_min_temperature_celsius, total_precipitation_cm
                    FROM weather_statistics 
                    ORDER BY station_id, year 
                    LIMIT 5
                """)
            ).fetchall()
            
            return {
                "total_statistics_records": total_stats,
                "year_range": {
                    "start_year": int(stats_range[0]) if stats_range[0] else None,
                    "end_year": int(stats_range[1]) if stats_range[1] else None
                },
                "stations_with_statistics": stations_with_stats,
                "sample_statistics": [
                    {
                        "station_id": row[0],
                        "year": int(row[1]),
                        "avg_max_temp_celsius": float(row[2]) if row[2] else None,
                        "avg_min_temp_celsius": float(row[3]) if row[3] else None,
                        "total_precipitation_cm": float(row[4]) if row[4] else None
                    }
                    for row in sample_stats
                ],
                "status": "success"
            }
        except Exception as e:
            logger.error(f"Error getting statistics summary: {str(e)}")
            return {
                "error": str(e),
                "status": "error"
            }