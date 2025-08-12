# app/api/routes/weather_statistics.py
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from typing import Dict, Any, Optional
import logging

from app.db.session import get_db
from app.services.weather_statistics import WeatherStatisticsService
from app.db.repositories.weather_repo import WeatherStatisticsRepository
from app.schemas.weather import WeatherStatsResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/calculate", response_model=Dict[str, Any])
async def calculate_all_statistics(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Calculate yearly statistics for all weather stations and years.
    
    For every year, for every weather station, this calculates:
    - Average maximum temperature (in degrees Celsius)
    - Average minimum temperature (in degrees Celsius) 
    - Total accumulated precipitation (in centimeters)
    
    Missing data is ignored when calculating these statistics.
    Statistics that cannot be calculated are stored as NULL.
    """
    try:
        logger.info("Weather statistics calculation endpoint called")
        
        # Create statistics service
        stats_service = WeatherStatisticsService(db)
        
        # Add calculation task to background tasks
        background_tasks.add_task(
            _run_statistics_calculation_task,
            db
        )
        
        return {
            "message": "Weather statistics calculation started",
            "status": "processing",
            "note": "Calculation is running in the background. Use /api/weather/statistics/status to check progress."
        }
        
    except Exception as e:
        logger.error(f"Error starting statistics calculation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start statistics calculation: {str(e)}"
        )


@router.post("/calculate/sync", response_model=Dict[str, Any])
async def calculate_all_statistics_sync(
    db: Session = Depends(get_db)
):
    """
    Calculate yearly statistics for all weather stations and years synchronously.
    
    This endpoint runs the calculation process synchronously and returns
    the complete results. Use this for smaller datasets or when you need
    immediate feedback.
    """
    try:
        logger.info("Synchronous weather statistics calculation endpoint called")
        
        # Create statistics service
        stats_service = WeatherStatisticsService(db)
        
        # Run calculation synchronously
        results = stats_service.calculate_all_statistics()
        
        return {
            "message": "Weather statistics calculation completed",
            "status": "completed",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error during synchronous statistics calculation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to complete statistics calculation: {str(e)}"
        )


@router.post("/calculate/station/{station_id}", response_model=Dict[str, Any])
async def calculate_station_statistics(
    station_id: str,
    year: Optional[int] = Query(None, description="Specific year to calculate. If not provided, calculates for all years."),
    db: Session = Depends(get_db)
):
    """
    Calculate yearly statistics for a specific weather station.
    
    Args:
        station_id: The weather station ID to calculate statistics for
        year: Optional specific year to calculate. If not provided, calculates for all years.
    """
    try:
        logger.info(f"Station statistics calculation endpoint called for station: {station_id}, year: {year}")
        
        # Create statistics service
        stats_service = WeatherStatisticsService(db)
        
        # Run calculation for specific station
        results = stats_service.calculate_station_statistics(station_id, year)
        
        return {
            "message": f"Statistics calculation completed for station {station_id}",
            "status": "completed",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error calculating statistics for station {station_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate statistics for station {station_id}: {str(e)}"
        )


@router.get("/status", response_model=Dict[str, Any])
async def get_statistics_status(
    db: Session = Depends(get_db)
):
    """
    Get the current status of calculated weather statistics.
    
    Returns summary information about the calculated statistics including
    total records, stations, and year ranges.
    """
    try:
        logger.info("Statistics status endpoint called")
        
        # Create statistics service
        stats_service = WeatherStatisticsService(db)
        
        # Get current status
        status = stats_service.get_statistics_summary()
        
        return {
            "message": "Current statistics status",
            "status": status
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get statistics status: {str(e)}"
        )


@router.get("/", response_model=WeatherStatsResponse)
async def get_weather_statistics(
    station_id: Optional[str] = Query(None, description="Filter by weather station ID"),
    year: Optional[int] = Query(None, description="Filter by specific year"),
    start_year: Optional[int] = Query(None, description="Filter by start year (inclusive)"),
    end_year: Optional[int] = Query(None, description="Filter by end year (inclusive)"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    size: int = Query(50, ge=1, le=1000, description="Number of records per page"),
    db: Session = Depends(get_db)
):
    """
    Get weather statistics with optional filtering and pagination.
    
    Returns calculated yearly statistics for weather stations including:
    - Average maximum temperature (degrees Celsius)
    - Average minimum temperature (degrees Celsius)
    - Total accumulated precipitation (centimeters)
    
    Query parameters:
    - station_id: Filter by specific weather station
    - year: Filter by specific year
    - start_year: Filter by start year (inclusive)
    - end_year: Filter by end year (inclusive)
    - page: Page number for pagination (starts from 1)
    - size: Number of records per page (max 1000)
    """
    try:
        logger.info(f"Weather statistics API called with filters: station_id={station_id}, year={year}, start_year={start_year}, end_year={end_year}")
        
        # Calculate skip for pagination
        skip = (page - 1) * size
        
        # Create repository
        stats_repo = WeatherStatisticsRepository(db)
        
        # Get statistics with filters
        statistics = stats_repo.get_statistics_by_filters(
            station_id=station_id,
            year=year,
            start_year=start_year,
            end_year=end_year,
            skip=skip,
            limit=size
        )
        
        # Get total count for pagination
        total = stats_repo.count_statistics_by_filters(
            station_id=station_id,
            year=year,
            start_year=start_year,
            end_year=end_year
        )
        
        return WeatherStatsResponse(
            total=total,
            page=page,
            size=size,
            data=statistics
        )
        
    except Exception as e:
        logger.error(f"Error getting weather statistics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get weather statistics: {str(e)}"
        )


async def _run_statistics_calculation_task(db: Session):
    """
    Background task to run the weather statistics calculation.
    
    Args:
        db: Database session
    """
    try:
        logger.info("Starting background statistics calculation task")
        
        # Create a new service instance for the background task
        stats_service = WeatherStatisticsService(db)
        
        # Run the calculation
        results = stats_service.calculate_all_statistics()
        
        logger.info("Background statistics calculation task completed successfully")
        logger.info(f"Results: {results}")
        
    except Exception as e:
        logger.error(f"Background statistics calculation task failed: {str(e)}")
        # In a production environment, you might want to store this error
        # in a task status table or send notifications
        raise