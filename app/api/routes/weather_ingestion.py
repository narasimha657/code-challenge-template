# app/api/routes/weather_ingestion.py
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Dict, Any
import logging

from app.db.session import get_db
from app.services.weather_ingestion import WeatherIngestionService

logger = logging.getLogger(__name__)

router = APIRouter()

# Hardcoded folder path as requested
WEATHER_DATA_FOLDER = "./wx_data"  # Adjust this path as needed


@router.post("/ingest", response_model=Dict[str, Any])
async def start_weather_ingestion(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Start the weather data ingestion process.
    
    This endpoint initiates the ingestion of weather data from text files
    in the hardcoded wx_data folder. The process runs in the background.
    """
    try:
        logger.info("Weather data ingestion endpoint called")
        
        # Create ingestion service
        ingestion_service = WeatherIngestionService(db)
        
        # Add ingestion task to background tasks
        background_tasks.add_task(
            _run_ingestion_task, 
            WEATHER_DATA_FOLDER, 
            db
        )
        
        return {
            "message": "Weather data ingestion started",
            "status": "processing",
            "data_folder": WEATHER_DATA_FOLDER,
            "note": "Ingestion is running in the background. Use /api/weather/ingestion/status to check progress."
        }
        
    except Exception as e:
        logger.error(f"Error starting weather ingestion: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to start ingestion: {str(e)}"
        )


@router.post("/ingest/sync", response_model=Dict[str, Any])
async def start_weather_ingestion_sync(
    db: Session = Depends(get_db)
):
    """
    Start the weather data ingestion process synchronously.
    
    This endpoint runs the ingestion process synchronously and returns
    the complete results. Use this for smaller datasets or when you need
    immediate feedback.
    """
    try:
        logger.info("Synchronous weather data ingestion endpoint called")
        
        # Create ingestion service
        ingestion_service = WeatherIngestionService(db)
        
        # Run ingestion synchronously
        results = ingestion_service.ingest_weather_data(WEATHER_DATA_FOLDER)
        
        return {
            "message": "Weather data ingestion completed",
            "status": "completed",
            "data_folder": WEATHER_DATA_FOLDER,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error during synchronous weather ingestion: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to complete ingestion: {str(e)}"
        )


@router.get("/ingestion/status", response_model=Dict[str, Any])
async def get_ingestion_status(
    db: Session = Depends(get_db)
):
    """
    Get the current status of weather data in the database.
    
    Returns statistics about the ingested weather data including
    total records, stations, and date ranges.
    """
    try:
        logger.info("Ingestion status endpoint called")
        
        # Create ingestion service
        ingestion_service = WeatherIngestionService(db)
        
        # Get current status
        status = ingestion_service.get_ingestion_status()
        
        return {
            "message": "Current ingestion status",
            "status": status,
            "data_folder": WEATHER_DATA_FOLDER
        }
        
    except Exception as e:
        logger.error(f"Error getting ingestion status: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to get ingestion status: {str(e)}"
        )


@router.get("/ingestion/config")
async def get_ingestion_config():
    """
    Get the current ingestion configuration.
    
    Returns the hardcoded configuration settings for the ingestion process.
    """
    return {
        "data_folder": WEATHER_DATA_FOLDER,
        "description": "Weather data ingestion configuration",
        "supported_formats": ["txt"],
        "expected_file_format": {
            "fields": [
                "date (YYYYMMDD)",
                "max_temperature_tenths", 
                "min_temperature_tenths",
                "precipitation_tenths"
            ],
            "separator": "tab",
            "missing_value_indicator": "-9999"
        }
    }


async def _run_ingestion_task(data_folder: str, db: Session):
    """
    Background task to run the weather data ingestion.
    
    Args:
        data_folder: Path to the weather data folder
        db: Database session
    """
    try:
        logger.info(f"Starting background ingestion task for folder: {data_folder}")
        
        # Create a new service instance for the background task
        ingestion_service = WeatherIngestionService(db)
        
        # Run the ingestion
        results = ingestion_service.ingest_weather_data(data_folder)
        
        logger.info("Background ingestion task completed successfully")
        logger.info(f"Results: {results}")
        
    except Exception as e:
        logger.error(f"Background ingestion task failed: {str(e)}")
        # In a production environment, you might want to store this error
        # in a task status table or send notifications
        raise