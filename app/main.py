# app/main.py - Updated to include notification routes and scheduler
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from app.core.config import settings
from app.db.session import engine, SessionLocal
from app.db.base import Base
import logging
from app.db.session import test_connection
from datetime import datetime
from app.api.routes.weather_ingestion import router as weather_ingestion_router
from app.api.routes.weather_statistics import router as weather_stats_router
from app.api.routes.weather import router as weather_router
# Import all models to register them with Base.metadata
from app.models import *
logger = logging.getLogger(__name__)

# Test database connection before starting
if not test_connection():
    logger.error("Database connection failed. Check your database configuration.")
    import sys
    sys.exit(1)
    
# Create tables
try:
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully.")
    
    # Initialize default adjustment reason codes
    db = SessionLocal()
except Exception as e:
    logger.error(f"Error creating database tables: {e}")
    raise

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json" if settings.DEBUG else None,
    docs_url=f"{settings.API_V1_STR}/docs" if settings.DEBUG else None,
    redoc_url=f"{settings.API_V1_STR}/redoc" if settings.DEBUG else None,
)

# Add CORS middleware with more explicit configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Comment out TrustedHostMiddleware for debugging
# app.add_middleware(
#     TrustedHostMiddleware, 
#     allowed_hosts=settings.ALLOWED_HOSTS.split(",") if settings.ALLOWED_HOSTS else ["*"]
# )

@app.middleware("http")
async def debug_middleware(request: Request, call_next):
    """Debug middleware to log all requests"""
    logger.info(f"Request: {request.method} {request.url}")
    logger.info(f"Headers: {dict(request.headers)}")
    logger.info(f"Client: {request.client}")
    
    response = await call_next(request)
    
    logger.info(f"Response status: {response.status_code}")
    return response

@app.middleware("http")
async def audit_logging_middleware(request: Request, call_next):
    start_time = datetime.utcnow()
    
    # Log request details
    audit_data = {
        "timestamp": start_time.isoformat(),
        "user_id": getattr(request.state, 'user_id', None),
        "ip_address": request.client.host,
        "method": request.method,
        "url": str(request.url),
        "user_agent": request.headers.get("user-agent"),
    }
    
    response = await call_next(request)
    
    # Log response details
    audit_data.update({
        "response_time": (datetime.utcnow() - start_time).total_seconds(),
        "status_code": response.status_code,
    })
    
    # Store in audit_logs table
    # logger.info(f"AUDIT: {json.dumps(audit_data)}")
    
    return response


@app.get("/")
async def root():
    return {"message": "Weather API is running", "version": "1.0.0"}

# Include API routes
app.include_router(weather_ingestion_router, prefix=f"{settings.API_V1_STR}/weather/ingestion", tags=["weather_ingestion"])
app.include_router(weather_stats_router, prefix=f"{settings.API_V1_STR}/weather/statistics", tags=["weather_statistics"])
app.include_router(weather_router, prefix=f"{settings.API_V1_STR}/weather", tags=["weather"])

if __name__ == "__main__":
    # Run the FastAPI app
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG, log_level="debug")