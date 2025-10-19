import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.database import SessionLocal
from app.api.api_v1.api import api_router
from app.initialization import ApplicationInitializer

# Initialize logger for uvicorn
uvicorn_logger = logging.getLogger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown events."""
    
    # Initialize the application
    initializer = ApplicationInitializer()
    
    try:
        uvicorn_logger.info("🚀 Starting Science N-grams API initialization...")
        
        with SessionLocal() as db:
            # Step 1: Initialize database and preprocessing
            uvicorn_logger.info("📊 Initializing database...")
            db_status = initializer.initialize_database(db)
            
            if db_status["preprocessing_completed"]:
                if db_status.get("preprocessing_time", 0) > 0:
                    uvicorn_logger.info(f"⚡ Preprocessing completed in {db_status['preprocessing_time']:.2f}s")
                
                # Step 2: Initialize burst detection (both methods)
                uvicorn_logger.info("🔥 Initializing burst detection...")
                burst_status = initializer.initialize_burst_detection(db, run_both=True)
                
                if burst_status["burst_detection_completed"]:
                    uvicorn_logger.info(f"⚡ Burst detection completed in {burst_status['detection_time']:.2f}s")
                    
                    # Log method-specific results
                    for method, success in burst_status.get("methods_run", {}).items():
                        status_icon = "✅" if success else "❌"
                        uvicorn_logger.info(f"  {status_icon} {method.capitalize()}: {'Success' if success else 'Failed'}")
                    
                    # Log detection counts with points
                    kleinberg_count = burst_status.get("kleinberg_detections", 0)
                    macd_count = burst_status.get("macd_detections", 0)
                    total_points = burst_status.get("total_points", 0)
                    kleinberg_points = burst_status.get("kleinberg_points", 0)
                    macd_points = burst_status.get("macd_points", 0)
                    
                    uvicorn_logger.info(f"📈 Detection Summary:")
                    uvicorn_logger.info(f"  • Kleinberg: {kleinberg_count:,} detections, {kleinberg_points:,} points")
                    uvicorn_logger.info(f"  • MACD: {macd_count:,} detections, {macd_points:,} points")
                    uvicorn_logger.info(f"  • Total: {total_points:,} burst points across both methods")
                else:
                    uvicorn_logger.warning("⚠️ Burst detection initialization incomplete")
                    if "error" in burst_status:
                        uvicorn_logger.error(f"❌ Error: {burst_status['error']}")
            else:
                uvicorn_logger.warning("⚠️ Database initialization incomplete")
                if "error" in db_status:
                    uvicorn_logger.error(f"❌ Error: {db_status['error']}")

            # Step 3: Load binary vote data
            uvicorn_logger.info("🗳️ Loading binary vote pairs configuration...")
            binary_vote_status = initializer.load_binary_vote_data(app.state)
            
            if binary_vote_status["binary_vote_data_loaded"]:
                uvicorn_logger.info(f"✅ Loaded {binary_vote_status['total_pairs']:,} binary vote pairs")
            else:
                uvicorn_logger.warning("⚠️ No binary vote data loaded")

            # Step 4: Load slider vote data
            uvicorn_logger.info("🗳️ Loading slider vote data...")
            slider_vote_status = initializer.load_slider_vote_data(app.state)
            
            if slider_vote_status["slider_vote_data_loaded"]:
                uvicorn_logger.info(f"✅ Loaded {slider_vote_status['total_pairs']:,} slider vote data")
            else:
                uvicorn_logger.warning("⚠️ No slider vote data loaded")

        # Store initialization summary in app state
        with SessionLocal() as db:
            app.state.initialization_summary = initializer.get_initialization_summary(db)
            app.state.burst_manager = initializer.burst_manager

        uvicorn_logger.info("🎉 Science N-grams API initialization completed! 🚀")
        
        yield
        
    except Exception as e:
        uvicorn_logger.error(f"🔥 Startup error: {e}")
        import traceback
        uvicorn_logger.error(f"Full traceback: {traceback.format_exc()}")
        raise

# FastAPI app setup
app = FastAPI(
    title="Science N-grams API",
    description="API for exploring scientific literature n-gram frequencies and burst patterns using Kleinberg and MACD methods",
    version="2.0.0",
    lifespan=lifespan
)

# CORS Middleware Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",  # For potential separate frontend apps
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Router Setup
app.include_router(api_router, prefix="/api/v1")

@app.get("/")
def read_root():
    """Root endpoint with API information."""
    return {
        "message": "Science N-grams API is running!",
        "version": "2.0.0",
        "features": [
            "Kleinberg burst detection",
            "MACD burst detection", 
            "N-gram frequency analysis",
            "Voting system",
            "Dynamic time-range scoring",
            "Point-based burst analysis"
        ],
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
def health_check():
    """Enhanced health check endpoint with detailed status."""
    try:
        with SessionLocal() as db:
            # Get current initialization summary
            if hasattr(app.state, 'initialization_summary'):
                summary = app.state.initialization_summary
            else:
                initializer = ApplicationInitializer()
                summary = initializer.get_initialization_summary(db)

            return {
                "status": "healthy",
                "version": "2.0.0",
                "components": {
                    "database": summary.get("database", {}),
                    "burst_detection": summary.get("burst_detection", {}),
                    "cache": summary.get("cache", {}),
                },
                "available_methods": summary.get("burst_methods_available", []),
                "vote_pairs": len(getattr(app.state, 'vote_pairs', [])),
                "model_structure": "point_based"
            }
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "version": "2.0.0"
        }

@app.get("/api/v1/methods")
def get_available_methods():
    """Get information about available burst detection methods."""
    return {
        "methods": [
            {
                "name": "kleinberg",
                "description": "Kleinberg's burst detection algorithm using state-based modeling",
                "parameters": {
                    "s": "Burst intensity parameter (default: 5.7)",
                    "gamma": "State transition cost parameter (default: 1.0)"
                },
                "output_fields": [
                    "global_score", "rank", "num_bursts", "burst_intervals"
                ],
                "analysis_type": "point_based"
            },
            {
                "name": "macd",
                "description": "MACD-based burst detection using Poisson-gated scoring",
                "parameters": {
                    "short_span": "Short EMA period (default: 24)",
                    "long_span": "Long EMA period (default: 48)", 
                    "signal_span": "Signal EMA period (default: 12)",
                    "poisson_threshold": "Poisson z-score threshold (default: 2.0)"
                },
                "output_fields": [
                    "global_score", "rank", "num_bursts", "burst_intervals"
                ],
                "analysis_type": "point_based"
            }
        ],
        "model_structure": {
            "burst_detections": "Summary records with global scores and method parameters",
            "burst_points": "Individual time point contributions for dynamic analysis"
        },
        "dynamic_capabilities": [
            "Score computation within any date range",
            "Real-time filtering by time intervals",
            "Cross-method comparison",
            "Point-level analysis"
        ]
    }

@app.get("/api/v1/status")
def get_system_status():
    """Get detailed system status including processing statistics."""
    try:
        with SessionLocal() as db:
            if hasattr(app.state, 'burst_manager'):
                burst_manager = app.state.burst_manager
                detection_status = burst_manager.get_detection_status(db)
                method_summary = burst_manager.get_method_summary(db)
                
                return {
                    "system": "operational",
                    "detection_status": detection_status,
                    "method_summary": method_summary,
                    "cache_info": burst_manager.get_cache_info(),
                    "model_type": "point_based",
                    "capabilities": {
                        "dynamic_scoring": True,
                        "time_range_filtering": True,
                        "point_level_analysis": True,
                        "cross_method_comparison": True
                    }
                }
            else:
                return {"system": "initializing", "message": "Burst manager not yet available"}
                
    except Exception as e:
        return {"system": "error", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
