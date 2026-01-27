from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError
import os
import logging
from sqlalchemy.pool import QueuePool
import psycopg2
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

Base = declarative_base()

## 🔒 PostgreSQL connection for vector operations
PG_CONNECTION_STRING = os.getenv("PG_CONNECTION_STRING")

if not PG_CONNECTION_STRING:
    # Try to build from individual components
    PG_USERNAME = os.getenv("PG_USERNAME")
    PG_PASSWORD = os.getenv("PG_PASSWORD")
    PG_HOSTNAME = os.getenv("PG_HOSTNAME")
    PG_DATABASE = os.getenv("PG_DATABASE")
    PG_PORT = os.getenv("PG_PORT")

    PG_CONNECTION_STRING = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOSTNAME}:{PG_PORT}/{PG_DATABASE}"


_pg_engine = None

def get_pg_engine():
    """Get PostgreSQL engine with connection pooling and proper cleanup"""
    global _pg_engine
    if _pg_engine is None:
        try:
            _pg_engine = create_engine(
                PG_CONNECTION_STRING, # type:ignore
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,  # Recycle connections every hour
                pool_timeout=30,  # Timeout for getting connection from pool
                connect_args={"connect_timeout": 10, "application_name": "mtlb_api", "sslmode": "prefer"},
            )
            logger.info("PostgreSQL engine created successfully")
        except Exception as e:
            logger.error(f"Error creating PostgreSQL engine: {e}")
            raise

    return _pg_engine


def get_pg_connection():
    """Get PostgreSQL connection with proper context management"""
    engine = get_pg_engine()
    connection = None
    try:
        connection = engine.connect()
        yield connection
    except Exception as e:
        logger.error(f"Error in PostgreSQL connection: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            try:
                connection.close()
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")


def test_postgres_connection():
    """Test PostgreSQL database connectivity"""
    try:
        engine = get_pg_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            if result:
                logger.info("✅ PostgreSQL database connection successful")
                return True
    except Exception as e:
        logger.error(f"❌ PostgreSQL database connection failed: {e}")
        return False


def check_pg_vector_extension():
    """Check if pgvector extension is installed and available"""
    try:
        engine = get_pg_engine()
        with engine.connect() as conn:
            # Check if vector extension is installed
            result = conn.execute(text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector')")).fetchone()

            if result and result[0]:
                logger.info("✅ PostgreSQL vector extension is available")
                return True
            else:
                logger.warning("⚠️ PostgreSQL vector extension not found")
                return False
    except Exception as e:
        logger.error(f"❌ Error checking vector extension: {e}")
        return False


def health_check():
    """Fast health check for all database connections with timeout handling"""
    health_status = {"postgresql": {"status": "unknown", "details": {}}, "overall": "unknown"}

    # Fast PostgreSQL health check
    try:
        pg_engine = get_pg_engine()
        with pg_engine.connect() as conn:
            # Set query timeout
            conn.execute(text("SET statement_timeout = '5s'"))
            result = conn.execute(text("SELECT 1")).fetchone()
            if result and result[0] == 1:
                health_status["postgresql"] = {"status": "healthy", "details": {"connection": "active"}}
            else:
                health_status["postgresql"] = {"status": "unhealthy", "details": {"error": "Invalid response"}}
    except Exception as e:
        health_status["postgresql"] = {"status": "error", "details": {"error": str(e)[:100]}}

    # Overall status
    postgres_ok = health_status["postgresql"]["status"] == "healthy"

    if postgres_ok:
        health_status["overall"] = "healthy"
    else:
        health_status["overall"] = "unhealthy"

    return health_status


# Initialize connections on module import
if __name__ == "__main__":
    print("🔍 Testing database connections...")

    # Test all connections
    health = health_check()

    print("\n📊 Database Health Status:")
    for service, status in health.items():
        if service != "connection_stats":
            print(f"  {service}: {status['status']} - {status['message']}")
