"""
PostgreSQL database connection configuration
"""
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging

# Database connection information
DB_CONFIG = {
    'host': 'db',
    'port': '5432',  # The external port of the Postgres container mapped to the host
    'user': 'final_project',
    'password': 'final_project',
    'database': 'final_project'
}

# Create connection URL
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


# Use this function to get the connection URL
def get_database_url():
    """Return the database connection URL"""
    return DATABASE_URL

def get_db_connection():
    """
    Create a connection to the PostgreSQL database
    Returns:
        SQLAlchemy engine object
    Raises:
        SQLAlchemyError: If the connection cannot be established
    """
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as connection:
            connection.execute("SELECT 1")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Cannot connect to database: {str(e)}")
        raise