import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.session import test_connection, engine
from app.db.base import Base

def main():
    # Test connection
    if test_connection():
        print("Database connection successful!")
        
        # Create all tables
        try:
            Base.metadata.create_all(bind=engine)
            print("Database tables created successfully!")
        except Exception as e:
            print(f"Error creating tables: {e}")
    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    main()