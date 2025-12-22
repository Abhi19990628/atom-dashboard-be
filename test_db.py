import os
import sys
import django

# Add the project directory to Python path
sys.path.append('.')

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')

# Setup Django
django.setup()

# Test connection
from django.db import connection

print("Testing database connection...")

try:
    with connection.cursor() as cursor:
        cursor.execute("SELECT version()")
        db_version = cursor.fetchone()[0]
        print(f"✅ Database connected successfully!")
        print(f"PostgreSQL Version: {db_version}")
        
        # Test your table
        cursor.execute("SELECT COUNT(*) FROM plant1_data")
        count = cursor.fetchone()[0]
        print(f"✅ plant1_data table has {count} records")
        
        # Show some sample data
        cursor.execute("SELECT * FROM plant1_data LIMIT 3")
        rows = cursor.fetchall()
        print(f"✅ Sample data:")
        for row in rows:
            print(f"   {row}")
            
except Exception as e:
    print(f"❌ Connection failed: {e}")