# backend/test_db_connection.py - NEW FILE FOR DB TEST
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'operator_app.settings')
django.setup()

from django.db import connection
from django.db.utils import OperationalError

def test_database_connection():
    print("=== DATABASE CONNECTION TEST ===")
    
    try:
        # Test basic connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print("✅ PostgreSQL Connection: SUCCESS")
            
            # Test plant2_data table exists
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = 'plant2_data'
            """)
            table_exists = cursor.fetchone()
            
            if table_exists:
                print("✅ plant2_data table: EXISTS")
                
                # Show table structure
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'plant2_data'
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                print("📋 Table Structure:")
                for col in columns:
                    print(f"   {col[0]} ({col[1]})")
                    
                # Count existing records
                cursor.execute("SELECT COUNT(*) FROM plant2_data")
                count = cursor.fetchone()[0]
                print(f"📊 Existing records: {count}")
                
            else:
                print("❌ plant2_data table: NOT FOUND")
                
                # Show all tables
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                tables = cursor.fetchall()
                print("📋 Available tables:")
                for table in tables:
                    print(f"   {table[0]}")
            
    except OperationalError as e:
        print(f"❌ Database Connection FAILED: {e}")
        print("\n🔧 Possible solutions:")
        print("1. Check if PostgreSQL server is running on 192.168.0.35")
        print("2. Check if database 'Atomone' exists")
        print("3. Check if user 'postgres' has correct password 'atomone'")
        print("4. Check if port 5432 is accessible")
        
    except Exception as e:
        print(f"❌ Other Error: {e}")

if __name__ == "__main__":
    test_database_connection()



