import psycopg2
from psycopg2.extras import execute_values

print("Data sync start ho raha hai...")

# 1. Local Database (Aapki AtomOne Factory wala)
local_conn = psycopg2.connect(
    host="192.168.0.35",
    database="Atomone",
    user="postgres",
    password="atomone",
    port="5432"
)
# Named cursor ya server-side cursor ka use karna RAM bachayega
local_cursor = local_conn.cursor()

# 2. Supabase Cloud Database
supabase_conn = psycopg2.connect(
    host="aws-1-ap-south-1.pooler.supabase.com",
    database="postgres",
    user="postgres.osjzghoogahecsiejcgj",
    password="Abhishek kumar", 
    port="5432"
)
supabase_cursor = supabase_conn.cursor()

print("Supabase table clean kar rahe hain...")
supabase_cursor.execute("TRUNCATE TABLE plant2_data;")
supabase_conn.commit()

# 3. Data nikalna aur Batch mein upload karna
print("Batch upload shuru ho raha hai...")
local_cursor.execute("SELECT timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift FROM plant2_data;")

insert_query = """
    INSERT INTO plant2_data (timestamp, tool_id, machine_no, count, cumulative_count, tpm, idle_time, shut_height, shift) 
    VALUES %s
"""

batch_size = 20000  # Ek baar mein 20,000 rows bhejenge
total_uploaded = 0

while True:
    # Local se 20,000 rows uthao
    records = local_cursor.fetchmany(batch_size)
    
    if not records:
        break # Jab saara data khatam ho jaye toh loop rok do
        
    # Supabase par bhej do
    execute_values(supabase_cursor, insert_query, records)
    supabase_conn.commit() # Har batch ke baad save karo
    
    total_uploaded += len(records)
    print(f"✅ Progress: {total_uploaded} rows cloud par upload ho chuki hain...")

print("🚀 BOOM! Saara 40 Lakh data Supabase par successfully upload ho gaya!")

# Connections close
local_cursor.close()
local_conn.close()
supabase_cursor.close()
supabase_conn.close()