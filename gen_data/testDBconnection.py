# # test db connection!
# import psycopg2

# try:
#     conn = psycopg2.connect(
#         dbname="postgres",
#         user="postgres",
#         password=1234,
#         host='127.0.0.1',
#         port="5432"
#     )
#     print("Connected successfully!")
#     conn.close()
# except Exception as e:
#     print("Connection failed:", e)



# test db connection!
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    print("Connected successfully!")

    # Create a cursor object to execute queries
    cursor = conn.cursor()
    
    # Query to get all table names in the public schema
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    
    # Fetch all table names
    tables = cursor.fetchall()
    
    print("\nAvailable tables:")
    for table in tables:
        print(table[0])
    
    # Close cursor and connection
    cursor.close()
    conn.close()
except Exception as e:
    print("Connection failed:", e)