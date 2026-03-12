
import argparse
import psycopg2
import sys

def get_args():
    parser = argparse.ArgumentParser(description="Remove non-ship MMSI entries from database.")
    parser.add_argument('--host', type=str, required=True, help="Database host")
    parser.add_argument('--port', type=int, required=True, help="Database port")
    parser.add_argument('--dbname', type=str, required=True, help="Database name")
    parser.add_argument('--user', type=str, required=True, help="Database user")
    parser.add_argument('--password', type=str, required=True, help="Database password")
    return parser.parse_args()

def get_db_connection(args):
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def get_trajectories_tables(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'trajectories_%' 
                AND table_schema = 'public';
            """)
            tables = [row[0] for row in cur.fetchall()]
            return tables
    except Exception as e:
        print(f"Error fetching table list: {e}")
        return []

def count_invalid_mmsi(conn, table_name):
    try:
        with conn.cursor() as cur:
            # Check if mmsi column exists (sanity check, though assumed based on table name pattern)
            cur.execute(f"""
                SELECT count(*) 
                FROM "{table_name}" 
                WHERE mmsi !~ '^[2-7]';
            """)
            count = cur.fetchone()[0]
            return count
    except Exception as e:
        print(f"Error counting in {table_name}: {e}")
        return 0

def delete_invalid_mmsi(conn, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM "{table_name}" 
                WHERE mmsi !~ '^[2-7]';
            """)
        conn.commit()
        print(f"Deleted invalid entries from {table_name}.")
    except Exception as e:
        conn.rollback()
        print(f"Error deleting from {table_name}: {e}")

def process_trajectories(conn):
    print("\n--- Processing Trajectories Tables ---")
    tables = get_trajectories_tables(conn)
    if not tables:
        print("No 'trajectories_' tables found.")
        return

    tables_with_counts = []
    total_invalid = 0
    
    print("Checking tables...")
    for table in tables:
        count = count_invalid_mmsi(conn, table)
        if count > 0:
            tables_with_counts.append((table, count))
            total_invalid += count
            print(f"  {table}: {count} invalid entries")
    
    if total_invalid == 0:
        print("No invalid MMSI entries found in trajectories tables.")
        return

    print(f"\nTotal invalid entries in trajectories tables: {total_invalid}")
    
    while True:
        response = input("Do you want to delete these entries? (y/n): ").lower().strip()
        if response == 'y':
            for table, _ in tables_with_counts:
                delete_invalid_mmsi(conn, table)
            break
        elif response == 'n':
            print("Skipping deletion for trajectories.")
            break
        else:
            print("Please enter 'y' or 'n'.")

def process_ships(conn):
    print("\n--- Processing Ships Table ---")
    table_name = "ships"
    
    # Check if table exists
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (table_name,))
            if not cur.fetchone():
                print(f"Table '{table_name}' does not exist.")
                return
    except Exception as e:
        print(f"Error checking for table {table_name}: {e}")
        return

    count = count_invalid_mmsi(conn, table_name)
    
    if count == 0:
        print(f"No invalid MMSI entries found in '{table_name}' table.")
        return

    print(f"Found {count} invalid entries in '{table_name}' table.")

    while True:
        response = input("Do you want to delete these entries? (y/n): ").lower().strip()
        if response == 'y':
            delete_invalid_mmsi(conn, table_name)
            break
        elif response == 'n':
            print("Skipping deletion for ships.")
            break
        else:
            print("Please enter 'y' or 'n'.")

def main():
    args = get_args()
    conn = get_db_connection(args)
    
    try:
        process_trajectories(conn)
        process_ships(conn)
    finally:
        conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
