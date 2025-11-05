import sqlite3
DATABSE_FILE = 'patients.db'
DQ_FAILURES_FOUND = 0

def get_db_connection():
    try:
        conn = sqlite3.connect(DATABSE_FILE)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None
def execute_dq_query(conn, check_name, sql_query):
    global DQ_FAILURES_FOUND
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql_query)
        bad_record_count = cursor.fetchone()[0]
        
        if bad_record_count > 0:
            print(f"🔴 FAILURE: {check_name} found {bad_record_count} bad records.")
            DQ_FAILURES_FOUND += 1
        else:
            print(f"🟢 SUCCESS: {check_name} passed.")
            
    except sqlite3.Error as e:
        print(f"🚨 SQL Error during {check_name}: {e}")
def check_null_keys(conn):
    sql_check = """
    SELECT COUNT(*) 
    FROM patients 
    WHERE patient_id IS NULL;
    """
    execute_dq_query(conn, "Critical Null Keys Check", sql_check)

def check_invalid_range(conn):
    sql_check = """
    SELECT COUNT(*) 
    FROM patients 
    WHERE weight_kg <= 0 OR weight_kg > 500;
    """
    execute_dq_query(conn, "Invalid Weight Range Check", sql_check)

def check_duplicate_records(conn):
    """Checks for duplicate patient IDs (assuming patient_id should be unique)."""
    # Why: Ensures unique records and prevents calculation errors in analysis. (Uniqueness Constraint)
    sql_check = """
    SELECT COUNT(patient_id)
    FROM (
        SELECT patient_id 
        FROM patients 
        GROUP BY patient_id
        HAVING COUNT(patient_id) > 1
    );
    """
    execute_dq_query(conn, "Duplicate Patient ID Check", sql_check)
def run_all_dq_checks():
    """Main function to run the sequential DQ workflow."""
    conn = get_db_connection()
    if conn is None:
        print("Cannot run checks without a database connection.")
        return False
    
    print("\n--- Starting Data Quality Checks ---")
    
    # Run each check sequentially
    check_null_keys(conn)
    check_invalid_range(conn)
    check_duplicate_records(conn)

    conn.close()
    
    # Step 10: Log Results
    if DQ_FAILURES_FOUND > 0:
        print(f"\n❌ PIPELINE FAILED: Total {DQ_FAILURES_FOUND} DQ checks failed.")
        # In a real pipeline, this would raise an exception to halt the next task in Airflow.
        return False
    else:
        print("\n✅ PIPELINE SUCCESS: All Data Quality checks passed.")
        return True

# Example execution (Will be moved to run_pipeline.py later)
if __name__ == '__main__':
    run_all_dq_checks()