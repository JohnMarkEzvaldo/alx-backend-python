import sqlite3
import functools
import logging
from datetime import datetime
from typing import Any, Callable, Optional

# Configure logging for database queries
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('db_queries')

def log_queries(func: Callable) -> Callable:
    """
    Decorator that logs SQL queries before execution.
    
    This decorator intercepts function calls that execute SQL queries,
    logs the query with timestamp, and handles potential errors gracefully.
    
    Args:
        func: The function to be decorated (should accept 'query' parameter)
        
    Returns:
        Wrapped function with query logging functionality
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # Extract query from function arguments
        query = None
        
        # Try to get query from kwargs first
        if 'query' in kwargs:
            query = kwargs['query']
        # If not in kwargs, try to find it in args based on function signature
        elif args:
            import inspect
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            if params and len(args) > 0:
                try:
                    query_index = params.index('query')
                    if len(args) > query_index:
                        query = args[query_index]
                except (ValueError, IndexError):
                    query = f"Query not found in standard position. Args: {args}"
        
        # Log the query with metadata
        timestamp = datetime.now().isoformat()
        function_name = func.__name__
        
        if query:
            clean_query = ' '.join(query.split()) if isinstance(query, str) else str(query)
            logger.info(f"[{timestamp}] Executing SQL in {function_name}: {clean_query}")
        else:
            logger.warning(f"[{timestamp}] {function_name} called but no query parameter found")
        
        try:
            result = func(*args, **kwargs)
            logger.info(f"[{timestamp}] Query in {function_name} completed successfully")
            return result
            
        except sqlite3.Error as e:
            logger.error(f"[{timestamp}] Database error in {function_name}: {e}")
            logger.error(f"[{timestamp}] Failed query: {query}")
            raise
            
        except Exception as e:
            logger.error(f"[{timestamp}] Unexpected error in {function_name}: {e}")
            logger.error(f"[{timestamp}] Failed query: {query}")
            raise
    
    return wrapper

# Example usage with the provided function
@log_queries
def fetch_all_users(query: str) -> list:
    """
    Fetch all users from the database using the provided query.
    
    Args:
        query: SQL query string to execute
        
    Returns:
        List of tuples containing query results
    """
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# Additional decorated functions for testing
@log_queries
def execute_query(query: str, params: Optional[tuple] = None) -> list:
    """Execute a parameterized query safely."""
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

@log_queries
def update_user(query: str, params: tuple) -> int:
    """Execute an UPDATE query and return affected rows."""
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query, params)
    affected_rows = cursor.rowcount
    conn.commit()
    conn.close()
    return affected_rows

# Test setup and demonstration
def setup_test_database():
    """Create a test database with sample data."""
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insert sample data
    sample_users = [
        ('Alice Johnson', 'alice@example.com', 28),
        ('Bob Smith', 'bob@example.com', 35),
        ('Carol Davis', 'carol@example.com', 22),
        ('David Wilson', 'david@example.com', 41)
    ]
    
    cursor.execute('DELETE FROM users')  # Clear existing data
    cursor.executemany('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', sample_users)
    
    conn.commit()
    conn.close()
    print("Test database setup complete!")

def run_tests():
    """Run comprehensive tests to demonstrate the decorator."""
    print("=" * 50)
    print("TESTING LOG_QUERIES DECORATOR")
    print("=" * 50)
    
    # Setup test database
    setup_test_database()
    
    print("\\n1. Testing successful query:")
    try:
        users = fetch_all_users(query="SELECT * FROM users")
        print(f"Retrieved {len(users)} users")
        for user in users[:2]:
            print(f"  - ID: {user[0]}, Name: {user[1]}, Email: {user[2]}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\\n2. Testing query with WHERE clause:")
    try:
        young_users = execute_query(
            query="SELECT name, age FROM users WHERE age < ?", 
            params=(30,)
        )
        print(f"Found {len(young_users)} users under 30")
        for user in young_users:
            print(f"  - {user[0]}, age {user[1]}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\\n3. Testing UPDATE query:")
    try:
        affected = update_user(
            query="UPDATE users SET age = ? WHERE name = ?",
            params=(29, 'Alice Johnson')
        )
        print(f"Updated {affected} rows")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\\n4. Testing malformed SQL (error handling):")
    try:
        bad_result = fetch_all_users(query="INVALID SQL QUERY")
        print("This shouldn't execute")
    except Exception as e:
        print(f"Expected error caught: {type(e).__name__}: {e}")
    
    print("\\n5. Testing non-existent table:")
    try:
        bad_result = fetch_all_users(query="SELECT * FROM non_existent_table")
        print("This shouldn't execute")
    except Exception as e:
        print(f"Expected error caught: {type(e).__name__}: {e}")
    
    print("\\n6. Testing database connection failure:")
    print("(Simulate manually by corrupting or locking the DB file if needed)")

if __name__ == "__main__":
    run_tests()
