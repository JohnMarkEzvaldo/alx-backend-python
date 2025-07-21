import sqlite3
import functools
import time
from typing import Callable, Any

def with_db_connection(func: Callable) -> Callable:
    """
    Decorator that opens and closes a SQLite connection.
    Passes the connection to the wrapped function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        conn = sqlite3.connect('users.db')
        try:
            return func(conn, *args, **kwargs)
        finally:
            conn.close()
    return wrapper

def retry_on_failure(retries: int = 3, delay: int = 1) -> Callable:
    """
    Decorator that retries a function if it raises an exception.
    
    Args:
        retries: Number of times to retry before giving up
        delay: Seconds to wait between retries
    
    Returns:
        A wrapped function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"[Attempt {attempt}] Error: {e}")
                    if attempt < retries:
                        print(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print("All retry attempts failed.")
                        raise
        return wrapper
    return decorator

@with_db_connection
@retry_on_failure(retries=3, delay=1)
def fetch_users_with_retry(conn: sqlite3.Connection) -> list:
    """
    Fetch all users from the database. Retries if failure occurs.
    
    Args:
        conn: SQLite connection object
    
    Returns:
        List of user records
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    return cursor.fetchall()

# Example usage
if __name__ == "__main__":
    try:
        users = fetch_users_with_retry()
        print(f"Fetched {len(users)} users.")
    except Exception as e:
        print(f"Final failure: {e}")
