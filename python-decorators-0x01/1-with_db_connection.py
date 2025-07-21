import sqlite3
import functools
from typing import Callable, Any

def with_db_connection(func: Callable) -> Callable:
    """
    Decorator that opens a SQLite database connection and passes it to the function.
    Automatically closes the connection after execution.
    
    Args:
        func: The function to decorate. Must accept `conn` as first argument.
    
    Returns:
        A wrapped function with connection handling.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        conn = sqlite3.connect('users.db')
        try:
            return func(conn, *args, **kwargs)
        finally:
            conn.close()
    return wrapper

@with_db_connection
def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> tuple:
    """
    Fetch a user by ID from the database.
    
    Args:
        conn: SQLite3 connection object (injected by decorator)
        user_id: ID of the user to fetch
    
    Returns:
        Tuple with user data
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    return cursor.fetchone()

# Example test
if __name__ == "__main__":
    user = get_user_by_id(user_id=1)
    if user:
        print(f"User found: ID={user[0]}, Name={user[1]}, Email={user[2]}")
    else:
        print("User not found.")
