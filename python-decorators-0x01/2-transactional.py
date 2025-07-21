import sqlite3
import functools
from typing import Callable, Any

def with_db_connection(func: Callable) -> Callable:
    """
    Decorator that manages opening and closing the database connection.
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

def transactional(func: Callable) -> Callable:
    """
    Decorator that wraps a function inside a transaction block.
    Commits on success, rolls back on failure.
    
    Args:
        func: The function to decorate. Must receive `conn` as first argument.
    
    Returns:
        A wrapped function with transaction management.
    """
    @functools.wraps(func)
    def wrapper(conn: sqlite3.Connection, *args, **kwargs) -> Any:
        try:
            result = func(conn, *args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            print(f"Transaction failed: {e}")
            raise
    return wrapper

@with_db_connection
@transactional
def update_user_email(conn: sqlite3.Connection, user_id: int, new_email: str) -> None:
    """
    Updates the email address of a user in the database.
    
    Args:
        conn: SQLite connection object (from decorator)
        user_id: ID of the user to update
        new_email: New email address to set
    """
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET email = ? WHERE id = ?", (new_email, user_id))

# Example test
if __name__ == "__main__":
    try:
        update_user_email(user_id=1, new_email='Crawford_Cartwright@hotmail.com')
        print("Email updated successfully.")
    except Exception as e:
        print(f"Failed to update email: {e}")
