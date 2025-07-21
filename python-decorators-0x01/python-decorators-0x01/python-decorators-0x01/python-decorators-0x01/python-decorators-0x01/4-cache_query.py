import sqlite3
import functools
from typing import Callable, Any, Dict

# Simple in-memory cache dictionary
query_cache: Dict[str, Any] = {}

def with_db_connection(func: Callable) -> Callable:
    """
    Decorator that opens and closes a SQLite database connection.
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

def cache_query(func: Callable) -> Callable:
    """
    Decorator that caches the results of SQL queries using a string key.
    
    Args:
        func: A function that executes a SQL query. Must accept `query` as arg.
    
    Returns:
        A wrapped function that uses cache if the query string has been seen.
    """
    @functools.wraps(func)
    def wrapper(conn: sqlite3.Connection, *args, **kwargs) -> Any:
        # Attempt to get the query string from either args or kwargs
        query = kwargs.get("query") or (args[0] if args else None)
        if not query:
            raise ValueError("Missing SQL query for caching")

        if query in query_cache:
            print("[CACHE HIT] Returning cached result.")
            return query_cache[query]

        print("[CACHE MISS] Executing and caching result.")
        result = func(conn, *args, **kwargs)
        query_cache[query] = result
        return result
    return wrapper

@with_db_connection
@cache_query
def fetch_users_with_cache(conn: sqlite3.Connection, query: str) -> list:
    """
    Fetches users using a query and caches results to avoid re-execution.
    
    Args:
        conn: SQLite connection object
        query: SQL SELECT query
    
    Returns:
        Query result list
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

# Test calls
if __name__ == "__main__":
    query = "SELECT * FROM users"

    print("First call (expected cache miss):")
    users = fetch_users_with_cache(query=query)
    print(f"Users fetched: {len(users)}")

    print("\nSecond call (expected cache hit):")
    users_again = fetch_users_with_cache(query=query)
    print(f"Users fetched from cache: {len(users_again)}")
