def exec_sql(sql, database):
    """The interface function to execute sql on database.

    Args:
        sql (String): The sql to execute.
        database (db): The database to apply sql.

    Returns:
        bool: The return value. True for success, False otherwise.
        String: The error message.

    """
    err_msg = "error message"
    return True, err_msg