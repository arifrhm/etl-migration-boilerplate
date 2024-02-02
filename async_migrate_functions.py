import asyncio
import asyncpg

async def get_function_names(source_conn):
    try:
        source_connection = await asyncpg.connect(**source_conn)
        function_names = await source_connection.fetch(
            "SELECT proname FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');"
        )
        return [record['proname'] for record in function_names]

    except Exception as e:
        print(f"Error retrieving function names: {e}")
        return []

    finally:
        if source_connection:
            await source_connection.close()

async def migrate_functions(source_conn, dest_conn):
    functions_to_migrate = await get_function_names(source_conn)

    try:
        source_connection = await asyncpg.connect(**source_conn)
        dest_connection = await asyncpg.connect(**dest_conn)

        for function_name in functions_to_migrate:
            function_definition = await source_connection.fetchval(
                f"SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = $1;", function_name
            )

            await dest_connection.execute(function_definition)
            print(f"Function '{function_name}' migrated successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if source_connection:
            await source_connection.close()
        if dest_connection:
            await dest_connection.close()

# Example usage
source_connection_info = {
    "host": "source_db_host",
    "database": "db1",
    "user": "your_username",
    "password": "your_password",
}

dest_connection_info = {
    "host": "dest_db_host",
    "database": "db2",
    "user": "your_username",
    "password": "your_password",
}

# Run the migration asynchronously
asyncio.run(migrate_functions(source_connection_info, dest_connection_info))
