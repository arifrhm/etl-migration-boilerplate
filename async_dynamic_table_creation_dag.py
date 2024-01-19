from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.transfers.postgres_to_postgres import AsyncPostgresOperator
import asyncpg
import asyncio
import logging
from logging.handlers import FileHandler

# Source and destination database connection parameters
source_db_params = {
    'dbname': 'source_database_name',
    'user': 'source_username',
    'password': 'source_password',
    'host': 'source_host',
    'port': 'source_port'
}

dest_db_params = {
    'dbname': 'destination_database_name',
    'user': 'destination_username',
    'password': 'destination_password',
    'host': 'destination_host',
    'port': 'destination_port'
}

# Configure logging
log_format = "%(asctime)s [%(levelname)s] - %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# Add a FileHandler to log to a file
file_handler = FileHandler('/path/to/your/log/file.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(file_handler)

async def create_tables_in_destination(**kwargs):
    try:
        source_conn = await asyncpg.connect(**source_db_params)
        dest_conn = await asyncpg.connect(**dest_db_params)

        # Get a list of table names from the source database (replace 'public' with your schema)
        source_tables = await source_conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

        # Iterate over each source table and dynamically create tables in the destination database
        for source_table in source_tables:
            source_table_name = source_table['table_name']

            # Retrieve column information for the source table
            columns_info = await source_conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{source_table_name}'")

            # Construct the CREATE TABLE statement for the destination database
            create_table_sql = f"CREATE TABLE {source_table_name} ("

            for column_info in columns_info:
                create_table_sql += f"{column_info['column_name']} {column_info['data_type']}, "

            create_table_sql = create_table_sql.rstrip(', ')  # Remove the trailing comma and space
            create_table_sql += ");"

            # Execute the CREATE TABLE statement in the destination database
            await dest_conn.execute(create_table_sql)

            # Log information for each table creation
            logging.info(f"Table '{source_table_name}' created in the destination database.")

    except Exception as e:
        # Log any exceptions that occur
        logging.error(f"Error: {e}")

    finally:
        # Close the connections
        await source_conn.close()
        await dest_conn.close()

# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'async_dynamic_table_creation_dag',
    default_args=default_args,
    description='Async Dynamically create tables in destination database',
    schedule_interval=timedelta(days=1),
)

# Define AsyncPostgresOperator
create_tables_task = AsyncPostgresOperator(
    task_id='create_tables_task',
    sql="SELECT 1",  # Placeholder SQL, as the actual query is executed asynchronously in the Python function
    postgres_conn_id='your_postgres_conn_id',  # Connection ID from Airflow UI
    params={},  # Pass any parameters needed for the query
    provide_context=True,  # Pass the context to the function
    python_callable=create_tables_in_destination,
    op_args=[],  # Optional arguments to pass to the python_callable
    op_kwargs={},  # Optional keyword arguments to pass to the python_callable
    dag=dag,
)

# Set task dependencies
create_tables_task

if __name__ == "__main__":
    dag.cli()
