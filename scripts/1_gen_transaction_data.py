import sys
import datetime as dt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, text, Integer, Identity

RUN_DATE_STR = sys.argv[1]

def generate_pseudo_data(run_date_str, num_rows=20000, num_users=1000):
    """Generates a Pandas DataFrame with pseudo transaction data."""
    # Generate pseudo user IDs
    user_ids = [f"user_{i:06d}" for i in np.random.choice(num_users, size=num_rows)]
    
    # Generate pseudo transaction IDs
    transaction_ids = np.random.randint(10**11, 10**12 - 1, size=num_rows)
    
    # Choose sources for the transactions
    sources = np.random.choice(["Current Account", "Credit Card", "Debit Card"],
                              size=num_rows, p=[0.6, 0.3, 0.1])
    
    # Generate random amounts
    amounts = np.random.uniform(1.0, 1000.0, size=num_rows)
    
    # Choose vendors from a list, with random probabilities
    vendors = ["Online Shopping", "Hospital", "Sport", "Grocery", "Restaurant",
               "Travel", "Entertainment", "Electronics", "Home Improvement",
               "Clothing", "Education", "Sending Out", "Utilities", "Other"]
    vendor_probabilities = np.random.dirichlet(np.ones(len(vendors)))
    vendor_choices = np.random.choice(vendors, size=num_rows, p=vendor_probabilities)
    
    # Generate pseudo times based on the run date
    start_date = dt.datetime.strptime(run_date_str, "%Y-%m-%d")
    time_deltas = [dt.timedelta(seconds=np.random.randint(0, 31536000)) for _ in range(num_rows)]
    times = [(start_date + delta).strftime('%Y-%m-%d %H:%M:%S') for delta in time_deltas]
    
    # Create the DataFrame
    df = pd.DataFrame({
        "User ID": user_ids,
        "Transaction ID": transaction_ids,
        "Amount": amounts,
        "Vendor": vendor_choices,
        "Sources": sources,
        "Time": times
    })
    
    return df

def insert_data_to_postgres(df, table_name="transaction_data"):
    """
    Inserts the DataFrame into a PostgreSQL table.
    It uses SQLAlchemy to connect to PostgreSQL.
    """
    # Use connection string from environment or default to the Airflow connection string
    postgres_conn_str = "postgresql+psycopg2://airflow:airflow@localhost/airflow"
    engine = create_engine(postgres_conn_str)
    
    try:
        # Define table metadata with primary key
        metadata = MetaData()
        transaction_table = Table(
            table_name, metadata,
            Column("id", Integer, Identity(), primary_key=True),  # Auto-incrementing primary key
            Column("User ID", String(255)),
            Column("Transaction ID", String(255)),
            Column("Amount", Float),
            Column("Vendor", String(255)),
            Column("Sources", String(255)),
            Column("Time", String(255)),
            schema='public'
        )
        
        # Create the table in the database
        metadata.create_all(engine)
        
        # Insert data into the table using pandas to_sql
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',  # Use 'append' to add data to the existing table
            index=False,
            schema='public'
        )
        
        print(f"Successfully inserted {len(df)} rows into {table_name} table at {RUN_DATE_STR}")

        # Verify the table exists
        with engine.connect() as connection:
            query = text(f"SELECT COUNT(*) FROM public.{table_name}")
            result = connection.execute(query)
            count = result.scalar()
            print(f"Table contains {count} rows")
    except Exception as e:
        raise

if __name__ == "__main__":
    # Generate pseudo transaction data
    df = generate_pseudo_data(RUN_DATE_STR)
    
    # Insert the pseudo data into PostgreSQL
    insert_data_to_postgres(df, table_name="transaction_data")

    print(f"Data generation and insertion complete at {RUN_DATE_STR}")