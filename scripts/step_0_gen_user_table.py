import sys
import os
import datetime as dt
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, text, Integer, Identity

dotenv_path = os.path.join("./scripts", '.env')
load_dotenv(dotenv_path)

def generate_user_data(num_rows=10000):
    """Generates a Pandas DataFrame with pseudo transaction data."""

    # User IDs
    user_ids = [f"user_{i:06d}" for i in np.random.choice(num_rows, size=num_rows)]

    # Transaction IDs
    age = np.random.randint(22, 90, size=num_rows)

    # Source
    gender = np.random.choice(["M", "F", "Other"], 
                            size=num_rows, p=[0.5, 0.4, 0.1])

    # Amounts
    occupation = np.random.choice(
            ["Software Engineer", "Teacher", "Doctor", "Sales", "Manager",
            "Student", "Business Owner", "Accountant", "Researcher", "Others"], 
            size=num_rows, 
            p=[0.15, 0.12, 0.08, 0.13, 0.12, 0.1, 0.1, 0.08, 0.07, 0.05]
    )

    # location
    location = np.random.choice(["City A", "City B", "City C", "City D", "City E", 
                                "City F", "City G", "City H", "Unknown"], 
                            size=num_rows, p=[0.1,0.12,0.08,0.1,0.15,0.05,0.1,0.1,0.2])
    
    # Day start
    start = dt.datetime(2020, 1, 1)
    end = dt.datetime(2025, 3, 1)
    
    # Calculate total days between start and end
    days_between = (end - start).days
    
    # Generate random days and add to start date
    random_days = np.random.randint(0, days_between, size=num_rows)
    day_start = [(start + dt.timedelta(days=int(x))).strftime('%Y-%m-%d') for x in random_days]

    # Create DataFrame
    df = pd.DataFrame({
        "user_id": user_ids,
        "age": age,
        "gender": gender,
        "location": location,
        "occupation": occupation,
        "day_start": day_start,
        
    })
    
    df = df.drop_duplicates(subset=["user_id"], keep="last")

    return df


def insert_data_to_postgres(df, table_name="customer_data"):
    postgres_conn_str = "postgresql+psycopg2://airflow:airflow@localhost/airflow"
    engine = create_engine(postgres_conn_str)
    
    try:
        # Define table metadata with primary key
        metadata = MetaData()
        transaction_table = Table(
            table_name, metadata,
            Column("user_id", String(255), primary_key=True),
            Column("age", String(255)),
            Column("gender", String(255)),
            Column("location", String(255)),
            Column("occupation", String(255)),
            Column("day_start", String(255)),
            schema='public'
        )
        
        # Create the table in the database
        metadata.create_all(engine)
        
        # Insert data into the table using pandas to_sql
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            schema='public'
        )

        # Verify the table exists
        with engine.connect() as connection:
            query = text(f"SELECT COUNT(*) FROM public.{table_name}")
            result = connection.execute(query)
            count = result.scalar()
            print(f"Table contains {count} rows")
    except Exception as e:
        raise
        
if __name__ == "__main__": 
    # Generate and print the data
    customer_df = generate_user_data(num_rows=10000)
    print(customer_df.head())
    
    try:
        insert_data_to_postgres(customer_df, table_name="customer_data")
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Data already exist {e}")
        sys.exit(1)

