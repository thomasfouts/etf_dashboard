import psycopg2
import pandas as pd


def get_db_connection():
    
    conn = psycopg2.connect(
        host='my-db-instance.c56guommcuf9.us-east-2.rds.amazonaws.com',
        database="initial_db",
        user="postgres",
        password="Tjfouts17"
    )
    return conn


def save_to_db(df, table_name):
    # Connect to the database
    conn = get_db_connection()
    cur = conn.cursor()

    # Ensure the DataFrame's index is datetime
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        raise ValueError("The DataFrame index must be a datetime object.")
    
    # Check if the table exists
    table_name = table_name.lower()
    cur.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
    """)
    table_exists = cur.fetchone()[0]

    # If the table does not exist, create it
    if not table_exists:
        # Create table statement based on the DataFrame's schema
        create_table_query = f"""
        CREATE TABLE {table_name} (
            datetime_index TIMESTAMP PRIMARY KEY,
            {', '.join([f"{col} {dtype}" for col, dtype in zip(df.columns, df.dtypes.replace({'object': 'TEXT', 'int64': 'INTEGER', 'float64': 'REAL'}))])}
        );
        """
        cur.execute(create_table_query)
        conn.commit()

    # Insert the data into the table with conflict handling
    for i, row in df.iterrows():
        insert_query = f"""
        INSERT INTO {table_name} (datetime_index, {', '.join(df.columns)})
        VALUES (%s, {', '.join(['%s'] * len(row))})
        ON CONFLICT (datetime_index) DO NOTHING;  -- Prevent duplicates
        """
        cur.execute(insert_query, (i,) + tuple(row))

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()



def load_from_csv(ticker):
    df = pd.read_csv(ticker + '.csv', index_col = 0)
    df.index = pd.to_datetime(df.index)
    df = df.iloc[:-3]
    return df

def csv_to_database():
    df = load_from_csv('XLB')
    save_to_db(df, 'XLB')


def ticker_csv_to_database():
    tickers = ['XLC', 'XLF','XLI', 'XLK','XLP','XLRE','XLU','XLV','XLY', 'S&P 500']
    for ticker in tickers:
        df = pd.read_csv(ticker + '.csv', index_col = 0)
        df.index = pd.to_datetime(df.index)
        df = df.iloc[:-3]
        
        ticker_name = ticker if ticker != 'S&P 500' else 'SP500'
        save_to_db(df, ticker_name)

def get_table(table_name):
    # Connect to the database
    conn = get_db_connection()
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, conn)
    
    conn.close()
    return df


if __name__ == '__main__':
    ticker_csv_to_database()