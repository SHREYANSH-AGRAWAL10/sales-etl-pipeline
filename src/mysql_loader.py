import mysql.connector
import logging

from sales_etl import run_etl, setup_logging


# -----------------------------
# CREATE MYSQL CONNECTION
# -----------------------------

def create_connection():

    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Qwerty@001",
        database="sales_db"
    )

    # print("Connected to MySQL successfully!")
    logging.info(f"connected to MySQL successfully!")

    return connection


# -----------------------------
# LOAD DATA
# -----------------------------

def load_data(df, connection):

    cursor = connection.cursor()

    try:

        insert_query = """
            INSERT IGNORE INTO sales (
                order_id,
                customer,
                product,
                category,
                quantity,
                price,
                discount,
                total_amount,
                final_amount,
                city,
                order_date
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s
            )
        """


        # Prepare data for bulk insert

        data = []

        for _, row in df.iterrows():

            data.append(
                (
                    row["order_id"],
                    row["customer"],
                    row["product"],
                    row["category"],
                    row["quantity"],
                    row["price"],
                    row["discount"],
                    row["total_amount"],
                    row["final_amount"],
                    row["city"],
                    row["order_date"]
                )
            )


        # Insert all rows at once

        cursor.executemany(
            insert_query,
            data
        )


        inserted_count = cursor.rowcount

        skipped_count = (
            len(data) - inserted_count
        )


        connection.commit()


        logging.info(
            f"Records processed: {len(data)}"
        )

        logging.info(
            f"Records inserted: {inserted_count}"
        )

        logging.info(
            f"Records skipped: {skipped_count}"
        )


    except Exception as e:

        connection.rollback()

        logging.error(
            f"Error while loading data: {e}"
        )

        raise


    finally:

        cursor.close()

# -----------------------------
# RUN PIPELINE
# -----------------------------

if __name__ == "__main__":

    setup_logging()

    df = run_etl()

    connection = create_connection()

    try:

        load_data(df, connection)

    finally:

        connection.close()