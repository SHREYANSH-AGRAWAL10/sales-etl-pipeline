import mysql.connector
import logging
import os

from dotenv import load_dotenv
load_dotenv()

from sales_etl import run_etl, setup_logging


# -----------------------------
# CREATE MYSQL CONNECTION
# -----------------------------

def create_connection():

    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
        # database = "Testing_error"
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

        # testing load failure
        # raise Exception("Testing load failure")

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

        return inserted_count, skipped_count

    except Exception as e:

        connection.rollback()

        logging.error(
            f"Error while loading data: {e}"
        )

        raise


    finally:

        cursor.close()

# -----------------------------
# Loading for ETL run
# -----------------------------

def log_etl_run (connection, records_processed, records_inserted , records_skipped , status , error_message=None):

    cursor = connection.cursor()

    try:

        query = """
            INSERT INTO etl_load_log (
                pipeline_name,
                records_processed,
                records_inserted,
                records_skipped,
                run_status,
                error_message
            )
            VALUES (
                %s, %s, %s, %s, %s, %s
            )
        """

        cursor.execute(
            query,
            ("Sales ETL", records_processed, records_inserted , records_skipped , status , error_message)
        )

        connection.commit()

        logging.info(
            f"ETL run logged with status: {status}"
        )


    except Exception as e:

        connection.rollback()

        logging.error(
            f"Error while logging ETL run: {e}"
        )

        raise


    finally:

        cursor.close()

# -----------------------------
# RUN PIPELINE
# -----------------------------

if __name__ == "__main__":

    setup_logging()

    connection = None

    try:

        df = run_etl()

        connection = create_connection()

        inserted_count, skipped_count = load_data(
            df,
            connection
        )

        log_etl_run(
            connection,
            len(df),
            inserted_count,
            skipped_count,
            "SUCCESS"
        )

    except Exception as e:

        logging.error(
            f"ETL pipeline failed: {e}"
        )

        if "df" in locals():
            records_processed = len(df)
        else:
            records_processed = 0
        
        log_etl_run(
                connection,
                records_processed,
                0,
                0,
                "FAILED",
                str(e)
        )

        raise

    finally:

        if connection:
            connection.close()