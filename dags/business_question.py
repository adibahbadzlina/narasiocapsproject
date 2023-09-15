import pendulum
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer


local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'Adibah Badzlina',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 9, 1, tzinfo=local_tz),
    'retries': 1,
}

dag =  DAG(
    'business_question',
    default_args=default_args,
    description='Task to answer business questions',
    schedule_interval='0 9 * * *',
    catchup=False,
)

daily_earnings = GenericTransfer(
    task_id = 'daily_earnings',
    preoperator = [
        "DROP TABLE IF EXISTS question1",
        """
        CREATE TABLE question1 (
        
        "InvoiceDate" TIMESTAMP NOT NULL,
        "DailyEarnings" NUMERIC(10,2) NOT NULL
    
        )
        """
    ],
    sql = """
    SELECT *
    FROM (
            SELECT "InvoiceDate", SUM("TotalPrice") as "DailyEarnings"
            FROM "artist_revenue"
            GROUP BY "InvoiceDate"
    ) as a
    """,
    destination_table = 'question1',
    source_conn_id = 'destination',
    destination_conn_id = 'final_dest',
    dag=dag,
)

most_productive_artist = GenericTransfer(
    task_id = 'most_productive_artist',
     preoperator = [
        "DROP TABLE IF EXISTS question2",
        """
        CREATE TABLE question2 (
        
        "ArtistId" INT NOT NULL,
        "ArtistName" VARCHAR,
        "TotalTrack" NUMERIC(10,2) NOT NULL
        )
        """
    ],
    sql = """
    SELECT "ArtistId", "ArtistName", COUNT("TrackId") as "TotalTrack"
    FROM "songs"
    GROUP BY "GenreName", "ArtistId", "ArtistName"
    ORDER BY "TotalTrack" DESC
    """,
    destination_table = 'question2',
    source_conn_id = 'destination',
    destination_conn_id = 'final_dest',
    dag=dag,
)

artist_earnings = GenericTransfer(
    task_id = 'artist_earnings',
    preoperator = [
        "DROP TABLE IF EXISTS question3",
        """
        CREATE TABLE question3 (
        "ArtistId" INT NOT NULL,
        "ArtistName" VARCHAR,
        "Earnings" NUMERIC(10,2) NOT NULL
        )
        """
    ],
    sql = """
    SELECT "ArtistId", "ArtistName", SUM("TotalPrice") as "Earnings"
    FROM "artist_revenue"
    GROUP BY "ArtistId", "ArtistName"
    ORDER BY "Earnings" DESC
    """,
    destination_table = 'question3',
    source_conn_id = 'destination',
    destination_conn_id = 'final_dest',

    dag=dag,
)

city_with_most_purchases = GenericTransfer(
    task_id = 'city_with_most_purchases',
     preoperator = [
        "DROP TABLE IF EXISTS question4",
        """
        CREATE TABLE question4 (
        
        "City" VARCHAR,
        "TotalPurchases" NUMERIC(10,2) NOT NULL
        
    
        )
        """
    ],
    sql = """
    SELECT "City", COUNT("InvoiceId") as "TotalPurchases"
    FROM "transactions"
    GROUP BY "City"
    ORDER BY "TotalPurchases" DESC
    """,
    destination_table = 'question4',
    source_conn_id = 'destination',
    destination_conn_id = 'final_dest',

    dag=dag,
)

daily_earnings >> most_productive_artist >> artist_earnings >> city_with_most_purchases