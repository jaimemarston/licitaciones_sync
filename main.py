# main.py
from api import fetch_data_from_api
from process import insert_data_into_db

if __name__ == "__main__":
    records = fetch_data_from_api()
    if records:
        insert_data_into_db(records)
