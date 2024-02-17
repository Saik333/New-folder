from snowflake.snowpark import Session
from os import environ
from dotenv import load_dotenv

load_dotenv()


def snowflake_connection():
    connection_parameters = {
        "account": "assjfrj-rw85836",
        "user": "saikumar",
        "password": f"{environ.get('snowflake_password')}",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "SAI",
        "schema": "PUBLIC",
    }
    try:
        print("Connecting to Snowflake")
        session = Session.builder.configs(connection_parameters).create()
        print("Connection to snowflake is successful")

    except Exception as e:
        print("connection failed")
        raise

    return session
