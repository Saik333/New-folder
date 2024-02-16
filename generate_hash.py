from snowpark_connect import snowflake_connection
from open_ai import open_ai
import json


def generate_hash_value(table_name: str) -> str:
    session = snowflake_connection()
    columns = session.sql(
        f"select ARRAY_AGG(OBJECT_CONSTRUCT(*)) from(SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns where table_name = '{table_name}' order by ordinal_position);"
    ).collect()[0][0]
    result_dict = {}
    columns = json.loads(columns)

    for column_info in columns:
        result_dict[column_info["COLUMN_NAME"]] = column_info["DATA_TYPE"]
    NUM = []
    TEX = []

    for key, value in result_dict.items():
        if value == "NUMBER":
            NUM.append(key)
        else:
            TEX.append(key)
    # print(NUM, TEX)
    hash_value_sql = open_ai(
        f"""please generate a sql select statement by making sum of numeric colums and count of non numeric from table emp
        numeric columns are {NUM} and non numeric columns are {TEX}  and finally make a full sum all of these values and put it in md5() function as md5(value)
        SQL statement doesn't require to find if they numeric just a select statement without any condictions"""
    )
    return hash_value_sql
