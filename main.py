from generate_hash import generate_hash_value
from snowpark_connect import snowflake_connection
import os
import sys

for arg in sys.argv[1:]:
    key, value = arg.split("=")
    os.environ[key] = value

# print(os.environ.get("source"))
# print(os.environ.get("target"))

def hash_value() -> str:
    count = 1
    source_hash = ""
    target_hash = ""
    while count <= 5:
        generate_hash = generate_hash_value(os.environ.get("source"))
        source_hash_sql = generate_hash
        target_hash_sql = source_hash_sql.replace(f"{os.environ.get('source').lower()}", f"{os.environ.get('target').lower()}")

        print("source_sql:", source_hash_sql)
        print("target_sql:", target_hash_sql)
        session = snowflake_connection()
        try:
            source_hash = session.sql(source_hash_sql).collect()[0][0]
            target_hash = session.sql(target_hash_sql).collect()[0][0]
            if source_hash == target_hash:
                print(f"""source hash = {source_hash}
target hash = {target_hash}""")
                return "PASS: Hash Validation successful" 
            else:
                return f"""FAIL: Both the hash values are not equal 
source hash = {source_hash}
target hash = {target_hash}"""
            
        except Exception as e:
            if count == 5:
                print("Unable to find the proper statement after 5 attempts raising the error")
                raise
            else:
                print(f"failure in attempt: {count}, retrying...")
                count += 1

print(hash_value())



