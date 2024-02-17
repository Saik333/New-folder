from generate_hash import generate_hash_value
from snowpark_connect import snowflake_connection
from find_duplicates import find_duplicate_values
from remove_duplicates import remove_duplicate_values
from find_nulls import find_null_values
from replace_nulls import replace_null_values
import os
import sys

for arg in sys.argv[1:]:
    key, value = arg.split("=")
    os.environ[key] = value

session = snowflake_connection()

# def refine_the_query(query: str) -> str:
#     words = query.split()
#     words[0] = "SELECT"
#     return " ".join(words)

def hash_value() -> str:
    count = 1
    source_hash = ""
    target_hash = ""
    while count <= 5:
        generate_hash = generate_hash_value(os.environ.get("source"))
        # if not generate_hash.strip().upper().startswith("SELECT"):
        #     generate_hash = refine_the_query(generate_hash)
        #     print("query after refining: ", generate_hash)
        source_hash_sql = generate_hash
        target_hash_sql = source_hash_sql.replace(f"{os.environ.get('source').lower()}", f"{os.environ.get('target').lower()}")

        print(f"source_sql: {source_hash_sql}")
        print(f"target_sql: {target_hash_sql}")
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
                print(f"failure in finding hash in attempt: {count}, retrying...")
                count += 1

def duplicates():
    count = 1
    while count <= 5:
        try:
            key_columns, duplicates_query = find_duplicate_values(os.environ.get("target"))
            print("duplicates_query:", duplicates_query)
            print("key columns: ", key_columns)
            # return
            # if not duplicates_query.strip().upper().startswith("SELECT"):
            #     duplicates_query = refine_the_query(duplicates_query)
            #     print("query after refining:", duplicates_query)
            duplicates = session.sql(duplicates_query)
            number_of_duplicates = int(duplicates.count())
            if number_of_duplicates > 0:
                print(f"{number_of_duplicates} duplicate(s) found, trying to remove them")
                counter = 1
                while counter <= 5:
                    try:
                        remove_deplicates_query = remove_duplicate_values(os.environ.get("target"), key_columns)
                        print("duplicates_remove_query:", remove_deplicates_query)
                        session.sql(remove_deplicates_query).collect()
                        print("duplicates removed")
                        return
                    except:
                        if counter == 5:
                            print("Unable to perform operation to remove duplicates after 5 attempts raising the error")
                            raise
                        else:
                            print(f"failure in removing duplicates attempt: {counter}, retrying...")
                            count += 1

            else:
                print("No duplicates found")
            return
        except:
            if count == 5:
                print("Unable to perform operation to find duplicates after 5 attempts raising the error")
                raise
            else:
                print(f"failure in finding duplicates in attempt: {count}, retrying...")
                count += 1

def missing_values():
    count = 1
    while count <= 5:
        try:
            key_columns, nulls_query = find_null_values(os.environ.get("target"))
            print("nulls_query:", nulls_query)
            print("key columns: ", key_columns)
            nulls = session.sql(nulls_query)
            number_of_nulls = int(nulls.count())
            if number_of_nulls > 0:
                print(f"{number_of_nulls} null(s) found, trying to replace them with custom value '0'")
                counter = 1
                while counter <= 5:
                    try:
                        replace_nulls_query = replace_null_values(os.environ.get("target"), key_columns)
                        print("null_replace_query:", replace_nulls_query)
                        session.sql(replace_nulls_query).collect()
                        print("nulls replaced")
                        return
                    except:
                        if counter == 5:
                            print("Unable to perform operation to replace nulls after 5 attempts raising the error")
                            raise
                        else:
                            print(f"failure in replacing nulls in attempt: {counter}, retrying...")
                            count += 1

            else:
                print("No nulls found")
            return
        except:
            if count == 5:
                print("Unable to perform operation to find nulls after 5 attempts raising the error")
                raise
            else:
                print(f"failure in finding nulls in attempt: {count}, retrying...")
                count += 1

print(hash_value())
duplicates()
missing_values()
# python main.py source=EMP target=EMP_TARGET

