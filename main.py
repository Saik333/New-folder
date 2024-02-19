import os
import yaml
import sys
from box import Box
from hash_check.generate_hash import generate_hash_value
from util.snowpark_connect import snowflake_connection
from util.open_ai import open_ai
from duplicate_check.find_duplicates import find_duplicate_values
from duplicate_check.remove_duplicates import remove_duplicate_values
from null_checks.find_nulls import find_null_values
from null_checks.replace_nulls import replace_null_values
from pattern_match.pattern_match import email_pattern_check
from range_check.range_check import min_max_range_check

session = snowflake_connection()
client = open_ai()

for arg in sys.argv[1:]:
    key, value = arg.split("=")
    os.environ[key] = value

# def refine_the_query(query: str) -> str:
#     words = query.split()
#     words[0] = "SELECT"
#     return " ".join(words)


def hash_value() -> str:
    count = 1
    source_hash = ""
    target_hash = ""
    while count <= 5:
        generate_hash = generate_hash_value(os.environ.get("source"), client, session)
        # if not generate_hash.strip().upper().startswith("SELECT"):
        #     generate_hash = refine_the_query(generate_hash)
        #     print("query after refining: ", generate_hash)
        source_hash_sql = generate_hash
        target_hash_sql = source_hash_sql.replace(
            f"{os.environ.get('source').lower()}", f"{os.environ.get('target').lower()}"
        )

        print(f"source_sql: {source_hash_sql}")
        print(f"target_sql: {target_hash_sql}")
        try:
            source_hash = session.sql(source_hash_sql).collect()[0][0]
            target_hash = session.sql(target_hash_sql).collect()[0][0]
            if source_hash == target_hash:
                print(
                    f"""source hash = {source_hash}
target hash = {target_hash}"""
                )
                print("PASS: Hash Validation successful")
                return "PASS: Hash Validation successful"
            else:
                print(
                    f"""Fail: Hash validation failed)
source hash = {source_hash}
target hash = {target_hash}"""
                )
                return "Fail: Hash validation failed"

        except Exception as e:
            if count == 5:
                print(
                    "Unable to find the proper statement after 5 attempts raising the error"
                )
                raise
            else:
                print(f"failure in finding hash in attempt: {count}, retrying...")
                count += 1


def duplicates():
    count = 1
    while count <= 5:
        try:
            key_columns, duplicates_query = find_duplicate_values(
                os.environ.get("target"), session, client
            )
            print("duplicates_query:", duplicates_query)
            print("key columns: ", key_columns)
            # return
            # if not duplicates_query.strip().upper().startswith("SELECT"):
            #     duplicates_query = refine_the_query(duplicates_query)
            #     print("query after refining:", duplicates_query)
            duplicates = session.sql(duplicates_query.replace(";", ""))
            number_of_duplicates = int(duplicates.count())
            if number_of_duplicates > 0:
                print(
                    f"{number_of_duplicates} duplicate(s) found, trying to remove them"
                )
                counter = 1
                while counter <= 5:
                    try:
                        remove_deplicates_query = remove_duplicate_values(
                            os.environ.get("target"), key_columns, session, client
                        )
                        print("duplicates_remove_query:", remove_deplicates_query)
                        session.sql(remove_deplicates_query).collect()
                        print("duplicates removed")
                        return
                    except:
                        if counter == 5:
                            print(
                                "Unable to perform operation to remove duplicates after 5 attempts raising the error"
                            )
                            raise
                        else:
                            print(
                                f"failure in removing duplicates attempt: {counter}, retrying..."
                            )
                            count += 1

            else:
                print("No duplicates found")
            return
        except:
            if count == 5:
                print(
                    "Unable to perform operation to find duplicates after 5 attempts raising the error"
                )
                raise
            else:
                print(f"failure in finding duplicates in attempt: {count}, retrying...")
                count += 1


def missing_values():
    count = 1
    while count <= 5:
        try:
            key_columns, nulls_query = find_null_values(
                os.environ.get("target"), session, client
            )
            print("nulls_query:", nulls_query)
            print("key columns: ", key_columns)
            nulls = session.sql(nulls_query.replace(";", ""))
            number_of_nulls = int(nulls.count())
            if number_of_nulls > 0:
                print(
                    f"{number_of_nulls} null(s) found, trying to replace them with custom value '0'"
                )
                counter = 1
                while counter <= 5:
                    try:
                        replace_nulls_query = replace_null_values(
                            os.environ.get("target"), key_columns, session, client
                        )
                        print("null_replace_query:", replace_nulls_query)
                        session.sql(replace_nulls_query).collect()
                        print("nulls replaced")
                        return
                    except:
                        if counter == 5:
                            print(
                                "Unable to perform operation to replace nulls after 5 attempts raising the error"
                            )
                            raise
                        else:
                            print(
                                f"failure in replacing nulls in attempt: {counter}, retrying..."
                            )
                            count += 1

            else:
                print("No nulls found")
            return
        except:
            if count == 5:
                print(
                    "Unable to perform operation to find nulls after 5 attempts raising the error"
                )
                raise
            else:
                print(f"failure in finding nulls in attempt: {count}, retrying...")
                count += 1


def pattern_check(config: dict):
    count = 1
    while count <= 5:
        try:
            pattern_check_query = email_pattern_check(
                os.environ.get("target"),
                client,
                config.pattern_check.column_name,
                config.pattern_check.pattern,
            )
            print("pattern_query:", pattern_check_query)
            mismatches = session.sql(pattern_check_query.replace(";", ""))
            number_of_mimatches = int(mismatches.count())
            if number_of_mimatches > 0:
                print(f"FAIL: {mismatches} pattern mismatch(s) found")
                mismatches.show()

            else:
                print("No pattern mismatches found")
            return
        except:
            if count == 5:
                print(
                    "Unable to perform operation to find pattern mismatches after 5 attempts raising the error"
                )
                raise
            else:
                print(
                    f"failure in finding pattern mismatches in attempt: {count}, retrying..."
                )
                count += 1


def range_check(config: dict):
    count = 1
    while count <= 5:
        try:
            range_check_query = min_max_range_check(
                os.environ.get("target"),
                client,
                config.range_check.column_name,
                config.range_check.min_value,
                config.range_check.max_value,
            )
            print("range_check_query:", range_check_query)
            mismatches = session.sql(range_check_query.replace(";", ""))
            number_of_mimatches = int(mismatches.count())
            if number_of_mimatches > 0:
                print(f"FAIL: {mismatches} range mismatch(s) found")
                mismatches.show()

            else:
                print("No range mismatches found")
            return
        except:
            if count == 5:
                print(
                    "Unable to perform operation to find range mismatches after 5 attempts raising the error"
                )
                raise
            else:
                print(
                    f"failure in finding range mismatches in attempt: {count}, retrying..."
                )
                count += 1


def run():
    with open(f"{os.getcwd()}/config/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    config = Box(config)
    os.environ["source"] = config.source_table
    os.environ["target"] = config.target_table
    if os.environ.get("check") in ("hash_validation", "all"):
        hash_value()
    if os.environ.get("check") in ("duplicate_validation", "all"):
        duplicates()
    if os.environ.get("check") in ("null_validation", "all"):
        missing_values()
    if os.environ.get("check") in ("pattern_validation", "all"):
        pattern_check(config)
    if os.environ.get("check") in ("range_validation", "all"):
        range_check(config)
    # duplicates()
    # missing_values()


if __name__ == "__main__":
    run()

# python main.py check=hash_validation
# python main.py check=duplicate_validation
# python main.py check=null_validation
