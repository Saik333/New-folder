import os

print(os.environ["SNOWFLAKE_TOKEN"])
import snowflake.connector


conn = snowflake.connector.connect(
    account="ranaqbw-np18722",
    user="b60392b3-a8da-4fd0-8302-9ffe6a612dba",
    authenticator="oauth",
    token=os.environ["SNOWFLAKE_TOKEN"],
    role="TEST_ROLE",
)
print
print(conn)

a = conn.cursor().execute("SELECT CURRENT_USER(), CURRENT_ROLE();")
print(a.fetchone())
