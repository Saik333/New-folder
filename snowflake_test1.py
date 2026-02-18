import os

print(os.environ["SNOWFLAKE_TOKEN"])
import snowflake.connector


conn = snowflake.connector.connect(
    account="ranaqbw-np18722",
    user="repo:Saik333/New-folder:ref:refs/heads/main",
    authenticator="oauth",
    token=os.environ["SNOWFLAKE_TOKEN"],
    role="TEST_ROLE",
)
print
print(conn)

a = conn.cursor().execute("SELECT CURRENT_USER(), CURRENT_ROLE();")
print(a.fetchone())

conn.cursor().execute("USE ROLE TEST_ROLE1;")
a = conn.cursor().execute("SELECT CURRENT_USER(), CURRENT_ROLE();")
print(a.fetchone())
