import snowflake.connector
import os

conn = snowflake.connector.connect(
    account="ranaqbw-np18722",
    user="saikumar",
    authenticator="oauth",
    token=os.environ["SNOWFLAKE_TOKEN"],
)
