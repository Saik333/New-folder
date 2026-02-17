import os

print(os.environ["SNOWFLAKE_TOKEN"])
import snowflake.connector


conn = snowflake.connector.connect(
    account="ranaqbw-np18722",
    user="5de13f72-cb16-47ec-8487-275ec596758c",
    authenticator="oauth",
    token=os.environ["SNOWFLAKE_TOKEN"],
)
print
print(conn)
