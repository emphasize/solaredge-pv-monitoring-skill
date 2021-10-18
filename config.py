from sqlalchemy import Column, Integer, Float, DateTime

SE_CREDENTIALS = {"apiKey": "XXXXXXXXX",
                  "siteID": "12345"}
SQL_CREDENTIALS = {"user": "user",
                   "password": "password",
                   "host": "ip:port"
                   }
SQL_SSL = {"CA": "/path/to/ca.pem",
           "CKEY": "/path/to/client-key.pem",
           "CCERT": "/path/to/client-cert.pem"}
#todo make table renaming possible
#to add your own colums, append them after the last table entry

SQL_DB_SCHEMAS = {"energy": {'id': Column(Integer, primary_key=True),
                             'Time': Column(DateTime),
                             'Production': Column(Float),
                             'FeedIn': Column(Float),
                             'SelfConsumption': Column(Float),
                             'Purchased': Column(Float),
                             'Consumption': Column(Float)}
                  }
# The Solaredge-Api corresponding to a table
SE_API_TABLE = {"energyDetails": "energy"}

SQL_TABLES_PREFIX = {"energy": "solar"}

# creates new tables in DAY, WEEK or MONTH -ly timespans
# SQL_SPLIT_TABLE_TIME = {"energy": "year"}
# if table is not mentioned it's a continuous table append
SQL_SPLIT_TABLE_TIME = {}

# derivative tables naming convention:
# the derivative table (table inherits schema of basetable)
# has to be named with the basename in front
SQL_DERIVATIVE_TABLES = {"energy": ["day", "week", "month", "year"]}
