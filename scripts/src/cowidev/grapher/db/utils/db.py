import os
import pymysql
from dotenv import load_dotenv


load_dotenv()

@property
def connection():
    # Connect to the database
    pymysql.connect(
        db=os.getenv('DB_NAME'),
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT')),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        charset='utf8mb4',
        autocommit=False
    ) # requires .commit(), so everything is implicitly a transaction
