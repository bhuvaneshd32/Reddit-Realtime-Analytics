import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Reddit API credentials
    REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
    REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
    REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'RedditAnalytics/1.0')
   
    # HBase configuration
    HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
    HBASE_PORT = int(os.getenv('HBASE_PORT', 9090))
    HBASE_TABLE_PREFIX = os.getenv('HBASE_TABLE_PREFIX', 'reddit_')