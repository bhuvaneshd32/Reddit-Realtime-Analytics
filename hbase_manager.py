from happybase import Connection
from config import Config
import json

class HBaseManager:
    def __init__(self):
        self.connection = Connection(host=Config.HBASE_HOST, port=Config.HBASE_PORT)
        self.ensure_tables()

    def ensure_tables(self):
        tables = [Config.HBASE_TABLE_PREFIX + 'posts', Config.HBASE_TABLE_PREFIX + 'comments']
        for table in tables:
            if table.encode() not in self.connection.tables():
                self.connection.create_table(
                    table,
                    {'post_data': dict(max_versions=1), 'metrics': dict(max_versions=1),
                     'comment_data': dict(max_versions=1)}
                )

    def store_post(self, post):
        table = self.connection.table(Config.HBASE_TABLE_PREFIX + 'posts')
        row_key = f"post_{post['id']}"
        data = {
            b'post_data:id': str(post['id']).encode(),
            b'post_data:title': post['title'].encode(),
            b'post_data:selftext': post.get('selftext', '').encode(),
            b'post_data:subreddit': post['subreddit'].encode(),
            b'post_data:author': post.get('author', '').encode(),
            b'post_data:created_utc': str(post['created_utc']).encode(),
            b'metrics:score': str(post.get('score', 0)).encode(),
            b'metrics:ups': str(post.get('ups', 0)).encode(),
            b'metrics:downs': str(post.get('downs', 0)).encode(),
            b'metrics:num_comments': str(post.get('num_comments', 0)).encode()
        }
        table.put(row_key.encode(), data)

    def store_comment(self, comment, post_id):
        table = self.connection.table(Config.HBASE_TABLE_PREFIX + 'comments')
        row_key = f"comment_{comment['id']}_post_{post_id}"
        data = {
            b'comment_data:id': str(comment['id']).encode(),
            b'comment_data:post_id': str(post_id).encode(),
            b'comment_data:text': comment['body'].encode(),
            b'comment_data:author': comment.get('author', '').encode(),
            b'comment_data:created_utc': str(comment['created_utc']).encode(),
            b'comment_data:parent_id': comment.get('parent_id', '').encode(),
            b'metrics:score': str(comment.get('score', 0)).encode(),
            b'metrics:ups': str(comment.get('ups', 0)).encode(),
            b'metrics:downs': str(comment.get('downs', 0)).encode()
        }
        table.put(row_key.encode(), data)