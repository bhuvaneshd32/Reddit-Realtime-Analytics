from config import Config
import praw
import keys

class RedditConnector:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=keys.client_id,
            client_secret=keys.client_secret,
            user_agent=keys.user_agent
        )

    def get_subreddit_posts(self, subreddit_name, limit=10):
        """Fetch posts from a subreddit's hot section."""
        subreddit = self.reddit.subreddit(subreddit_name)
        return subreddit.hot(limit=limit)

    def get_post_comments(self, post_id):
        """Fetch comments for a specific post."""
        submission = self.reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)
        return submission.comments

    def search_posts(self, query, limit=50):
        """Search Reddit for posts matching query across all subreddits."""
        try:
            return self.reddit.subreddit("all").search(query, limit=limit)
        except Exception as e:
            raise Exception(f"Search failed: {e}")