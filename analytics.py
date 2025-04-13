from hbase_manager import HBaseManager
import pandas as pd
from textblob import TextBlob
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import networkx as nx

class RedditAnalytics:
    def __init__(self, hbase_manager):
        self.hbase = hbase_manager

    def get_top_subreddits(self, limit=5):
        df = self.to_dataframe("posts")
        if df.empty:
            return []
        return df['post_data:subreddit'].value_counts().head(limit).items()

    def get_top_authors(self, limit=5):
        df = self.to_dataframe("posts")
        if df.empty:
            return []
        return df['post_data:author'].value_counts().head(limit).items()

    def get_engagement_metrics(self):
        df = self.to_dataframe("posts")
        if df.empty:
            return {'avg_score': 0, 'avg_comments': 0}
        return {
            'avg_score': df['metrics:score'].astype(float).mean(),
            'avg_comments': df['metrics:num_comments'].astype(float).mean()
        }

    def get_sentiment_trends(self):
        """Fine-grained sentiment analysis with TextBlob."""
        posts_df = self.to_dataframe("posts")
        comments_df = self.to_dataframe("comments")
        sentiment = {'positive': 0, 'neutral': 0, 'negative': 0}
        
        if posts_df.empty and comments_df.empty:
            return sentiment
        
        for df in [posts_df, comments_df]:
            text_col = 'post_data:title' if df is posts_df else 'comment_data:text'
            if text_col in df.columns:
                for text in df[text_col].dropna():
                    blob = TextBlob(str(text))
                    polarity = blob.sentiment.polarity
                    if polarity > 0:
                        sentiment['positive'] += 1
                    elif polarity < 0:
                        sentiment['negative'] += 1
                    else:
                        sentiment['neutral'] += 1
        
        total = sum(sentiment.values())
        if total > 0:
            sentiment = {k: v/total for k, v in sentiment.items()}
        return sentiment

    def get_topics(self, num_topics=5):
        """Extract topics from post titles using LDA."""
        df = self.to_dataframe("posts")
        if 'post_data:title' not in df.columns or df.empty:
            return []
        
        vectorizer = CountVectorizer(stop_words='english', max_df=0.95, min_df=2)
        X = vectorizer.fit_transform(df['post_data:title'].dropna())
        if X.shape[0] == 0:
            return []
        
        lda = LatentDirichletAllocation(n_components=num_topics, random_state=42)
        lda.fit(X)
        
        feature_names = vectorizer.get_feature_names_out()
        topics = []
        for topic_idx, topic in enumerate(lda.components_):
            top_words = [feature_names[i] for i in topic.argsort()[:-6:-1]]
            topics.append(f"Topic {topic_idx+1}: {', '.join(top_words)}")
        return topics

    def get_user_influence(self, limit=5):
        """Calculate user influence based on scores and network centrality."""
        posts_df = self.to_dataframe("posts")
        comments_df = self.to_dataframe("comments")
        if posts_df.empty and comments_df.empty:
            return []
        
        # Score-based influence
        influence = {}
        if not posts_df.empty:
            for _, row in posts_df.iterrows():
                author = row['post_data:author']
                score = float(row['metrics:score'])
                if author:
                    influence[author] = influence.get(author, 0) + score
        
        if not comments_df.empty:
            for _, row in comments_df.iterrows():
                author = row['comment_data:author']
                score = float(row['metrics:score'])
                if author:
                    influence[author] = influence.get(author, 0) + score * 0.5  # Comments weighted less
        
        # Network centrality
        G = nx.DiGraph()
        if not comments_df.empty:
            for _, row in comments_df.iterrows():
                author = row['comment_data:author']
                parent_id = row['comment_data:parent_id']
                if author and parent_id and parent_id.startswith('t1_'):
                    parent_author = comments_df[comments_df['comment_data:id'] == parent_id[3:]]['comment_data:author']
                    if not parent_author.empty:
                        G.add_edge(author, parent_author.iloc[0])
        
        if G.nodes:
            centrality = nx.betweenness_centrality(G)
            for node in centrality:
                influence[node] = influence.get(node, 0) + centrality[node] * 100
        
        return sorted(influence.items(), key=lambda x: x[1], reverse=True)[:limit]

    def get_temporal_trends(self):
        """Posting frequency by day and hour."""
        df = self.to_dataframe("posts")
        if 'post_data:created_utc' not in df.columns or df.empty:
            return {}
        
        df['datetime'] = pd.to_datetime(df['post_data:created_utc'], unit='s')
        df['day'] = df['datetime'].dt.day_name()
        df['hour'] = df['datetime'].dt.hour
        
        trends = {
            'by_day': df['day'].value_counts().to_dict(),
            'by_hour': df['hour'].value_counts().to_dict()
        }
        return trends

    def to_dataframe(self, table_name):
        table = self.hbase.connection.table(f"reddit_{table_name}")
        rows = []
        for key, data in table.scan():
            row = {'row_key': key.decode()}
            for col, value in data.items():
                row[col.decode()] = value.decode()
            rows.append(row)
        return pd.DataFrame(rows)