import sys
import os
import streamlit as st
import prawcore
import logging
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to sys.path
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from reddit_connector import RedditConnector
    from hbase_manager import HBaseManager
    from analytics import RedditAnalytics
    from visualization import RedditVisualizer
except ModuleNotFoundError as e:
    st.error(f"Import error: {e}")
    logger.error(f"Import error: {e}")
    raise

def fetch_and_store_data(reddit, hbase, subreddit_name=None, query=None, post_id=None, limit=50):
    """Fetch and store Reddit data by subreddit, search query, or post ID."""
    try:
        if sum(1 for x in [subreddit_name, query, post_id] if x) != 1:
            st.error("Please provide exactly one of: subreddit name, search query, or post ID.")
            logger.error("Invalid input: multiple or no inputs provided")
            return False

        source = (
            f"r/{subreddit_name}" if subreddit_name else
            f"search: {query}" if query else
            f"post: {post_id}"
        )
        st.write(f"Fetching and storing data from {source}...")
        logger.info(f"Fetching data from {source}")
        progress_bar = st.progress(0)
        
        posts = []
        if subreddit_name:
            posts = list(reddit.get_subreddit_posts(subreddit_name, limit=limit))
        elif query:
            posts = list(reddit.search_posts(query, limit=limit))
        else:
            posts = [reddit.get_post_by_id(post_id)] if post_id else []
        
        total_posts = len(posts)
        logger.info(f"Fetched {total_posts} posts")
        if total_posts == 0:
            st.warning(f"No posts found in {source}.")
            logger.warning(f"No posts found in {source}")
            return False
        
        for i, post in enumerate(posts):
            post_dict = {
                'id': post.id,
                'title': post.title,
                'selftext': post.selftext or '',
                'subreddit': post.subreddit.display_name,
                'author': post.author.name if post.author else '[deleted]',
                'created_utc': post.created_utc,
                'score': post.score,
                'ups': post.ups,
                'downs': post.downs,
                'num_comments': post.num_comments,
                'url': post.url
            }
            hbase.store_post(post_dict)
            st.write(f"Stored post: {post.title[:50]}...")
            logger.info(f"Stored post: {post.id}")
            try:
                comments = reddit.get_post_comments(post.id)
                comment_count = 0
                for comment in comments:
                    comment_dict = {
                        'id': comment.id,
                        'body': comment.body,
                        'post_id': post.id,
                        'author': comment.author.name if comment.author else '[deleted]',
                        'created_utc': comment.created_utc,
                        'score': comment.score,
                        'ups': comment.ups,
                        'downs': comment.downs,
                        'parent_id': comment.parent_id
                    }
                    hbase.store_comment(comment_dict, post.id)
                    comment_count += 1
                st.write(f"Stored {comment_count} comments for post {post.id}")
                logger.info(f"Stored {comment_count} comments for post {post.id}")
            except Exception as e:
                st.write(f"Error storing comments for post {post.id}: {e}")
                logger.error(f"Error storing comments for post {post.id}: {e}")
            progress_bar.progress((i + 1) / total_posts)
        st.write(f"Total posts stored: {total_posts}")
        return True
    
    except prawcore.exceptions.NotFound:
        st.error(f"{'Subreddit' if subreddit_name else 'Search' if query else 'Post'} not found: {source}.")
        logger.error(f"Not found: {source}")
        return False
    except prawcore.exceptions.Forbidden:
        st.error(f"Access to {source} is restricted (private or banned).")
        logger.error(f"Forbidden: {source}")
        return False
    except Exception as e:
        st.error(f"Error fetching/storing data: {e}")
        logger.error(f"Fetch/store error: {e}", exc_info=True)
        return False

def perform_analysis(analytics):
    """Perform extended analysis."""
    try:
        st.write("\nPerforming analysis...")
        logger.info("Starting analysis")
        posts_df = analytics.to_dataframe("posts")
        comments_df = analytics.to_dataframe("comments")
        st.write(f"Posts in HBase: {len(posts_df)}")
        st.write(f"Comments in HBase: {len(comments_df)}")
        logger.info(f"Posts in HBase: {len(posts_df)}, Comments: {len(comments_df)}")
        
        top_subreddits = analytics.get_top_subreddits()
        st.write(f"Top subreddits: {list(top_subreddits)}")
        logger.info(f"Top subreddits: {list(top_subreddits)}")
        top_authors = analytics.get_top_authors()
        engagement = analytics.get_engagement_metrics()
        sentiment = analytics.get_sentiment_trends()
        topics = analytics.get_topics()
        influence = analytics.get_user_influence()
        temporal_trends = analytics.get_temporal_trends()
        return top_subreddits, top_authors, engagement, sentiment, topics, influence, temporal_trends, posts_df, comments_df
    except Exception as e:
        st.error(f"Analysis error: {e}")
        logger.error(f"Analysis error: {e}", exc_info=True)
        return [], [], {'avg_score': 0, 'avg_comments': 0}, {}, [], [], {}, pd.DataFrame(), pd.DataFrame()

def display_results(top_subreddits, top_authors, engagement, sentiment, topics, influence, temporal_trends):
    """Display extended analysis results."""
    try:
        st.header("Analysis Results")
        
        st.subheader("Top Subreddits")
        if top_subreddits:
            for subreddit, count in top_subreddits:
                st.write(f"- {subreddit}: {count} posts")
        else:
            st.write("No subreddit data available.")
        
        st.subheader("Top Authors")
        if top_authors:
            for author, count in top_authors:
                st.write(f"- {author}: {count} posts")
        else:
            st.write("No author data available.")
        
        st.subheader("Engagement Metrics")
        st.write(f"- Average Score: {engagement['avg_score']:.2f}")
        st.write(f"- Average Comments: {engagement['avg_comments']:.2f}")
        
        st.subheader("Sentiment Trends")
        if sum(sentiment.values()) > 0:
            for sentiment_type, value in sentiment.items():
                st.write(f"- {sentiment_type.capitalize()}: {value*100:.1f}%")
        else:
            st.write("No sentiment data available.")
        
        st.subheader("Top Topics")
        if topics:
            for topic in topics:
                st.write(f"- {topic}")
        else:
            st.write("No topics identified.")
        
        st.subheader("Most Influential Users")
        if influence:
            for user, score in influence:
                st.write(f"- {user}: Influence Score {score:.2f}")
        else:
            st.write("No influence data available.")
        
        st.subheader("Temporal Trends")
        if temporal_trends.get('by_day'):
            st.write("Posts by Day:")
            for day, count in temporal_trends.get('by_day', {}).items():
                st.write(f"- {day}: {count} posts")
        else:
            st.write("No daily trends available.")
        if temporal_trends.get('by_hour'):
            st.write("Posts by Hour:")
            for hour, count in temporal_trends.get('by_hour', {}).items():
                st.write(f"- {hour}:00: {count} posts")
        else:
            st.write("No hourly trends available.")
    except Exception as e:
        st.error(f"Error displaying results: {e}")
        logger.error(f"Display error: {e}", exc_info=True)

def generate_and_show_visualizations(posts_df, comments_df):
    """Generate all visualizations."""
    try:
        st.header("Visualizations")
        logger.info("Generating visualizations")
        
        is_single_post = len(posts_df) == 1
        
        if not is_single_post:
            st.subheader("Top Subreddits by Post Volume")
            fig = RedditVisualizer.plot_top_subreddits(posts_df)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write("No data for top subreddits.")
            
            st.subheader("Most Active Authors")
            fig = RedditVisualizer.plot_active_authors(posts_df)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write("No data for active authors.")
        
        st.subheader("Comment Distribution per Post")
        fig = RedditVisualizer.plot_comment_distribution(posts_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No data for comment distribution.")
        
        st.subheader("Score vs Number of Comments")
        fig = RedditVisualizer.plot_score_vs_comments(posts_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No data for score vs comments.")
        
        st.subheader("Posting Trend Over Time")
        fig = RedditVisualizer.plot_posting_trend(posts_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No data for posting trend.")
        
        st.subheader("Weekly Posting Heatmap")
        fig = RedditVisualizer.plot_posting_heatmap(posts_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No data for posting heatmap.")
        
        st.subheader("Top Scoring Posts Over Time")
        fig = RedditVisualizer.plot_top_scoring_posts(posts_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No data for top scoring posts.")
        
        st.subheader("Word Cloud of Post Titles")
        fig = RedditVisualizer.plot_word_cloud(posts_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No data for word cloud.")
    
    except Exception as e:
        st.error(f"Error generating visualizations: {e}")
        logger.error(f"Visualization error: {e}", exc_info=True)

def main():
    try:
        st.title("Reddit Analytics Dashboard")
        st.sidebar.header("Settings")
        
        input_type = st.sidebar.radio("Fetch Data By:", ("Subreddit", "Search Query", "Post ID"))
        subreddit_name = None
        query = None
        post_id = None
        if input_type == "Subreddit":
            subreddit_name = st.sidebar.text_input("Subreddit Name", "python").strip().lower()
        elif input_type == "Search Query":
            query = st.sidebar.text_input("Search Query", "python programming").strip()
        else:
            post_id = st.sidebar.text_input("Post ID", "").strip()
        
        limit = st.sidebar.slider("Number of Posts to Fetch (ignored for Post ID)", 10, 100, 50)
        run_button = st.sidebar.button("Run Analysis")

        reddit = RedditConnector()
        hbase = HBaseManager()
        analytics = RedditAnalytics(hbase)

        if run_button:
            with st.spinner("Fetching and analyzing data..."):
                logger.info("Run button clicked")
                success = fetch_and_store_data(reddit, hbase, subreddit_name, query, post_id, limit)
                if success:
                    top_subreddits, top_authors, engagement, sentiment, topics, influence, temporal_trends, posts_df, comments_df = perform_analysis(analytics)
                    display_results(top_subreddits, top_authors, engagement, sentiment, topics, influence, temporal_trends)
                    generate_and_show_visualizations(posts_df, comments_df)
                else:
                    st.error("Data fetching failed. Please check inputs and try again.")
                    logger.error("Data fetching failed")
            
            hbase.connection.close()
            st.success("Analysis complete and connection closed!")
            logger.info("Analysis complete")
    
    except Exception as e:
        st.error(f"Application error: {e}")
        logger.error(f"Main error: {e}", exc_info=True)
        if 'hbase' in locals():
            hbase.connection.close()

if __name__ == "__main__":
    main()