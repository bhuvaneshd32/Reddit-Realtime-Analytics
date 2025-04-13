import streamlit as st
import pandas as pd
import sys
import os
import happybase
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from hbase_manager import HBaseManager
    from config import Config
except ModuleNotFoundError as e:
    st.error(f"Import error: {e}")
    logger.error(f"Import error: {e}")
    raise

# Extend HBaseManager to add close method
class ExtendedHBaseManager(HBaseManager):
    def close(self):
        """Close the HBase connection."""
        try:
            if hasattr(self, 'connection'):
                self.connection.close()
                logger.info("HBase connection closed")
        except Exception as e:
            logger.error(f"Error closing HBase connection: {e}")

@st.cache_data(ttl=60)
def fetch_table_data(table_name, _hbase, prefix=Config.HBASE_TABLE_PREFIX):
    """Fetch all data from an HBase table."""
    try:
        table = _hbase.connection.table(f"{prefix}{table_name}")
        rows = []
        for key, data in table.scan():
            row = {'row_key': key.decode()}
            for col, value in data.items():
                row[col.decode()] = value.decode()
            rows.append(row)
        df = pd.DataFrame(rows)
        logger.info(f"Fetched {len(df)} rows from {prefix}{table_name}")
        return df
    except Exception as e:
        logger.error(f"Error fetching {table_name}: {e}")
        st.error(f"Error fetching {table_name}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_table_stats(_hbase, prefix=Config.HBASE_TABLE_PREFIX):
    """Compute database statistics."""
    try:
        posts_df = fetch_table_data("posts", _hbase, prefix)
        comments_df = fetch_table_data("comments", _hbase, prefix)
        
        stats = {
            'total_posts': len(posts_df),
            'total_comments': len(comments_df),
            'unique_subreddits': posts_df['post_data:subreddit'].nunique() if not posts_df.empty and 'post_data:subreddit' in posts_df.columns else 0,
            'unique_post_authors': posts_df['post_data:author'].nunique() if not posts_df.empty and 'post_data:author' in posts_df.columns else 0,
            'unique_comment_authors': comments_df['comment_data:author'].nunique() if not comments_df.empty and 'comment_data:author' in comments_df.columns else 0,
            'posts_per_subreddit': posts_df.groupby('post_data:subreddit').size().to_dict() if not posts_df.empty and 'post_data:subreddit' in posts_df.columns else {}
        }
        logger.info(f"Computed stats: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Error computing stats: {e}")
        st.error(f"Error computing stats: {e}")
        return {}

def delete_all_data(_hbase, prefix=Config.HBASE_TABLE_PREFIX):
    """Delete all data from posts and comments tables."""
    try:
        for table_name in ['posts', 'comments']:
            table = _hbase.connection.table(f"{prefix}{table_name}")
            # Scan all row keys and delete
            row_keys = [key for key, _ in table.scan()]
            for key in row_keys:
                table.delete(key)
            logger.info(f"Deleted all rows from {prefix}{table_name}")
        st.success("All data deleted from database!")
    except Exception as e:
        logger.error(f"Error deleting data: {e}")
        st.error(f"Error deleting data: {e}")

def main():
    st.set_page_config(page_title="HBase Admin Dashboard", layout="wide")
    st.title("HBase Admin Dashboard")
    
    try:
        hbase = ExtendedHBaseManager()
        
        # Dashboard Stats
        st.header("Database Statistics")
        stats = get_table_stats(hbase)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Posts", stats.get('total_posts', 0))
        with col2:
            st.metric("Total Comments", stats.get('total_comments', 0))
        with col3:
            st.metric("Unique Subreddits", stats.get('unique_subreddits', 0))
        
        col4, col5 = st.columns(2)
        with col4:
            st.metric("Unique Post Authors", stats.get('unique_post_authors', 0))
        with col5:
            st.metric("Unique Comment Authors", stats.get('unique_comment_authors', 0))
        
        # Posts per Subreddit
        with st.expander("Posts per Subreddit"):
            if stats.get('posts_per_subreddit'):
                for subreddit, count in sorted(stats['posts_per_subreddit'].items()):
                    st.write(f"{subreddit}: {count} posts")
            else:
                st.write("No subreddit data.")
        
        # Tabs for Posts and Comments
        tab1, tab2 = st.tabs(["Posts", "Comments"])
        
        with tab1:
            st.header("Manage Posts")
            
            # Filters
            st.subheader("Filter Posts")
            col1, col2 = st.columns(2)
            with col1:
                subreddit_filter = st.text_input("Filter by Subreddit", "", key="subreddit_filter_posts")
            with col2:
                author_filter = st.text_input("Filter by Author", "", key="author_filter_posts")
            
            # Fetch and Display Posts
            posts_df = fetch_table_data("posts", hbase)
            if not posts_df.empty:
                filtered_posts = posts_df
                if subreddit_filter and 'post_data:subreddit' in posts_df.columns:
                    filtered_posts = filtered_posts[filtered_posts['post_data:subreddit'].str.lower().str.contains(subreddit_filter, na=False)]
                if author_filter and 'post_data:author' in posts_df.columns:
                    filtered_posts = filtered_posts[filtered_posts['post_data:author'].str.contains(author_filter, case=False, na=False)]
                
                st.subheader("View Posts")
                st.dataframe(
                    filtered_posts,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'row_key': st.column_config.TextColumn("Row Key"),
                        'post_data:id': st.column_config.TextColumn("Post ID"),
                        'post_data:title': st.column_config.TextColumn("Title"),
                        'post_data:subreddit': st.column_config.TextColumn("Subreddit"),
                        'post_data:author': st.column_config.TextColumn("Author"),
                        'metrics:score': st.column_config.NumberColumn("Score")
                    }
                )
            else:
                st.write("No posts found.")
            
            # Update Post
            st.subheader("Update Post")
            with st.form("update_post_form"):
                post_ids = posts_df['post_data:id'].tolist() if not posts_df.empty and 'post_data:id' in posts_df.columns else []
                update_id = st.selectbox("Select Post ID to Update", [""] + post_ids)
                new_title = st.text_input("New Title", key="new_title_post")
                new_score = st.number_input("New Score", min_value=0, value=0)
                submit_update = st.form_submit_button("Update Post")
                if submit_update and update_id:
                    try:
                        table = hbase.connection.table(f"{Config.HBASE_TABLE_PREFIX}posts")
                        row_key = posts_df[posts_df['post_data:id'] == update_id]['row_key'].iloc[0]
                        table.put(row_key, {
                            b'post_data:title': new_title.encode(),
                            b'metrics:score': str(new_score).encode()
                        })
                        st.success(f"Post {update_id} updated!")
                        st.cache_data.clear()  # Clear cache to refresh data
                    except Exception as e:
                        st.error(f"Error updating post: {e}")
                        logger.error(f"Error updating post {update_id}: {e}")
            
            # Delete Post
            st.subheader("Delete Post")
            with st.form("delete_post_form"):
                delete_id = st.selectbox("Select Post ID to Delete", [""] + post_ids)
                confirm_delete = st.checkbox("Confirm Deletion")
                submit_delete = st.form_submit_button("Delete Post")
                if submit_delete and delete_id and confirm_delete:
                    try:
                        table = hbase.connection.table(f"{Config.HBASE_TABLE_PREFIX}posts")
                        row_key = posts_df[posts_df['post_data:id'] == delete_id]['row_key'].iloc[0]
                        table.delete(row_key)
                        st.success(f"Post {delete_id} deleted!")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Error deleting post: {e}")
                        logger.error(f"Error deleting post {delete_id}: {e}")
        
        with tab2:
            st.header("Manage Comments")
            
            # Filters
            st.subheader("Filter Comments")
            col1, col2 = st.columns(2)
            with col1:
                comment_subreddit_filter = st.text_input("Filter by Subreddit", "", key="subreddit_filter_comments")
            with col2:
                comment_author_filter = st.text_input("Filter by Author", "", key="author_filter_comments")
            
            # Fetch and Display Comments
            comments_df = fetch_table_data("comments", hbase)
            if not comments_df.empty:
                filtered_comments = comments_df
                if comment_subreddit_filter and 'comment_data:subreddit' in comments_df.columns:
                    filtered_comments = filtered_comments[filtered_comments['comment_data:subreddit'].str.lower().str.contains(comment_subreddit_filter, na=False)]
                if comment_author_filter and 'comment_data:author' in comments_df.columns:
                    filtered_comments = filtered_comments[filtered_comments['comment_data:author'].str.contains(comment_author_filter, case=False, na=False)]
                
                st.subheader("View Comments")
                st.dataframe(
                    filtered_comments,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'row_key': st.column_config.TextColumn("Row Key"),
                        'comment_data:id': st.column_config.TextColumn("Comment ID"),
                        'comment_data:body': st.column_config.TextColumn("Text"),
                        'comment_data:subreddit': st.column_config.TextColumn("Subreddit"),
                        'comment_data:author': st.column_config.TextColumn("Author"),
                        'comment_data:post_id': st.column_config.TextColumn("Post ID"),
                        'metrics:score': st.column_config.NumberColumn("Score")
                    }
                )
            else:
                st.write("No comments found.")
            
            # Update Comment
            st.subheader("Update Comment")
            with st.form("update_comment_form"):
                comment_ids = comments_df['comment_data:id'].tolist() if not comments_df.empty and 'comment_data:id' in comments_df.columns else []
                update_comment_id = st.selectbox("Select Comment ID to Update", [""] + comment_ids)
                new_text = st.text_area("New Comment Text", key="new_text_comment")
                new_comment_score = st.number_input("New Score", min_value=0, value=0)
                submit_comment_update = st.form_submit_button("Update Comment")
                if submit_comment_update and update_comment_id:
                    try:
                        table = hbase.connection.table(f"{Config.HBASE_TABLE_PREFIX}comments")
                        row_key = comments_df[comments_df['comment_data:id'] == update_comment_id]['row_key'].iloc[0]
                        table.put(row_key, {
                            b'comment_data:body': new_text.encode(),
                            b'metrics:score': str(new_comment_score).encode()
                        })
                        st.success(f"Comment {update_comment_id} updated!")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Error updating comment: {e}")
                        logger.error(f"Error updating comment {update_comment_id}: {e}")
            
            # Delete Comment
            st.subheader("Delete Comment")
            with st.form("delete_comment_form"):
                delete_comment_id = st.selectbox("Select Comment ID to Delete", [""] + comment_ids)
                confirm_comment_delete = st.checkbox("Confirm Deletion")
                submit_comment_delete = st.form_submit_button("Delete Comment")
                if submit_comment_delete and delete_comment_id and confirm_comment_delete:
                    try:
                        table = hbase.connection.table(f"{Config.HBASE_TABLE_PREFIX}comments")
                        row_key = comments_df[posts_df['comment_data:id'] == delete_comment_id]['row_key'].iloc[0]
                        table.delete(row_key)
                        st.success(f"Comment {delete_comment_id} deleted!")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Error deleting comment: {e}")
                        logger.error(f"Error deleting comment {delete_comment_id}: {e}")
        
        # Delete All Data
        st.header("Danger Zone")
        with st.expander("Clear Entire Database", expanded=False):
            st.warning("This action will delete ALL posts and comments. It cannot be undone.")
            with st.form("delete_all_form"):
                confirm_clear = st.checkbox("I understand and want to delete all data")
                delete_all_button = st.form_submit_button("Delete Everything")
                if delete_all_button and confirm_clear:
                    with st.spinner("Deleting all data..."):
                        delete_all_data(hbase)
                        st.cache_data.clear()
        
    except Exception as e:
        st.error(f"Application error: {e}")
        logger.error(f"Main error: {e}")
        if 'hbase' in locals():
            hbase.close()

if __name__ == "__main__":
    main()