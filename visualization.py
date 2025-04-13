import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from textblob import TextBlob
from wordcloud import WordCloud, STOPWORDS
import base64
from io import BytesIO
from PIL import Image
import requests
from datetime import datetime

class RedditVisualizer:
    @staticmethod
    def plot_top_subreddits(posts_df, save_path='top_subreddits.html'):
        """Bar chart of top subreddits by post volume."""
        if posts_df.empty or 'post_data:subreddit' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No subreddit data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Top Subreddits by Post Volume', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.groupby('post_data:subreddit').size().reset_index(name='Post Count')
        df = df.sort_values('Post Count', ascending=False).head(5)
        
        fig = px.bar(
            df,
            x='post_data:subreddit',
            y='Post Count',
            title='Top Subreddits by Post Volume',
            color='Post Count',
            color_continuous_scale='Viridis',
            text='Post Count',
            height=500
        )
        fig.update_traces(textposition='auto')
        fig.update_layout(
            xaxis_title='Subreddit',
            yaxis_title='Number of Posts',
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6"),
            showlegend=False
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_active_authors(posts_df, save_path='active_authors.html'):
        """Bar chart of most active authors by post count."""
        if posts_df.empty or 'post_data:author' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No author data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Most Active Authors', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.groupby('post_data:author').size().reset_index(name='Post Count')
        df = df[df['post_data:author'] != '[deleted]'].sort_values('Post Count', ascending=False).head(5)
        
        fig = px.bar(
            df,
            x='post_data:author',
            y='Post Count',
            title='Most Active Authors',
            color='Post Count',
            color_continuous_scale='Viridis',
            text='Post Count',
            height=500
        )
        fig.update_traces(textposition='auto')
        fig.update_layout(
            xaxis_title='Author',
            yaxis_title='Number of Posts',
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6"),
            showlegend=False
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_comment_distribution(posts_df, save_path='comment_distribution.html'):
        """Histogram of comments per post."""
        if posts_df.empty or 'metrics:num_comments' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No comment data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Comment Distribution per Post', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.copy()
        df['metrics:num_comments'] = df['metrics:num_comments'].astype(float)
        
        fig = px.histogram(
            df,
            x='metrics:num_comments',
            nbins=20,
            title='Comment Distribution per Post',
            color_discrete_sequence=['#006666'],
            height=500
        )
        fig.update_layout(
            xaxis_title='Number of Comments',
            yaxis_title='Number of Posts',
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6"),
            showlegend=False
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_score_vs_comments(posts_df, save_path='score_vs_comments.html'):
        """Scatter plot of score vs comments."""
        if posts_df.empty or 'metrics:score' not in posts_df.columns or 'metrics:num_comments' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No engagement data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Score vs Number of Comments', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.copy()
        df['metrics:score'] = df['metrics:score'].astype(float)
        df['metrics:num_comments'] = df['metrics:num_comments'].astype(float)
        
        fig = px.scatter(
            df,
            x='metrics:num_comments',
            y='metrics:score',
            title='Score vs Number of Comments',
            hover_data=['post_data:title', 'post_data:subreddit'],
            color_discrete_sequence=['#006666'],
            height=500
        )
        fig.update_layout(
            xaxis_title='Number of Comments',
            yaxis_title='Score',
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6"),
            showlegend=False
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_posting_trend(posts_df, save_path='posting_trend.html'):
        """Line chart of post counts over time."""
        if posts_df.empty or 'post_data:created_utc' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No activity data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Posting Trend Over Time', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.copy()
        df['datetime'] = pd.to_datetime(df['post_data:created_utc'], unit='s')
        df['date'] = df['datetime'].dt.date
        trend = df.groupby('date').size().reset_index(name='Post Count')
        
        fig = px.line(
            trend,
            x='date',
            y='Post Count',
            title='Posting Trend Over Time',
            markers=True,
            color_discrete_sequence=['#006666'],
            height=500
        )
        fig.update_traces(
            line=dict(width=3),
            hovertemplate='Date: %{x}<br>Posts: %{y}<extra></extra>'
        )
        fig.update_layout(
            xaxis_title='Date',
            yaxis_title='Number of Posts',
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6"),
            showlegend=False
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_posting_heatmap(posts_df, save_path='posting_heatmap.html'):
        """Heatmap of posts by day and hour."""
        if posts_df.empty or 'post_data:created_utc' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No activity data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Weekly Posting Heatmap', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.copy()
        df['datetime'] = pd.to_datetime(df['post_data:created_utc'], unit='s')
        df['day'] = df['datetime'].dt.day_name()
        df['hour'] = df['datetime'].dt.hour
        
        heatmap_data = df.pivot_table(index='day', columns='hour', aggfunc='size', fill_value=0)
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        heatmap_data = heatmap_data.reindex(days, fill_value=0)
        
        fig = px.imshow(
            heatmap_data,
            labels=dict(x='Hour of Day', y='Day of Week', color='Post Count'),
            title='Weekly Posting Heatmap',
            color_continuous_scale='Viridis',
            height=500
        )
        fig.update_layout(
            xaxis_title='Hour of Day',
            yaxis_title='Day of Week',
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6")
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_top_scoring_posts(posts_df, save_path='top_scoring_posts.html'):
        """Line chart of top-scoring posts over time."""
        if posts_df.empty or 'metrics:score' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No score data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Top Scoring Posts Over Time', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.copy()
        df['datetime'] = pd.to_datetime(df['post_data:created_utc'], unit='s')
        df['date'] = df['datetime'].dt.date
        df['metrics:score'] = df['metrics:score'].astype(float)
        top_posts = df.sort_values('metrics:score', ascending=False).head(10)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=top_posts['date'],
            y=top_posts['metrics:score'],
            mode='lines+markers',
            text=top_posts['post_data:title'],
            hovertemplate='Date: %{x}<br>Score: %{y}<br>Title: %{text}<extra></extra>',
            line=dict(color='#006666', width=3)
        ))
        fig.update_layout(
            title='Top Scoring Posts Over Time',
            xaxis_title='Date',
            yaxis_title='Score',
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6"),
            showlegend=False,
            height=500
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_word_cloud(posts_df, save_path='word_cloud.html'):
        """Word cloud of post titles."""
        if posts_df.empty or 'post_data:title' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No title data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Word Cloud of Post Titles', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        text = ' '.join(posts_df['post_data:title'].astype(str))
        stopwords = set(STOPWORDS)
        wordcloud = WordCloud(
            width=800, height=400,
            background_color='#2D2D2D',
            stopwords=stopwords,
            min_font_size=10,
            colormap='viridis'
        ).generate(text)
        
        # Convert wordcloud to image
        img = wordcloud.to_image()
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format='PNG')
        img_byte_arr = img_byte_arr.getvalue()
        img_base64 = base64.b64encode(img_byte_arr).decode('utf-8')
        
        fig = go.Figure()
        fig.add_layout_image(
            dict(
                source=f"data:image/png;base64,{img_base64}",
                xref="paper", yref="paper",
                x=0, y=1,
                sizex=1, sizey=1,
                xanchor="left", yanchor="top",
                layer="below"
            )
        )
        fig.update_layout(
            title=dict(text='Word Cloud of Post Titles', font=dict(size=20, color="#E6E6E6")),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            height=500
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_top_images(posts_df, save_path='top_images.html'):
        """Gallery of top-scored image posts."""
        if posts_df.empty or 'post_data:url' not in posts_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No image data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Top Liked Images', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = posts_df.copy()
        df['metrics:score'] = df['metrics:score'].astype(float)
        image_df = df[df['post_data:url'].str.contains(r'\.(jpg|png)$', na=False)]
        top_images = image_df.sort_values('metrics:score', ascending=False).head(3)
        
        if top_images.empty:
            fig = go.Figure()
            fig.add_annotation(
                text="No images found.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Top Liked Images', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        fig = go.Figure()
        for i, row in enumerate(top_images.itertuples()):
            try:
                response = requests.get(row._8, timeout=5)  # post_data:url
                img = Image.open(BytesIO(response.content))
                img = img.resize((150, 150), Image.LANCZOS)
                img_byte_arr = BytesIO()
                img.save(img_byte_arr, format='PNG')
                img_base64 = base64.b64encode(img_byte_arr.getvalue()).decode('utf-8')
                
                fig.add_layout_image(
                    dict(
                        source=f"data:image/png;base64,{img_base64}",
                        xref="paper", yref="paper",
                        x=0.1 + (i * 0.35), y=0.8,
                        sizex=0.3, sizey=0.3,
                        xanchor="center", yanchor="top"
                    )
                )
                fig.add_annotation(
                    x=0.1 + (i * 0.35), y=0.5,
                    text=f"Score: {row._10}<br>Title: {row._4[:30]}...",  # metrics:score, post_data:title
                    showarrow=False,
                    font=dict(size=12, color="#E6E6E6"),
                    xref="paper", yref="paper"
                )
            except Exception as e:
                print(f"Error loading image {row._8}: {e}")
                continue
        
        fig.update_layout(
            title=dict(text='Top Liked Images', font=dict(size=20, color="#E6E6E6")),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            height=500
        )
        fig.write_html(save_path, auto_open=False)
        return fig

    @staticmethod
    def plot_comment_sentiment(comments_df, save_path='comment_sentiment.html'):
        """Pie chart of comment sentiment."""
        if comments_df.empty or 'comment_data:body' not in comments_df.columns:
            fig = go.Figure()
            fig.add_annotation(
                text="No comment data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Sentiment Analysis on Comments', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df = comments_df.copy()
        df['sentiment'] = df['comment_data:body'].apply(
            lambda x: 'positive' if TextBlob(str(x)).sentiment.polarity > 0 else
                      'negative' if TextBlob(str(x)).sentiment.polarity < 0 else 'neutral'
        )
        sentiment_counts = df['sentiment'].value_counts().to_dict()
        sentiment_data = {
            'positive': sentiment_counts.get('positive', 0),
            'neutral': sentiment_counts.get('neutral', 0),
            'negative': sentiment_counts.get('negative', 0)
        }
        total = sum(sentiment_data.values())
        if total == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="No sentiment data available.",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#B0B0B0")
            )
            fig.update_layout(
                title=dict(text='Sentiment Analysis on Comments', font=dict(size=20, color="#E6E6E6")),
                plot_bgcolor='#2D2D2D',
                paper_bgcolor='#2D2D2D',
                font=dict(size=12, color="#E6E6E6"),
                height=500
            )
            fig.write_html(save_path, auto_open=False)
            return fig
        
        df_pie = pd.DataFrame({
            'Sentiment': sentiment_data.keys(),
            'Percentage': [v / total * 100 for v in sentiment_data.values()]
        })
        
        fig = px.pie(
            df_pie,
            names='Sentiment',
            values='Percentage',
            title='Sentiment Analysis on Comments',
            color='Sentiment',
            color_discrete_map={
                'positive': '#006666',
                'neutral': '#B0B0B0',
                'negative': '#CC4B2A'
            },
            height=500
        )
        fig.update_traces(
            textinfo='percent+label',
            hovertemplate='%{label}: %{value:.1f}%<extra></extra>'
        )
        fig.update_layout(
            plot_bgcolor='#2D2D2D',
            paper_bgcolor='#2D2D2D',
            font=dict(size=12, color="#E6E6E6"),
            title_font=dict(size=20, color="#E6E6E6"),
            showlegend=True,
            legend_title='Sentiment'
        )
        fig.write_html(save_path, auto_open=False)
        return fig