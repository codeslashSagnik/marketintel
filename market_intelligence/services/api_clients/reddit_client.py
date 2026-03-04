"""
services/api_clients/reddit_client.py

Reddit API client using PRAW (Python Reddit API Wrapper).
Fetches recent posts for a keyword and returns sentiment-ready records.

Usage:
    client = RedditClient()
    posts  = client.fetch_posts("grocery")   # list[dict]
"""
import logging
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger("services.api_clients.reddit")


class RedditClient:
    """
    Fetches recent Reddit posts/comments for market sentiment tracking.

    Requires PRAW credentials in settings:
        REDDIT_CLIENT_ID
        REDDIT_CLIENT_SECRET
        REDDIT_USER_AGENT
    """

    SUBREDDITS  = ["india", "IndiaInvestments", "bangalore", "mumbai", "delhi"]
    POST_LIMIT  = 50   # posts per keyword per call

    def __init__(self):
        try:
            import praw
            self.reddit = praw.Reddit(
                client_id     = settings.REDDIT_CLIENT_ID,
                client_secret = settings.REDDIT_CLIENT_SECRET,
                user_agent    = settings.REDDIT_USER_AGENT,
            )
            logger.info("RedditClient | initialised successfully")
        except Exception as exc:
            logger.error("RedditClient | init failed error=%s", exc)
            self.reddit = None

    def fetch_posts(self, keyword: str) -> list[dict]:
        """
        Search Reddit for posts matching a keyword.

        Args:
            keyword: Search term, e.g. "bigbasket"

        Returns:
            List of normalised post dicts ready for SentimentData.
        """
        if not self.reddit:
            logger.warning("RedditClient.fetch_posts | reddit=None keyword=%s", keyword)
            return []

        logger.info("RedditClient.fetch_posts | keyword=%s status=start", keyword)

        try:
            posts = []
            results = self.reddit.subreddit("all").search(
                keyword,
                limit   = self.POST_LIMIT,
                sort    = "new",
                time_filter = "day",
            )

            for submission in results:
                posts.append(
                    self._normalise(submission, keyword)
                )

            logger.info(
                "RedditClient.fetch_posts | keyword=%s records=%d status=success",
                keyword, len(posts),
            )
            return posts

        except Exception as exc:
            logger.error(
                "RedditClient.fetch_posts | keyword=%s status=failed error=%s",
                keyword, exc, exc_info=True,
            )
            return []

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _normalise(self, submission, keyword: str) -> dict:
        """
        Map a PRAW Submission to the shape expected by SentimentData.
        Sentiment scoring is a placeholder — plug in TextBlob / VADER here.
        """
        import datetime
        created_utc = datetime.datetime.utcfromtimestamp(
            submission.created_utc
        ).replace(tzinfo=datetime.timezone.utc)

        text = f"{submission.title} {submission.selftext}".strip()

        # ── Plug real NLP here (TextBlob / VADER / Transformers) ──────────────
        sentiment_score = self._score_sentiment(text)
        # ─────────────────────────────────────────────────────────────────────

        return {
            "text":            text[:2000],      # truncate to DB field limit
            "sentiment_score": sentiment_score,
            "keyword":         keyword,
            "created_at":      created_utc,
        }

    def _score_sentiment(self, text: str) -> float:
        """
        Placeholder sentiment scorer.
        Swap this with TextBlob, VADER, or a fine-tuned transformer model.
        Returns a score in [-1.0, +1.0].
        """
        try:
            from textblob import TextBlob
            return TextBlob(text).sentiment.polarity
        except ImportError:
            # TextBlob not installed — return neutral score
            return 0.0
