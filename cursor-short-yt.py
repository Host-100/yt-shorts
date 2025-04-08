#!/usr/bin/env python3
# Shorts Downloader Pro v2.7 - 2025-04-15
# Major improvements from v2.2:
# • Task Queue System: Properly manages download queue with pause/resume capabilities
# • Enhanced Media Processing: Watermark removal option, audio extraction, and custom quality selection
# • User Preferences: Personalized settings stored per user with custom caption templates
# • Intelligent Error Handling: Auto-retry with exponential backoff for network issues
# • Inline Mode Support: Share downloaded shorts directly to other chats
# • Performance Optimization: Multi-threaded downloads with proper cleanup
# • Advanced Admin Panel: Usage statistics, user management, and blacklist options
# • Batch Processing: Improved handling of multiple URLs with smart prioritization
# • Rate Limiting: Fair usage policies to prevent abuse
# • Security Enhancements: Input validation and better error handling

import os
import sys
import json
import yt_dlp
import telebot
import shutil
import logging
import threading
import random
import re
import time
import queue
import hashlib
import asyncio
import concurrent.futures
import subprocess
import flask
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from collections import defaultdict
from datetime import datetime, timedelta
from humanize import naturalsize, intword
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, InlineQueryResultVideo, InlineQueryResultArticle, InputTextMessageContent
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
import glob
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters
import math

# Database setup
Base = declarative_base()

class UserModel(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String(50), unique=True, nullable=False)
    join_date = Column(DateTime, default=datetime.now)
    total_downloads = Column(Integer, default=0)
    gdrive_enabled = Column(Boolean, default=True)
    quality = Column(String(20), default="high")
    remove_watermark = Column(Boolean, default=False)
    auto_audio = Column(Boolean, default=False)
    caption_template = Column(Text, default=None)
    last_used = Column(DateTime, default=datetime.now)
    admin_override = Column(Boolean, default=False)  # For admins to bypass video length limits
    
    tokens = relationship("TokenModel", back_populates="user", cascade="all, delete-orphan")
    downloads = relationship("DownloadModel", back_populates="user", cascade="all, delete-orphan")
    
class TokenModel(Base):
    __tablename__ = 'tokens'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String(50), ForeignKey('users.user_id'), nullable=False)
    token_data = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    expires_at = Column(DateTime)
    
    user = relationship("UserModel", back_populates="tokens")
    
class DownloadModel(Base):
    __tablename__ = 'downloads'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String(50), ForeignKey('users.user_id'), nullable=False)
    url = Column(String(255), nullable=False)
    date = Column(DateTime, default=datetime.now)
    status = Column(String(20), default="completed")
    size = Column(Integer, default=0)
    drive_link = Column(String(255), nullable=True)
    
    user = relationship("UserModel", back_populates="downloads")

# Initialize database
engine = create_engine(Config.DATABASE_URL)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

# Create tables if they don't exist
def init_db():
    Base.metadata.create_all(engine)

# Global dictionaries for managing user tasks and cancellation flags.
current_tasks = {}  # Maps user_id to currently processing file path (if any)
cancel_flags = {}   # Maps user_id to a boolean cancellation flag

def send_auto_delete_message(bot, chat_id, text, delay, parse_mode='Markdown'):
    """Sends a message and schedules its deletion after 'delay' seconds."""
    try:
        msg = bot.send_message(chat_id, text, parse_mode=parse_mode)
        threading.Timer(delay, lambda: delete_message_safe(bot, chat_id, msg.message_id)).start()
    except Exception as e:
        logging.error(f"Auto-delete message error: {e}")

def delete_message_safe(bot, chat_id, message_id):
    try:
        bot.delete_message(chat_id, message_id)
    except Exception as e:
        logging.error(f"Error deleting message {message_id} in chat {chat_id}: {e}")

def restart_bot():
    """Fully restart the bot process using the absolute path of the script."""
    logging.info("Restarting bot as requested by admin.")
    python = sys.executable
    script = os.path.abspath(sys.argv[0])
    os.execl(python, python, script, *sys.argv[1:])

class Config:
    # Use environment variables with fallbacks for Railway.app deployment
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "7996960881:AAGUvVyTStBO8tf-YS2G6HJhQop6HGwrUmg")
    CHANNEL_USERNAME = os.environ.get("CHANNEL_USERNAME", "greninjatoonsofficial")
    ADMIN_IDS = [int(id) for id in os.environ.get("ADMIN_IDS", "1367377480").split(",")]
    ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", "1367377480"))
    
    # Directory configuration
    TEMP_FOLDER = os.environ.get("TEMP_FOLDER", "temp_downloads")
    PROCESSED_FOLDER = os.environ.get("PROCESSED_FOLDER", "processed_downloads")
    DATA_DIR = os.environ.get("DATA_DIR", "data")
    
    # For consistent naming - these all point to the same directories
    TEMP_DOWNLOAD_DIR = TEMP_FOLDER
    PROCESSED_DIR = PROCESSED_FOLDER
    
    MAX_FOLDER_SIZE_MB = int(os.environ.get("MAX_FOLDER_SIZE_MB", "500"))
    DOWNLOAD_LIMIT_PER_DAY = int(os.environ.get("DOWNLOAD_LIMIT_PER_DAY", "1000"))
    MAX_VIDEO_SIZE_MB = int(os.environ.get("MAX_VIDEO_SIZE_MB", "250"))
    USER_STATS_FILE = os.environ.get("USER_STATS_FILE", "user_stats.json")
    USER_PREFS_FILE = os.environ.get("USER_PREFS_FILE", "user_prefs.json")
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "1gzqnesWrdWfyt6cLZ3UJ667DNE9lFkqG")
    LOG_FORMAT = os.environ.get("LOG_FORMAT", '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    LOG_FILE = os.environ.get("LOG_FILE", 'bot.log')
    LOG_MAX_SIZE = int(os.environ.get("LOG_MAX_SIZE", str(5 * 1024 * 1024)))
    LOG_BACKUP_COUNT = int(os.environ.get("LOG_BACKUP_COUNT", "5"))
    DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///bot_data.db")
    AUTH_CALLBACK_URL = os.environ.get("AUTH_CALLBACK_URL", "https://bot.example.com/oauth2callback")
    PORT = int(os.environ.get("PORT", "8080"))
    # All available reactions (the thumbs-up is always included)
    ALL_REACTIONS = ['👍', '😂', '😊', '😎', '😘', '❤️', '🤯', '💀', '👻', '🥶', '😼', '🔥', '✨', '🐐']
    DEFAULT_CAPTION_TEMPLATE = "{title}\n\n👁 Views: {view_count}\n👍 Likes: {like_count}\n⏱ Duration: {duration}\n📅 Uploaded: {upload_date}\n🎥 Quality: {quality} {fps}fps\n💾 Size: {size}\n\n🔗 Original: [Link]({url})"
    
    # Enhanced download settings
    PREFERRED_FORMAT = os.environ.get("PREFERRED_FORMAT", "mkv")  # Prefer MKV container
    MAX_VIDEO_LENGTH_MINUTES = int(os.environ.get("MAX_VIDEO_LENGTH_MINUTES", "10"))  # Non-admin video length limit
    INCLUDE_SUBTITLES = os.environ.get("INCLUDE_SUBTITLES", "true").lower() == "true"
    INCLUDE_CHAPTERS = os.environ.get("INCLUDE_CHAPTERS", "true").lower() == "true"
    INCLUDE_THUMBNAIL = os.environ.get("INCLUDE_THUMBNAIL", "true").lower() == "true"
    INCLUDE_AUDIO_TRACKS = os.environ.get("INCLUDE_AUDIO_TRACKS", "true").lower() == "true"
    
    # Subtitle and audio language preferences
    SUBTITLE_LANGUAGES = os.environ.get("SUBTITLE_LANGUAGES", "en,en-auto").split(",")
    AUDIO_LANGUAGES = os.environ.get("AUDIO_LANGUAGES", "en-US,fr,ru,de,ja").split(",")
    
    # Quality options (now used for fallback only, we'll try to get best quality first)
    QUALITY_OPTIONS = {
        "low": "480p",
        "medium": "720p",
        "high": "1080p"
    }
    
    MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "5"))
    MAX_QUEUE_SIZE = int(os.environ.get("MAX_QUEUE_SIZE", "10"))
    RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "60"))
    RATE_LIMIT_COUNT = int(os.environ.get("RATE_LIMIT_COUNT", "10"))
    RETRY_ATTEMPTS = int(os.environ.get("RETRY_ATTEMPTS", "3"))
    RETRY_DELAY = int(os.environ.get("RETRY_DELAY", "1"))
    MAX_RETRY_DELAY = int(os.environ.get("MAX_RETRY_DELAY", "10"))
    # Google Drive OAuth scopes
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    # For donation button
    DONATION_LINK = os.environ.get("DONATION_LINK", "https://example.com/donate")

class ReactionHandler:
    def __init__(self):
        self.reactions = Config.ALL_REACTIONS

    def get_custom_reactions(self, message_text):
        # Return the full set of emojis (always includes a thumbs-up)
        return self.reactions

    def react_to_message(self, message, bot):
        try:
            reactions = self.get_custom_reactions(message.text or message.caption or "")
            # Pick one random emoji from the available set
            selected = random.choice(reactions)
            bot.set_message_reaction(
                chat_id=message.chat.id,
                message_id=message.message_id,
                reaction=[{"type": "emoji", "emoji": selected}]
            )
            return True
        except Exception as e:
            logging.error(f"Error adding reaction: {e}")
            return False

class UserStats:
    def __init__(self):
        self._lock = threading.Lock()

    def update_downloads(self, user_id):
        with self._lock:
            user_id = str(user_id)
            today = datetime.now().strftime("%Y-%m-%d")
            
            session = Session()
            try:
                # Get or create user
                user = session.query(UserModel).filter_by(user_id=user_id).first()
                if not user:
                    user = UserModel(
                        user_id=user_id,
                        join_date=datetime.now(),
                        total_downloads=0,
                        gdrive_enabled=True
                    )
                    session.add(user)
                
                # Create download record
                download = DownloadModel(
                    user_id=user_id,
                    url="",  # This could be populated if needed
                    date=datetime.now(),
                    status="completed"
                )
                session.add(download)
                
                # Update user stats
                user.total_downloads += 1
                user.last_used = datetime.now()
                
                session.commit()
                
                # Count downloads today
                today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                tomorrow = today_start + timedelta(days=1)
                count = session.query(DownloadModel).filter(
                    DownloadModel.user_id == user_id,
                    DownloadModel.date >= today_start,
                    DownloadModel.date < tomorrow
                ).count()
                
                return count
            except Exception as e:
                session.rollback()
                logging.error(f"Error updating downloads: {e}")
                return 0
            finally:
                session.close()

    def check_limit(self, user_id):
        user_id = str(user_id)
        session = Session()
        try:
            # Count today's downloads
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow = today_start + timedelta(days=1)
            count = session.query(DownloadModel).filter(
                DownloadModel.user_id == user_id,
                DownloadModel.date >= today_start,
                DownloadModel.date < tomorrow
            ).count()
            
            return count < Config.DOWNLOAD_LIMIT_PER_DAY
        except Exception as e:
            logging.error(f"Error checking limit: {e}")
            return True  # Default to allowing download on error
        finally:
            session.close()

    def get_user_stats(self, user_id):
        user_id = str(user_id)
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=user_id).first()
            if not user:
                return {}
                
            # Count today's downloads
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow = today_start + timedelta(days=1)
            today_downloads = session.query(DownloadModel).filter(
                DownloadModel.user_id == user_id,
                DownloadModel.date >= today_start,
                DownloadModel.date < tomorrow
            ).count()
            
            # Create stats dict
            stats = {
                "downloads": {datetime.now().strftime("%Y-%m-%d"): today_downloads},
                "join_date": user.join_date.strftime("%Y-%m-%d"),
                "total_downloads": user.total_downloads,
                "gdrive_enabled": user.gdrive_enabled
            }
            
            return stats
        except Exception as e:
            logging.error(f"Error getting user stats: {e}")
            return {}
        finally:
            session.close()

    def get_gdrive_preference(self, user_id):
        user_id = str(user_id)
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=user_id).first()
            if not user:
                return True  # Default to enabled
            return user.gdrive_enabled
        except Exception as e:
            logging.error(f"Error getting GDrive preference: {e}")
            return True
        finally:
            session.close()

    def toggle_gdrive_preference(self, user_id):
        user_id = str(user_id)
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=user_id).first()
            if not user:
                # Create user if not exists
                user = UserModel(
                    user_id=user_id,
                    join_date=datetime.now(),
                    gdrive_enabled=True
                )
                session.add(user)
                
            # Toggle preference
            user.gdrive_enabled = not user.gdrive_enabled
            session.commit()
            
            return user.gdrive_enabled
        except Exception as e:
            session.rollback()
            logging.error(f"Error toggling GDrive preference: {e}")
            return True
        finally:
            session.close()

class GDriveCache:
    _cache = {}
    _lock = threading.Lock()
    
    @classmethod
    def get(cls, key):
        with cls._lock:
            return cls._cache.get(key)
            
    @classmethod
    def set(cls, key, value):
        with cls._lock:
            cls._cache[key] = value

class GoogleDriveManager:
    def __init__(self, bot=None):
        self.bot = bot
        self.auth_users = {}  # Store temporary auth state: user_id -> auth_state
        self.app = self._create_web_app()

    def _create_web_app(self):
        """Create a simple Flask app for OAuth callback handling."""
        app = flask.Flask(__name__)
        
        @app.route('/oauth2callback')
        def oauth2callback():
            state = flask.request.args.get('state', '')
            if not state in self.auth_users:
                return 'Invalid state parameter', 400
                
            user_id = self.auth_users[state]
            flow = self.auth_users.get(f"flow_{user_id}")
            if not flow:
                return 'Authentication flow expired', 400
                
            # Exchange auth code for access token
            flow.fetch_token(authorization_response=flask.request.url)
            credentials = flow.credentials
            
            # Save credentials to database
            session = Session()
            try:
                # Check if token already exists
                token = session.query(TokenModel).filter_by(user_id=str(user_id)).first()
                if token:
                    token.token_data = credentials.to_json()
                    token.created_at = datetime.now()
                    token.expires_at = datetime.now() + timedelta(seconds=credentials.expiry)
                else:
                    token = TokenModel(
                        user_id=str(user_id),
                        token_data=credentials.to_json(),
                        expires_at=datetime.now() + timedelta(seconds=credentials.expiry)
                    )
                    session.add(token)
                session.commit()
                
                # Clean up auth state
                del self.auth_users[state]
                if f"flow_{user_id}" in self.auth_users:
                    del self.auth_users[f"flow_{user_id}"]
                
                # Notify user through bot
                if self.bot:
                    self.bot.send_message(
                        user_id,
                        "✅ Google Drive successfully connected! You can now use Drive backup features."
                    )
                
                return 'Authentication successful! You can close this window and return to the bot.'
            except Exception as e:
                session.rollback()
                logging.error(f"Error saving token: {e}")
                return 'Authentication error occurred', 500
            finally:
                session.close()
    
    def start_oauth_flow(self, user_id):
        """Start OAuth flow for a user and return the auth URL."""
        try:
            # Generate a random state token
            state = hashlib.sha256(f"{user_id}_{time.time()}".encode()).hexdigest()
            
            # Create the flow
            flow = Flow.from_client_secrets_file(
                'client_secrets.json',  # We'll need to handle this in a new way
                scopes=Config.SCOPES,
                redirect_uri=Config.AUTH_CALLBACK_URL
            )
            
            # Save state
            self.auth_users[state] = user_id
            self.auth_users[f"flow_{user_id}"] = flow
            
            # Generate authorization URL
            auth_url, _ = flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true',
                state=state,
                prompt='consent'  # Force to show consent screen for refresh token
            )
            
            return auth_url
        except Exception as e:
            logging.error(f"Error starting OAuth flow: {e}")
            return None
    
    def get_drive_service(self, user_id):
        """Get an authenticated Google Drive service for specific user."""
        session = Session()
        try:
            token_entry = session.query(TokenModel).filter_by(user_id=str(user_id)).first()
            if not token_entry:
                return None
                
            creds = Credentials.from_authorized_user_info(json.loads(token_entry.token_data), Config.SCOPES)
            
            # Check if token is expired and needs refresh
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Update token in database
                token_entry.token_data = creds.to_json()
                token_entry.expires_at = datetime.now() + timedelta(seconds=creds.expiry)
                session.commit()
            
            return build('drive', 'v3', credentials=creds, cache_discovery=False)
        except Exception as e:
            logging.error(f"Error getting drive service for user {user_id}: {e}")
            return None
        finally:
            session.close()
    
    def check_auth_status(self, user_id):
        """Check if a user is authenticated with Google Drive."""
        session = Session()
        try:
            token = session.query(TokenModel).filter_by(user_id=str(user_id)).first()
            if not token:
                return False
                
            # Check if token is expired and can't be refreshed
            creds = Credentials.from_authorized_user_info(json.loads(token.token_data), Config.SCOPES)
            if creds.expired and not creds.refresh_token:
                return False
                
            return True
        except Exception as e:
            logging.error(f"Error checking auth status: {e}")
            return False
        finally:
            session.close()

    def get_welcome_gif_url(self):
        """Get welcome GIF URL from global admin drive."""
        cached_url = GDriveCache.get('welcome_gif')
        if cached_url:
            return cached_url
        try:
            # Use admin credentials for welcome GIF
            admin_id = Config.ADMIN_IDS[0]
            service = self.get_drive_service(admin_id)
            if not service:
                return None
                
            results = service.files().list(
                q=f"name='welcome.gif' and '{Config.GDRIVE_FOLDER_ID}' in parents",
                fields="files(webViewLink)"
            ).execute()
            if results.get('files'):
                url = results['files'][0]['webViewLink']
                GDriveCache.set('welcome_gif', url)
                return url
        except Exception as e:
            logging.error(f"Error getting welcome GIF URL: {e}")
        return None

    def upload_file(self, user_id, file_path, file_name):
        """Upload a file to the user's Google Drive."""
        try:
            service = self.get_drive_service(user_id)
            if not service:
                return None
                
            file_metadata = {
                'name': file_name,
                'parents': [Config.GDRIVE_FOLDER_ID]
            }
            media = MediaFileUpload(file_path, resumable=True)
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            return file.get('webViewLink')
        except Exception as e:
            logging.error(f"Drive upload error for user {user_id}: {e}")
            return None
    
    def run_webapp(self):
        """Run the Flask web app for auth callbacks."""
        # Start in a separate thread
        threading.Thread(target=lambda: self.app.run(
            host='0.0.0.0',
            port=Config.PORT,
            debug=False,
            use_reloader=False
        ), daemon=True).start()

class UserPreferences:
    def __init__(self):
        self._lock = threading.Lock()

    def get_user_prefs(self, user_id):
        user_id = str(user_id)
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=user_id).first()
            if not user:
                # Create default user preferences
                user = UserModel(
                    user_id=user_id,
                    join_date=datetime.now(),
                    quality="high",
                    gdrive_enabled=True,
                    remove_watermark=False,
                    auto_audio=False,
                    caption_template=Config.DEFAULT_CAPTION_TEMPLATE,
                    last_used=datetime.now()
                )
                session.add(user)
                session.commit()
                
            # Convert to dict format for compatibility
            prefs = {
                "quality": user.quality,
                "gdrive_enabled": user.gdrive_enabled,
                "remove_watermark": user.remove_watermark,
                "auto_audio": user.auto_audio,
                "caption_template": user.caption_template or Config.DEFAULT_CAPTION_TEMPLATE,
                "last_used": user.last_used.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return prefs
        except Exception as e:
            session.rollback()
            logging.error(f"Error getting user preferences: {e}")
            # Return defaults on error
            return {
                "quality": "high",
                "gdrive_enabled": True,
                "remove_watermark": False,
                "auto_audio": False,
                "caption_template": Config.DEFAULT_CAPTION_TEMPLATE,
                "last_used": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        finally:
            session.close()

    def update_preference(self, user_id, key, value):
        user_id = str(user_id)
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=user_id).first()
            if not user:
                # Create user if not exists
                user = UserModel(user_id=user_id, join_date=datetime.now())
                session.add(user)
                
            # Update the specified preference
            if hasattr(user, key):
                setattr(user, key, value)
                user.last_used = datetime.now()
                session.commit()
                return value
            else:
                logging.warning(f"Attempted to update unknown preference key: {key}")
                return None
        except Exception as e:
            session.rollback()
            logging.error(f"Error updating preference: {e}")
            return None
        finally:
            session.close()

    def get_quality_format(self, user_id):
        prefs = self.get_user_prefs(user_id)
        quality = prefs.get("quality", "high")
        return Config.QUALITY_OPTIONS.get(quality, Config.QUALITY_OPTIONS["high"])

    def is_watermark_removal_enabled(self, user_id):
        prefs = self.get_user_prefs(user_id)
        return prefs.get("remove_watermark", False)

    def is_audio_extraction_enabled(self, user_id):
        prefs = self.get_user_prefs(user_id)
        return prefs.get("auto_audio", False)

    def get_caption_template(self, user_id):
        prefs = self.get_user_prefs(user_id)
        return prefs.get("caption_template", Config.DEFAULT_CAPTION_TEMPLATE)

    def toggle_preference(self, user_id, key):
        user_id = str(user_id)
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=user_id).first()
            if not user:
                # Create user if not exists
                user = UserModel(user_id=user_id, join_date=datetime.now())
                session.add(user)
                
            # Toggle boolean preference
            if hasattr(user, key) and isinstance(getattr(user, key), bool):
                current_value = getattr(user, key)
                setattr(user, key, not current_value)
                user.last_used = datetime.now()
                session.commit()
                return not current_value
            else:
                logging.warning(f"Attempted to toggle non-boolean preference: {key}")
                return None
        except Exception as e:
            session.rollback()
            logging.error(f"Error toggling preference: {e}")
            return None
        finally:
            session.close()

class DownloadTask:
    def __init__(self, url, user_id, chat_id, message_id=None, priority=0):
        self.url = url
        self.user_id = user_id
        self.chat_id = chat_id
        self.message_id = message_id
        self.priority = priority
        self.id = hashlib.md5(f"{url}_{user_id}_{time.time()}".encode()).hexdigest()
        self.status = "queued"  # queued, downloading, processing, uploading, completed, failed, cancelled
        self.progress = 0
        self.file_path = None
        self.drive_link = None
        self.error = None
        self.created_at = datetime.now()
        self.completed_at = None
        self.retries = 0

    def __lt__(self, other):
        # For priority queue ordering
        return self.priority > other.priority  # Higher priority first

    def update_status(self, status, progress=None, error=None):
        self.status = status
        if progress is not None:
            self.progress = progress
        if error:
            self.error = error
        if status in ["completed", "failed", "cancelled"]:
            self.completed_at = datetime.now()

    def get_duration(self):
        if not self.completed_at:
            return (datetime.now() - self.created_at).total_seconds()
        return (self.completed_at - self.created_at).total_seconds()

    def to_dict(self):
        return {
            "id": self.id,
            "url": self.url,
            "user_id": self.user_id,
            "chat_id": self.chat_id,
            "status": self.status,
            "progress": self.progress,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "drive_link": self.drive_link,
            "error": self.error
        }

class DownloadQueue:
    def __init__(self, max_workers=Config.MAX_CONCURRENT_DOWNLOADS):
        self.task_queue = queue.PriorityQueue()
        self.active_tasks = {}  # task_id -> task
        self.completed_tasks = {}  # task_id -> task
        self.user_queues = defaultdict(list)  # user_id -> [task_ids]
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.lock = threading.Lock()
        self.stats = {
            "queued": 0,
            "active": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0
        }

    def add_task(self, task):
        with self.lock:
            # Check if user has too many tasks
            if len(self.user_queues[task.user_id]) >= Config.MAX_QUEUE_SIZE:
                return False, "Queue limit reached for user"
            
            self.task_queue.put((task.priority, task))
            self.user_queues[task.user_id].append(task.id)
            self.active_tasks[task.id] = task
            self.stats["queued"] += 1
            return True, task.id

    def get_next_task(self):
        try:
            _, task = self.task_queue.get(block=False)
            with self.lock:
                self.stats["queued"] -= 1
                self.stats["active"] += 1
            return task
        except queue.Empty:
            return None

    def complete_task(self, task_id, status="completed", error=None):
        with self.lock:
            if task_id in self.active_tasks:
                task = self.active_tasks.pop(task_id)
                task.update_status(status, error=error)
                self.completed_tasks[task_id] = task
                self.stats["active"] -= 1
                if status == "completed":
                    self.stats["completed"] += 1
                elif status == "failed":
                    self.stats["failed"] += 1
                elif status == "cancelled":
                    self.stats["cancelled"] += 1
                # Remove from user queue
                if task.user_id in self.user_queues and task_id in self.user_queues[task.user_id]:
                    self.user_queues[task.user_id].remove(task_id)
                return task
        return None

    def cancel_user_tasks(self, user_id):
        with self.lock:
            cancelled = []
            for task_id in list(self.user_queues[user_id]):
                if task_id in self.active_tasks:
                    task = self.active_tasks[task_id]
                    self.complete_task(task_id, status="cancelled")
                    cancelled.append(task_id)
            return cancelled

    def get_user_tasks(self, user_id):
        with self.lock:
            tasks = []
            for task_id in self.user_queues[user_id]:
                if task_id in self.active_tasks:
                    tasks.append(self.active_tasks[task_id])
            return tasks

    def get_queue_stats(self):
        with self.lock:
            return dict(self.stats)

    def cleanup_old_tasks(self, max_age_hours=24):
        with self.lock:
            now = datetime.now()
            threshold = now - timedelta(hours=max_age_hours)
            
            to_remove = []
            for task_id, task in self.completed_tasks.items():
                if task.completed_at and task.completed_at < threshold:
                    to_remove.append(task_id)
            
            for task_id in to_remove:
                del self.completed_tasks[task_id]

class RateLimiter:
    def __init__(self, window=Config.RATE_LIMIT_WINDOW, max_requests=Config.RATE_LIMIT_COUNT):
        self.window = window  # Time window in seconds
        self.max_requests = max_requests
        self.user_requests = defaultdict(list)  # user_id -> [timestamp1, timestamp2, ...]
        self.lock = threading.Lock()

    def check_rate_limit(self, user_id):
        with self.lock:
            now = time.time()
            # Remove timestamps outside the window
            self.user_requests[user_id] = [ts for ts in self.user_requests[user_id] if now - ts < self.window]
            # Check if user has exceeded the limit
            return len(self.user_requests[user_id]) < self.max_requests

    def add_request(self, user_id):
        with self.lock:
            now = time.time()
            self.user_requests[user_id].append(now)
            return True

class ShortsDownloaderBot:
    def __init__(self):
        self.bot = telebot.TeleBot(Config.BOT_TOKEN)
        # Initialize database
        init_db()
        # Initialize drive manager with bot reference
        self.drive_manager = GoogleDriveManager(self.bot)
        self.user_stats = UserStats()
        self.user_prefs = UserPreferences()
        self.reaction_handler = ReactionHandler()
        self.download_queue = DownloadQueue()
        self.rate_limiter = RateLimiter()
        
        # Ensure necessary folders exist
        self.ensure_directories()
        
        # Clean up temporary folders on startup
        self.cleanup_temp_folder(aggressive=True)
        setup_logging()
        
        # Start the web app for Google Drive auth
        self.drive_manager.run_webapp()
        
        # Start background task processor
        self.task_processor_thread = threading.Thread(target=self.process_download_queue, daemon=True)
        self.task_processor_thread.start()
        
        # Start cleanup thread for old tasks
        self.cleanup_thread = threading.Thread(target=self.run_periodic_cleanup, daemon=True)
        self.cleanup_thread.start()
        
        # Start storage monitoring thread
        self.storage_monitor_thread = threading.Thread(target=self.monitor_storage, daemon=True)
        self.storage_monitor_thread.start()
        
        self.register_handlers()
        
        try:
            self.bot.delete_my_commands()
            self.bot.set_my_commands([
                telebot.types.BotCommand("start", "Start the bot"),
                telebot.types.BotCommand("help", "Show help information"),
                telebot.types.BotCommand("stats", "View your download statistics"),
                telebot.types.BotCommand("about", "About the bot"),
                telebot.types.BotCommand("settings", "Bot settings"),
                telebot.types.BotCommand("donate", "Support development"),
                telebot.types.BotCommand("version", "Bot version information"),
                telebot.types.BotCommand("queue", "View your download queue"),
                telebot.types.BotCommand("togglegdrive", "Toggle Google Drive backup"),
                telebot.types.BotCommand("togglewatermark", "Toggle watermark removal"),
                telebot.types.BotCommand("toggleaudio", "Toggle audio extraction"),
                telebot.types.BotCommand("setquality", "Set video quality"),
                telebot.types.BotCommand("admin_override", "Admin-only: Toggle length limit override"),
                telebot.types.BotCommand("cancel", "Cancel current download(s)"),
                telebot.types.BotCommand("restart", "Admin-only: Restart the bot")
            ])
        except Exception as e:
            logging.error(f"Error setting bot commands: {e}")

    def ensure_directories(self):
        """Ensure all required directories exist at runtime."""
        for directory in [Config.TEMP_FOLDER, Config.PROCESSED_FOLDER]:
            os.makedirs(directory, exist_ok=True)
            logging.info(f"Ensured directory exists: {directory}")

    def get_directory_size(self, directory):
        """Get the size of a directory in bytes."""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
        return total_size

    def monitor_storage(self):
        """Monitor storage usage and clean up if necessary."""
        while True:
            try:
                # Get current sizes
                temp_size = self.get_directory_size(Config.TEMP_FOLDER)
                processed_size = self.get_directory_size(Config.PROCESSED_FOLDER)
                
                # Log current sizes
                temp_size_mb = temp_size / (1024 * 1024)
                processed_size_mb = processed_size / (1024 * 1024)
                total_size_mb = temp_size_mb + processed_size_mb
                
                logging.info(f"Storage monitor: TEMP: {temp_size_mb:.2f}MB, PROCESSED: {processed_size_mb:.2f}MB, TOTAL: {total_size_mb:.2f}MB")
                
                # If we're approaching the limit, perform cleanup
                if total_size_mb > Config.MAX_FOLDER_SIZE_MB * 0.8:
                    logging.warning(f"Storage approaching limit ({total_size_mb:.2f}MB/{Config.MAX_FOLDER_SIZE_MB}MB), performing aggressive cleanup")
                    self.cleanup_temp_folder(aggressive=True)
                    self.cleanup_processed_folder()
                
                # Check every 5 minutes
                time.sleep(300)
            except Exception as e:
                logging.error(f"Error in storage monitor: {e}")
                time.sleep(60)  # Sleep shorter time on error

    def cleanup_temp_folder(self, aggressive=False):
        """Clean up temporary download folder.
        
        Args:
            aggressive: If True, removes all files regardless of age
        """
        try:
            if not os.path.exists(Config.TEMP_FOLDER):
                return
                
            count = 0
            total_size = 0
            now = time.time()
            
            # Get all files sorted by modification time (oldest first)
            files = []
            for f in os.listdir(Config.TEMP_FOLDER):
                file_path = os.path.join(Config.TEMP_FOLDER, f)
                if os.path.isfile(file_path):
                    # Get file stats
                    stats = os.stat(file_path)
                    files.append((file_path, stats.st_mtime, stats.st_size))
            
            # Sort by modification time (oldest first)
            files.sort(key=lambda x: x[1])
            
            # Always remove files older than 2 hours
            cutoff_time = now - 7200  # 2 hours
            
            # If aggressive, remove all files
            if aggressive:
                files_to_remove = files
            else:
                # Otherwise just remove old files
                files_to_remove = [f for f in files if f[1] < cutoff_time]
            
            # Remove files
            for file_path, _, size in files_to_remove:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        count += 1
                        total_size += size
                except Exception as e:
                    logging.error(f"Error removing temp file {file_path}: {e}")
            
            # If we're still over the limit, remove more files
            if not aggressive:
                temp_size = self.get_directory_size(Config.TEMP_FOLDER)
                temp_size_mb = temp_size / (1024 * 1024)
                
                if temp_size_mb > Config.MAX_FOLDER_SIZE_MB * 0.5:
                    logging.warning(f"Temp folder still too large ({temp_size_mb:.2f}MB), performing aggressive cleanup")
                    remaining_files = [f for f in files if os.path.exists(f[0])]
                    remaining_files.sort(key=lambda x: x[1])  # Sort by modification time
                    
                    # Remove oldest files until we're under 50% of the limit
                    for file_path, _, size in remaining_files:
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                count += 1
                                total_size += size
                                
                                # Check if we've removed enough
                                temp_size = self.get_directory_size(Config.TEMP_FOLDER)
                                temp_size_mb = temp_size / (1024 * 1024)
                                if temp_size_mb < Config.MAX_FOLDER_SIZE_MB * 0.3:
                                    break
                        except Exception as e:
                            logging.error(f"Error removing temp file {file_path}: {e}")
            
            if count > 0:
                logging.info(f"Cleaned up temp folder: removed {count} files ({total_size / (1024 * 1024):.2f}MB)")
        except Exception as e:
            logging.error(f"Error cleaning up temp folder: {e}")

    def cleanup_processed_folder(self):
        """Clean up processed files folder."""
        try:
            if not os.path.exists(Config.PROCESSED_FOLDER):
                return
                
            count = 0
            total_size = 0
            now = time.time()
            
            # Get all files sorted by modification time (oldest first)
            files = []
            for f in os.listdir(Config.PROCESSED_FOLDER):
                file_path = os.path.join(Config.PROCESSED_FOLDER, f)
                if os.path.isfile(file_path):
                    # Get file stats
                    stats = os.stat(file_path)
                    files.append((file_path, stats.st_mtime, stats.st_size))
            
            # Sort by modification time (oldest first)
            files.sort(key=lambda x: x[1])
            
            # Remove files older than 1 hour
            cutoff_time = now - 3600  # 1 hour
            
            for file_path, mtime, size in files:
                if mtime < cutoff_time:
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                            count += 1
                            total_size += size
                    except Exception as e:
                        logging.error(f"Error removing processed file {file_path}: {e}")
            
            if count > 0:
                logging.info(f"Cleaned up processed folder: removed {count} files ({total_size / (1024 * 1024):.2f}MB)")
        except Exception as e:
            logging.error(f"Error cleaning up processed folder: {e}")

    def cleanup_task_files(self, task):
        """Clean up any files associated with a task."""
        try:
            if task.file_path and os.path.exists(task.file_path):
                size = os.path.getsize(task.file_path)
                os.remove(task.file_path)
                logging.info(f"Removed file {task.file_path} ({size / (1024 * 1024):.2f}MB)")
                
                # Also clean up any associated files (thumbnails, subtitles, etc.)
                base_path = os.path.splitext(task.file_path)[0]
                for ext in ['.jpg', '.jpeg', '.png', '.webp', '.vtt', '.srt']:
                    thumb_path = f"{base_path}.{ext}"
                    if os.path.exists(thumb_path):
                        os.remove(thumb_path)
                        logging.info(f"Removed associated file {thumb_path}")
        except Exception as e:
            logging.error(f"Error removing task file: {e}")

    def run_periodic_cleanup(self):
        """Run periodic cleanup tasks in the background."""
        while True:
            try:
                # Clean up old tasks from the queue
                self.download_queue.cleanup_old_tasks()
                
                # Check storage threshold and take actions if needed
                storage_result = self.check_storage_threshold()
                if storage_result["action"] != "none":
                    logger.info(f"Periodic cleanup: Storage check result - {storage_result['action']}, usage: {storage_result['usage_human']}")
                else:
                    # If storage threshold not reached, still do basic cleanup
                    self.cleanup_temp_folder()
                    self.cleanup_processed_folder()
                
                # Sleep for 30 minutes
                time.sleep(1800)
            except Exception as e:
                logging.error(f"Error in periodic cleanup: {e}")
                time.sleep(60)  # Sleep shorter time on error

    def process_download_queue(self):
        """Process tasks from the download queue in the background."""
        while True:
            try:
                task = self.download_queue.get_next_task()
                if task:
                    # Process task in a new thread
                    self.executor.submit(self.process_download_task, task)
                else:
                    # No tasks, sleep for a bit
                    time.sleep(1)
            except Exception as e:
                logging.error(f"Error in queue processor: {e}")
                time.sleep(5)

    def process_download_task(self, task):
        """Process a single download task completely."""
        try:
            # Update task status
            task.update_status("downloading")
            
            # Check if the task was cancelled
            if task.status == "cancelled":
                self.download_queue.complete_task(task.id, "cancelled")
                return
            
            # Extract video info
            self.update_task_message(task, "⬇️ Extracting video information...")
            info = self.extract_video_info(task.url)
            if not info:
                self.update_task_message(task, "❌ Failed to extract video info.")
                self.download_queue.complete_task(task.id, "failed", "Failed to extract video info")
                return
            
            # Check if it's a valid YouTube Short
            if not self.is_valid_short(info, task.user_id):
                self.update_task_message(task, "⚠️ Not a valid YouTube Short (duration > 3 minutes).")
                self.download_queue.complete_task(task.id, "failed", "Not a valid YouTube Short")
                return
            
            # Download the video
            self.update_task_message(task, "⬇️ Downloading video...")
            download_result = self.download_video(task, info)
            if not download_result:
                self.update_task_message(task, "❌ Failed to download video.")
                self.download_queue.complete_task(task.id, "failed", "Download failed")
                return
            
            file_path, info = download_result
            task.file_path = file_path
            
            # Check if the task was cancelled
            if task.status == "cancelled":
                self.cleanup_task_files(task)
                self.download_queue.complete_task(task.id, "cancelled")
                return
            
            # Check file size limit
            file_size = os.path.getsize(file_path)
            if file_size > (Config.MAX_VIDEO_SIZE_MB * 1024 * 1024):
                self.update_task_message(task, f"⚠️ Video is too large (>{Config.MAX_VIDEO_SIZE_MB}MB)!")
                self.cleanup_task_files(task)
                self.download_queue.complete_task(task.id, "failed", "File too large")
                return
            
            # Process video if needed (watermark removal, etc.)
            if self.user_prefs.is_watermark_removal_enabled(task.user_id):
                self.update_task_message(task, "🔄 Removing watermark...")
                processed_path = self.process_video(task, file_path)
                if processed_path:
                    # Use processed file instead
                    task.file_path = processed_path
            
            # Extract audio if enabled
            if self.user_prefs.is_audio_extraction_enabled(task.user_id):
                self.update_task_message(task, "🔊 Extracting audio...")
                audio_path = self.extract_audio(task, file_path)
                # Audio will be sent separately
            
            # Upload to Google Drive if enabled
            if self.user_prefs.get_user_prefs(task.user_id).get("gdrive_enabled", True):
                self.update_task_message(task, "☁️ Uploading to Google Drive...")
                drive_link = self.upload_to_drive(task.file_path, os.path.basename(task.file_path))
                task.drive_link = drive_link
            
            # Prepare caption based on user's template
            caption = self.prepare_video_caption(task, info, file_size)
            
            # Send to Telegram
            self.update_task_message(task, "📤 Uploading to Telegram...")
            duration = info.get('duration', 0)
            success = self.send_video_to_telegram(task.chat_id, task.file_path, caption, int(duration))
            
            if success:
                self.update_task_message(task, "✅ Download completed!", delete_after=3)
                # Update user stats
                self.user_stats.update_downloads(task.user_id)
                self.download_queue.complete_task(task.id, "completed")
            else:
                error_text = "⚠️ Failed to send video to Telegram."
                if task.drive_link:
                    error_text += f"\n\n☁️ [Download from Google Drive]({task.drive_link})"
                self.update_task_message(task, error_text)
                self.download_queue.complete_task(task.id, "failed", "Upload to Telegram failed")
            
            # Clean up files
            self.cleanup_task_files(task)
            
        except Exception as e:
            logging.error(f"Error processing task {task.id}: {e}")
            self.update_task_message(task, f"❌ Error: {str(e)}")
            self.download_queue.complete_task(task.id, "failed", str(e))
            self.cleanup_task_files(task)

    def update_task_message(self, task, text, delete_after=None):
        """Update the status message for a task."""
        try:
            if task.message_id:
                self.bot.edit_message_text(
                    text,
                    task.chat_id,
                    task.message_id,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                if delete_after:
                    threading.Timer(delete_after, lambda: delete_message_safe(self.bot, task.chat_id, task.message_id)).start()
        except Exception as e:
            logging.error(f"Error updating task message: {e}")

    def extract_video_info(self, url, retries=Config.RETRY_ATTEMPTS):
        """Extract video information with retries and exponential backoff."""
        for attempt in range(retries):
            try:
                with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
                    return ydl.extract_info(url, download=False)
            except Exception as e:
                logging.error(f"Error extracting info on attempt {attempt+1}: {e}")
                if attempt < retries - 1:
                    # Exponential backoff
                    delay = min(Config.RETRY_DELAY * (2 ** attempt), Config.MAX_RETRY_DELAY)
                    time.sleep(delay)
        return None

    def is_valid_short(self, info, user_id):
        """Check if the video is a valid YouTube Short or within allowed duration limits."""
        # Get user to check if they have admin override
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=str(user_id)).first()
            is_admin = str(user_id) in [str(admin_id) for admin_id in Config.ADMIN_IDS]
            has_admin_override = is_admin and (user and user.admin_override)
            
            # If user has admin override, allow any length
            if has_admin_override:
                return True
            
            duration = info.get('duration', 0)
            if 'shorts' in info.get('webpage_url', '').lower():
                # For YouTube Shorts, use the original 3-minute limit
                return duration <= 180  # 3 minutes max
            else:
                # For regular YouTube videos, check against the configured limit
                return duration <= (Config.MAX_VIDEO_LENGTH_MINUTES * 60)  # Convert minutes to seconds
        except Exception as e:
            logging.error(f"Error checking video validity: {e}")
            # Default to shorts-only if there's an error
            duration = info.get('duration', 0)
            return 'shorts' in info.get('webpage_url', '').lower() and duration <= 180
        finally:
            session.close()

    def download_video(self, task, info, retries=Config.RETRY_ATTEMPTS):
        """Download a video with retry logic and enhanced options."""
        user_id = task.user_id
        url = task.url
        
        # Build yt-dlp format string for highest quality
        is_short = 'shorts' in info.get('webpage_url', '').lower()
        
        # Prepare format selection string to get the best quality
        if is_short:
            # For shorts, always get best quality
            format_spec = "bestvideo+bestaudio/best"
        else:
            # For regular videos, get best quality
            format_spec = "bestvideo+bestaudio/best"
        
        # Set up post-processing options for embedding metadata, thumbnails, etc.
        postprocessors = []
        
        # Add thumbnail embedding if enabled
        if Config.INCLUDE_THUMBNAIL:
            postprocessors.append({
                'key': 'EmbedThumbnail',
            })
        
        # Add subtitle downloading if enabled
        subtitles_opts = {}
        if Config.INCLUDE_SUBTITLES:
            for lang in Config.SUBTITLE_LANGUAGES:
                subtitles_opts[lang] = [{'ext': 'vtt'}]
        
        # Add subtitle embedding post-processor if enabled
        if Config.INCLUDE_SUBTITLES and subtitles_opts:
            postprocessors.append({
                'key': 'FFmpegEmbedSubtitle',
                'already_have_subtitle': False,
            })
        
        # Add chapter embedding if enabled
        if Config.INCLUDE_CHAPTERS:
            postprocessors.append({
                'key': 'FFmpegMetadata',
                'add_chapters': True,
            })
        
        # For embedding metadata
        postprocessors.append({
            'key': 'FFmpegMetadata',
            'add_metadata': True,
        })
        
        # Setup yt-dlp options
        ydl_opts = {
            'format': format_spec,
            'outtmpl': os.path.join(Config.TEMP_FOLDER, f'{user_id}_{int(time.time())}_%(title)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True,
            'socket_timeout': 30,
            'retries': retries,
            'postprocessors': postprocessors,
            'writethumbnail': Config.INCLUDE_THUMBNAIL,
            'embedthumbnail': Config.INCLUDE_THUMBNAIL,
            'writesubtitles': Config.INCLUDE_SUBTITLES,
            'writeautomaticsub': Config.INCLUDE_SUBTITLES,
            'subtitleslangs': Config.SUBTITLE_LANGUAGES if Config.INCLUDE_SUBTITLES else [],
            'embedsubtitles': Config.INCLUDE_SUBTITLES,
            'addchapters': Config.INCLUDE_CHAPTERS,
            'merge_output_format': Config.PREFERRED_FORMAT,  # Use preferred format (mkv)
        }
        
        # Add subtitles configuration if enabled
        if Config.INCLUDE_SUBTITLES and subtitles_opts:
            ydl_opts['subtitleslangs'] = list(subtitles_opts.keys())
        
        for attempt in range(retries):
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    file_path = ydl.prepare_filename(info)
                    
                    # If the extension doesn't match our preferred format due to download issues,
                    # check if the file exists with actual extension
                    if not os.path.exists(file_path):
                        base, _ = os.path.splitext(file_path)
                        for ext in ['.mp4', '.mkv', '.webm']:
                            test_path = base + ext
                            if os.path.exists(test_path):
                                file_path = test_path
                                break
                    
                    if os.path.exists(file_path):
                        return file_path, info
            except Exception as e:
                logging.error(f"Download error on attempt {attempt+1}: {e}")
                if attempt < retries - 1:
                    delay = min(Config.RETRY_DELAY * (2 ** attempt), Config.MAX_RETRY_DELAY)
                    time.sleep(delay)
        
        return None

    def process_video(self, task, file_path):
        """Process video to remove watermarks using ffmpeg."""
        try:
            # Create output path in processed folder
            filename = os.path.basename(file_path)
            output_path = os.path.join(Config.PROCESSED_FOLDER, f"nowm_{filename}")
            
            # Use ffmpeg to crop out watermark area (adjust parameters as needed)
            # This is a simplified example - actual watermark removal might need more complex approach
            cmd = [
                'ffmpeg', '-i', file_path, 
                '-vf', 'crop=in_w:in_h-70:0:0',  # Crop bottom 70 pixels (common watermark location)
                '-c:a', 'copy', output_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if os.path.exists(output_path):
                return output_path
            return None
        except Exception as e:
            logging.error(f"Error processing video: {e}")
            return None

    def extract_audio(self, task, file_path):
        """Extract audio from video file."""
        try:
            # Create output path
            filename = os.path.basename(file_path)
            base_name = os.path.splitext(filename)[0]
            output_path = os.path.join(Config.PROCESSED_FOLDER, f"{base_name}.mp3")
            
            # Use ffmpeg to extract audio
            cmd = [
                'ffmpeg', '-i', file_path, 
                '-q:a', '0', '-map', 'a', output_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if os.path.exists(output_path):
                # Send audio file to user
                with open(output_path, 'rb') as audio:
                    self.bot.send_audio(
                        task.chat_id,
                        audio,
                        caption=f"🎵 Audio extracted from: {base_name}",
                        performer="Shorts Downloader Pro",
                        title=base_name
                    )
                # Clean up audio file
                os.remove(output_path)
                return output_path
            return None
        except Exception as e:
            logging.error(f"Error extracting audio: {e}")
            return None

    def format_date(self, date_str):
        """Format date string from YYYYMMDD to a more readable format."""
        try:
            if not date_str or len(date_str) != 8:
                return "Unknown"
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            return f"{day}/{month}/{year}"
        except Exception:
            return "Unknown"

    def format_number(self, number):
        """Format large numbers with commas for readability."""
        try:
            return intword(number) if number else "0"
        except Exception:
            return str(number)

    def prepare_video_caption(self, task, info, file_size):
        """Prepare video caption based on user's template."""
        video_title = info.get('title', 'YouTube Video')
        duration = info.get('duration', 0)
        duration_str = f"{int(duration//60)}:{int(duration%60):02d}" if duration else "N/A"
        
        # Get format info for the final downloaded format
        format_info = info.get('formats', [])[-1] if info.get('formats') else {}
        
        # Get resolution
        height = format_info.get('height', 1080)
        width = format_info.get('width', 1920)
        fps = format_info.get('fps', 30)
        
        # Determine quality description based on resolution
        if height >= 2160:
            quality = "4K"
        elif height >= 1440:
            quality = "2K/1440p"
        elif height >= 1080:
            quality = "1080p"
        elif height >= 720:
            quality = "720p"
        elif height >= 480:
            quality = "480p"
        else:
            quality = f"{height}p"
        
        # Get file extension
        _, file_ext = os.path.splitext(task.file_path)
        file_ext = file_ext.lstrip('.').upper()
        
        # Get additional features info
        features = []
        if Config.INCLUDE_SUBTITLES and info.get('requested_subtitles'):
            subtitle_langs = list(info.get('requested_subtitles', {}).keys())
            if subtitle_langs:
                features.append(f"Subtitles: {', '.join(subtitle_langs)}")
        
        if Config.INCLUDE_CHAPTERS and info.get('chapters'):
            features.append(f"Chapters: {len(info.get('chapters', []))}")
        
        if Config.INCLUDE_THUMBNAIL:
            features.append("Thumbnail")
        
        if Config.INCLUDE_AUDIO_TRACKS and info.get('requested_formats'):
            audio_tracks = [f for f in info.get('requested_formats', []) if f.get('acodec') != 'none']
            if len(audio_tracks) > 1:
                features.append(f"Multiple Audio Tracks: {len(audio_tracks)}")
        
        features_text = "\n".join([f"• {feature}" for feature in features]) if features else ""
        
        upload_date = self.format_date(info.get('upload_date', ''))
        
        template = self.user_prefs.get_caption_template(task.user_id)
        
        # Replace placeholders in the template
        caption = template.format(
            title=video_title,
            view_count=self.format_number(info.get('view_count', 0)),
            like_count=self.format_number(info.get('like_count', 0)),
            duration=duration_str,
            upload_date=upload_date,
            quality=quality,
            fps=fps,
            size=naturalsize(file_size),
            url=task.url
        )
        
        # Add features information
        if features_text:
            caption += f"\n\n📋 *Additional Features:*\n{features_text}"
        
        # Add format information
        caption += f"\n\n📦 Format: {file_ext} ({width}x{height})"
        
        # Add Google Drive link if available
        if task.drive_link:
            caption += f"\n☁️ [Backup Link]({task.drive_link})"
        
        return caption

    def handle_admin_toggle_override(self, message):
        """Toggle admin override for video length limits."""
        user_id = message.from_user.id
        
        # Check if user is an admin
        if user_id not in Config.ADMIN_IDS:
            self.bot.reply_to(message, "❌ This command is only available for admins.")
            return
        
        session = Session()
        try:
            user = session.query(UserModel).filter_by(user_id=str(user_id)).first()
            
            if not user:
                # Create user if doesn't exist
                user = UserModel(
                    user_id=str(user_id),
                    join_date=datetime.now(),
                    gdrive_enabled=True,
                    admin_override=True
                )
                session.add(user)
                session.commit()
                self.bot.reply_to(message, "✅ Admin override for video length limits has been enabled.")
                return
                
            # Toggle the admin_override flag
            user.admin_override = not user.admin_override
            session.commit()
            
            status = "enabled" if user.admin_override else "disabled"
            self.bot.reply_to(message, f"✅ Admin override for video length limits has been {status}.")
        except Exception as e:
            session.rollback()
            logging.error(f"Error toggling admin override: {e}")
            self.bot.reply_to(message, "❌ An error occurred while toggling admin override.")
        finally:
            session.close()

    def handle_admin(self, message):
        """Handle /admin command - show admin panel"""
        if message.from_user.id not in Config.ADMIN_IDS:
            self.bot.send_message(message.chat.id, "You are not authorized to use this feature")
            return

        temp_size_mb = self.get_folder_size(Config.TEMP_DOWNLOAD_DIR) / (1024 * 1024)
        processed_size_mb = self.get_folder_size(Config.PROCESSED_DIR) / (1024 * 1024)

        # Get queue statistics
        active_count = len(self.task_queue.active_tasks)
        waiting_count = len(self.task_queue.waiting_tasks)
        completed_count = len(self.task_queue.completed_tasks)
        
        # Get system information
        disk = shutil.disk_usage("/")
        free_space_gb = disk.free / (1024**3)
        uptime = datetime.now() - self.start_time
        
        # Create admin panel message
        msg = f"🔧 *Admin Panel*\n\n"
        msg += f"*Storage Stats:*\n"
        msg += f"├ Temp folder: {temp_size_mb:.2f} MB\n"
        msg += f"├ Processed folder: {processed_size_mb:.2f} MB\n"
        msg += f"└ Free space: {free_space_gb:.2f} GB\n\n"
        
        msg += f"*Queue Stats:*\n"
        msg += f"├ Active tasks: {active_count}\n"
        msg += f"├ Waiting tasks: {waiting_count}\n"
        msg += f"└ Completed tasks: {completed_count}\n\n"
        
        msg += f"*System Info:*\n"
        msg += f"├ Bot uptime: {str(uptime).split('.')[0]}\n"
        msg += f"└ Python version: {sys.version.split()[0]}"
        
        # Create admin keyboard
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("🧹 Clean temp", callback_data="admin_clean_temp"),
            types.InlineKeyboardButton("🔄 Restart", callback_data="admin_restart")
        )
        markup.row(
            types.InlineKeyboardButton("📊 Storage", callback_data="admin_storage"),
            types.InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")
        )
        
        self.bot.send_message(message.chat.id, msg, parse_mode="Markdown", reply_markup=markup)

    def handle_storage(self, message):
        """Handle storage command - show detailed storage statistics"""
        if message.from_user.id not in Config.ADMIN_IDS:
            self.bot.send_message(message.chat.id, "You are not authorized to use this feature")
            return
            
        # Get disk usage
        disk = shutil.disk_usage("/")
        total_gb = disk.total / (1024**3)
        used_gb = disk.used / (1024**3)
        free_gb = disk.free / (1024**3)
        used_percent = (disk.used / disk.total) * 100
        
        # Get folder sizes
        temp_size_mb = self.get_folder_size(Config.TEMP_DOWNLOAD_DIR) / (1024 * 1024)
        processed_size_mb = self.get_folder_size(Config.PROCESSED_DIR) / (1024 * 1024)
        
        # Count files
        temp_files = len(os.listdir(Config.TEMP_DOWNLOAD_DIR)) if os.path.exists(Config.TEMP_DOWNLOAD_DIR) else 0
        processed_files = len(os.listdir(Config.PROCESSED_DIR)) if os.path.exists(Config.PROCESSED_DIR) else 0
        
        # Create storage report message
        msg = f"📊 *Storage Report*\n\n"
        msg += f"*Disk Usage:*\n"
        msg += f"├ Total: {total_gb:.2f} GB\n"
        msg += f"├ Used: {used_gb:.2f} GB ({used_percent:.1f}%)\n"
        msg += f"└ Free: {free_gb:.2f} GB\n\n"
        
        msg += f"*Application Folders:*\n"
        msg += f"├ Temp folder: {temp_size_mb:.2f} MB ({temp_files} files)\n"
        msg += f"└ Processed folder: {processed_size_mb:.2f} MB ({processed_files} files)\n\n"
        
        # Add warning if storage is low
        if free_gb < 1.0:
            msg += "⚠️ *Warning:* Free storage is critically low!\n"
        elif free_gb < 3.0:
            msg += "⚠️ *Warning:* Free storage is running low!\n"
        
        # Create keyboard
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("🧹 Clean temp", callback_data="admin_clean_temp"),
            types.InlineKeyboardButton("↩️ Back to admin", callback_data="admin_back")
        )
        
        self.bot.send_message(message.chat.id, msg, parse_mode="Markdown", reply_markup=markup)

    def get_folder_size(self, folder_path):
        """Calculate total size of a folder in bytes"""
        total_size = 0
        if os.path.exists(folder_path):
            for dirpath, dirnames, filenames in os.walk(folder_path):
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    if os.path.isfile(file_path):
                        total_size += os.path.getsize(file_path)
        return total_size

    def register_handlers(self):
        @self.bot.message_handler(commands=['start'])
        def start_wrapper(message):
            try:
                self.handle_start(message)
            except Exception as e:
                logging.error(f"Error in start handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")

        @self.bot.message_handler(commands=['help'])
        def help_wrapper(message):
            try:
                self.handle_help(message)
            except Exception as e:
                logging.error(f"Error in help handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")

        @self.bot.message_handler(commands=['stats'])
        def stats_wrapper(message):
            try:
                self.handle_stats(message)
            except Exception as e:
                logging.error(f"Error in stats handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")

        @self.bot.message_handler(commands=['donate'])
        def donate_wrapper(message):
            try:
                self.handle_donate(message)
            except Exception as e:
                logging.error(f"Error in donate handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")

        @self.bot.message_handler(commands=['about'])
        def about_wrapper(message):
            try:
                self.handle_about(message)
            except Exception as e:
                logging.error(f"Error in about handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")

        @self.bot.message_handler(commands=['settings'])
        def settings_wrapper(message):
            try:
                self.handle_settings(message)
            except Exception as e:
                logging.error(f"Error in settings handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")

        @self.bot.message_handler(commands=['version'])
        def version_wrapper(message):
            try:
                self.handle_version(message)
            except Exception as e:
                logging.error(f"Error in version handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")

        @self.bot.message_handler(commands=['togglegdrive'])
        def toggle_gdrive_wrapper(message):
            try:
                new_pref = self.user_stats.toggle_gdrive_preference(message.from_user.id)
                status = "enabled" if new_pref else "disabled"
                self.bot.send_message(message.chat.id, f"✅ Google Drive backup has been {status}.")
            except Exception as e:
                logging.error(f"Error in togglegdrive handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")
                
        @self.bot.message_handler(commands=['admin_override'])
        def admin_override_wrapper(message):
            try:
                self.handle_admin_toggle_override(message)
            except Exception as e:
                logging.error(f"Error in admin override handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")
                
        @self.bot.message_handler(commands=['admin'])
        def admin_wrapper(message):
            try:
                self.handle_admin(message)
            except Exception as e:
                logging.error(f"Error in admin handler: {e}")
                self.bot.reply_to(message, "An error occurred. Please try again.")
                
        @self.bot.message_handler(commands=['storage'])
        def storage_wrapper(message):
            try:
                # Only allow admin access
                if message.from_user.id not in Config.ADMIN_IDS:
                    self.bot.reply_to(message, "❌ This command is only available for admins.")
                    return
                    
                # Get storage information
                temp_size = self.get_directory_size(Config.TEMP_FOLDER)
                temp_size_mb = temp_size / (1024 * 1024)
                processed_size = self.get_directory_size(Config.PROCESSED_FOLDER)
                processed_size_mb = processed_size / (1024 * 1024)
                total_size_mb = temp_size_mb + processed_size_mb
                
                # Count files in each directory
                temp_files = len([f for f in os.listdir(Config.TEMP_FOLDER) if os.path.isfile(os.path.join(Config.TEMP_FOLDER, f))])
                processed_files = len([f for f in os.listdir(Config.PROCESSED_FOLDER) if os.path.isfile(os.path.join(Config.PROCESSED_FOLDER, f))])
                
                storage_text = (
                    "💾 *Storage Report*\n\n"
                    f"📁 *Temp Folder:*\n"
                    f"• Size: {temp_size_mb:.2f}MB\n"
                    f"• Files: {temp_files}\n\n"
                    f"📁 *Processed Folder:*\n"
                    f"• Size: {processed_size_mb:.2f}MB\n"
                    f"• Files: {processed_files}\n\n"
                    f"📊 *Total Storage:*\n"
                    f"• Used: {total_size_mb:.2f}MB/{Config.MAX_FOLDER_SIZE_MB}MB\n"
                    f"• Percentage: {(total_size_mb/Config.MAX_FOLDER_SIZE_MB*100):.1f}%\n"
                    f"• Free: {(Config.MAX_FOLDER_SIZE_MB - total_size_mb):.2f}MB\n\n"
                    "Use /cleanstorage to perform cleanup"
                )
                
                self.bot.send_message(message.chat.id, storage_text, parse_mode='Markdown')
            except Exception as e:
                logging.error(f"Error in storage handler: {e}")
                self.bot.reply_to(message, "An error occurred while getting storage information.")
                
        @self.bot.message_handler(commands=['cleanstorage'])
        def cleanstorage_wrapper(message):
            try:
                # Only allow admin access
                if message.from_user.id not in Config.ADMIN_IDS:
                    self.bot.reply_to(message, "❌ This command is only available for admins.")
                    return
                
                # Send status message
                status_msg = self.bot.reply_to(message, "🧹 Cleaning storage...")
                
                # Clean up temp and processed folders
                self.cleanup_temp_folder(aggressive=True)
                self.cleanup_processed_folder()
                
                # Update status
                temp_size = self.get_directory_size(Config.TEMP_FOLDER)
                temp_size_mb = temp_size / (1024 * 1024)
                processed_size = self.get_directory_size(Config.PROCESSED_FOLDER)
                processed_size_mb = processed_size / (1024 * 1024)
                total_size_mb = temp_size_mb + processed_size_mb
                
                self.bot.edit_message_text(
                    f"✅ Storage cleaned successfully!\n\n"
                    f"Current usage: {total_size_mb:.2f}MB/{Config.MAX_FOLDER_SIZE_MB}MB "
                    f"({(total_size_mb/Config.MAX_FOLDER_SIZE_MB*100):.1f}%)",
                    message.chat.id,
                    status_msg.message_id
                )
            except Exception as e:
                logging.error(f"Error in cleanstorage handler: {e}")
                self.bot.reply_to(message, "An error occurred while cleaning storage.")

        @self.bot.message_handler(commands=['cancel'])
        def cancel_wrapper(message):
            try:
                user_id = message.from_user.id
                cancel_flags[user_id] = True
                file_path = current_tasks.get(user_id)
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    logging.info(f"Cancelled and removed file for user {user_id}")
                self.bot.send_message(message.chat.id, "🛑 All current tasks cancelled.", parse_mode='Markdown')
            except Exception as e:
                logging.error(f"Error in cancel command: {e}")
                self.bot.reply_to(message, "❌ Error cancelling tasks.")

        @self.bot.message_handler(commands=['restart'])
        def restart_wrapper(message):
            try:
                if message.from_user.id not in Config.ADMIN_IDS:
                    self.bot.send_message(message.chat.id, "❌ You are not authorized to use this command.")
                    return
                msg = self.bot.send_message(message.chat.id, "🔄 Restarting bot as requested...")
                threading.Timer(120, lambda: delete_message_safe(self.bot, message.chat.id, msg.message_id)).start()
                threading.Timer(2, restart_bot).start()
            except Exception as e:
                logging.error(f"Error in restart handler: {e}")
                self.bot.reply_to(message, "❌ Error restarting the bot.")

        @self.bot.message_handler(
            func=lambda message: message.text and (
                any(domain in message.text.lower() for domain in ["youtube.com/shorts", "youtu.be"]) or
                "youtube.com/watch" in message.text.lower()
            )
        )
        def youtube_wrapper(message):
            try:
                self.handle_youtube_short(message)
            except Exception as e:
                logging.error(f"Error in YouTube handler: {e}")
                self.bot.reply_to(message, "An error occurred while processing the video. Please try again.")
                
        @self.bot.callback_query_handler(func=lambda call: True)
        def callback_wrapper(call):
            try:
                if call.data == "help":
                    self.handle_help(call.message)
                elif call.data == "stats":
                    self.handle_stats(call.message)
                elif call.data == "admin_clean_temp":
                    if call.from_user.id in Config.ADMIN_IDS:
                        self.cleanup_temp_folder(aggressive=True)
                        self.bot.answer_callback_query(call.id, "Temp folder cleaned successfully")
                        # Refresh admin panel
                        self.handle_admin(call.message)
                    else:
                        self.bot.answer_callback_query(call.id, "You are not authorized to use this feature", show_alert=True)
                elif call.data == "admin_restart":
                    if call.from_user.id in Config.ADMIN_IDS:
                        self.bot.answer_callback_query(call.id, "Restarting bot...")
                        msg = self.bot.send_message(call.message.chat.id, "🔄 Restarting bot as requested...")
                        threading.Timer(120, lambda: delete_message_safe(self.bot, call.message.chat.id, msg.message_id)).start()
                        threading.Timer(2, restart_bot).start()
                    else:
                        self.bot.answer_callback_query(call.id, "You are not authorized to use this feature", show_alert=True)
                elif call.data == "admin_storage":
                    if call.from_user.id in Config.ADMIN_IDS:
                        self.bot.answer_callback_query(call.id)
                        self.bot.delete_message(call.message.chat.id, call.message.message_id)
                        self.handle_storage(call.message)
                    else:
                        self.bot.answer_callback_query(call.id, "You are not authorized to use this feature", show_alert=True)
                else:
                    self.bot.answer_callback_query(call.id)
            except Exception as e:
                logging.error(f"Callback error: {e}")
                self.bot.answer_callback_query(
                    call.id,
                    "❌ Error processing request",
                    show_alert=True
                )

    def handle_callback(self, call):
        """Handle callback queries from inline keyboards"""
        try:
            # Admin callbacks require authorization
            if call.data.startswith('admin_') and call.from_user.id not in Config.ADMIN_IDS:
                self.bot.answer_callback_query(call.id, "You are not authorized to use admin features.")
                return

            if call.data == "admin_clean_temp":
                self.handle_clean_temp(call.message)
                self.bot.answer_callback_query(call.id, "Temp folder cleaned!")
            elif call.data == "admin_restart":
                self.bot.answer_callback_query(call.id, "Restarting bot...")
                self.handle_restart(call.message)
            elif call.data == "admin_storage":
                self.bot.answer_callback_query(call.id, "Generating storage report...")
                self.handle_storage(call.message)
            elif call.data == "admin_settings":
                self.bot.answer_callback_query(call.id, "Opening settings menu...")
                self.handle_settings(call.message)
            elif call.data == "admin_back":
                self.bot.answer_callback_query(call.id, "Returning to admin panel...")
                self.handle_admin(call.message)
            else:
                # Handle other callbacks...
                pass
        except Exception as e:
            logging.error(f"Error in handle_callback: {e}")
            self.bot.answer_callback_query(call.id, "An error occurred")

    def handle_clean_temp(self, call):
        """Handle manual cleaning of temporary files"""
        try:
            # Show "cleaning in progress" message
            self.bot.edit_message_text(
                "🧹 Cleaning temporary files...",
                call.message.chat.id,
                call.message.message_id
            )
            
            # Clean all temp files (0 hours = clean everything)
            removed_count, freed_space = self.cleanup_old_files(Config.TEMP_DOWNLOAD_DIR, hours=0)
            
            # Format the freed space
            if freed_space < 1024:
                space_str = f"{freed_space} bytes"
            elif freed_space < 1024 * 1024:
                space_str = f"{freed_space/1024:.2f} KB"
            elif freed_space < 1024 * 1024 * 1024:
                space_str = f"{freed_space/(1024*1024):.2f} MB"
            else:
                space_str = f"{freed_space/(1024*1024*1024):.2f} GB"
            
            # Show completion message
            result_msg = f"✅ Cleaning complete!\n\nRemoved: {removed_count} files\nFreed: {space_str}"
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.row(
                telebot.types.InlineKeyboardButton("↩️ Back", callback_data="admin_storage"),
                telebot.types.InlineKeyboardButton("🏠 Admin Panel", callback_data="admin_panel")
            )
            
            self.bot.edit_message_text(
                result_msg,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )
            
            logging.info(f"Manual cleanup completed: {removed_count} files removed, {space_str} freed")
        except Exception as e:
            error_msg = f"Error during manual cleanup: {e}"
            logging.error(error_msg)
            self.bot.answer_callback_query(call.id, "❌ Cleanup failed")
            try:
                self.bot.edit_message_text(
                    f"❌ Cleanup failed: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=telebot.types.InlineKeyboardMarkup().add(
                        telebot.types.InlineKeyboardButton("Back to Admin", callback_data="admin_panel")
                    )
                )
            except:
                pass

    def handle_restart(self, message):
        """Restart the bot"""
        if message.from_user.id not in Config.ADMIN_IDS:
            self.bot.send_message(message.chat.id, "You are not authorized to use this feature")
            return
            
        try:
            self.bot.send_message(message.chat.id, "🔄 Restarting bot...")
            logging.info("Bot restart requested by admin")
            
            # Execute restart
            python = sys.executable
            os.execl(python, python, *sys.argv)
            
        except Exception as e:
            logging.error(f"Error restarting bot: {e}")
            self.bot.send_message(message.chat.id, f"Error restarting bot: {str(e)}")

    def handle_settings(self, message):
        """Show settings menu"""
        if message.from_user.id not in Config.ADMIN_IDS:
            self.bot.send_message(message.chat.id, "You are not authorized to use this feature")
            return
            
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("↩️ Back to admin", callback_data="admin_back")
        )
        
        self.bot.send_message(
            message.chat.id,
            "⚙️ *Settings Menu*\n\nSettings configuration will be implemented soon.",
            parse_mode="Markdown",
            reply_markup=markup
        )

    def ensure_directories(self):
        """Ensure all required directories exist"""
        for directory in [Config.TEMP_FOLDER, Config.PROCESSED_FOLDER]:
            os.makedirs(directory, exist_ok=True)
            logging.info(f"Ensured directory exists: {directory}")
    
    def start_auto_cleanup(self):
        """Start auto cleanup process in background thread"""
        cleanup_thread = threading.Thread(target=self.auto_cleanup_loop)
        cleanup_thread.daemon = True
        cleanup_thread.start()
        logging.info("Started auto cleanup background thread")
    
    def auto_cleanup_loop(self):
        """Background loop to periodically clean old temporary files"""
        while True:
            try:
                # Clean files older than 24 hours
                self.cleanup_old_files(Config.TEMP_DOWNLOAD_DIR, hours=24)
                # Sleep for 3 hours before next cleanup
                time.sleep(3 * 60 * 60)
            except Exception as e:
                logging.error(f"Error in auto cleanup: {e}")
                # If error occurs, sleep for a shorter time
                time.sleep(30 * 60)
    
    def cleanup_old_files(self, directory, days=None, hours=None):
        """
        Remove files older than specified time from directory
        Args:
            directory: Directory to clean
            days: Remove files older than this many days
            hours: Remove files older than this many hours
        
        Returns:
            (number of files removed, bytes freed)
        """
        if not os.path.exists(directory):
            return 0, 0
            
        # Calculate the cutoff time
        if days is not None:
            cutoff_time = time.time() - (days * 24 * 60 * 60)
        elif hours is not None:
            cutoff_time = time.time() - (hours * 60 * 60)
        else:
            # Default to 7 days if not specified
            cutoff_time = time.time() - (7 * 24 * 60 * 60)
            
        removed_count = 0
        freed_space = 0
        
        for file_path in glob.glob(f"{directory}/*"):
            if os.path.isfile(file_path):
                # Get file modification time
                mod_time = os.path.getmtime(file_path)
                
                # If file is older than cutoff time, remove it
                if mod_time < cutoff_time:
                    try:
                        # Get file size before deleting
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        removed_count += 1
                        freed_space += file_size
                    except Exception as e:
                        logger.error(f"Failed to remove file {file_path}: {e}")
        
        return removed_count, freed_space
        
    def format_size(self, size_bytes):
        """Format bytes to human-readable size"""
        if size_bytes == 0:
            return "0B"
        
        size_names = ("B", "KB", "MB", "GB", "TB", "PB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        
        return f"{s} {size_names[i]}"

    def get_storage_info(self):
        """Get storage information for the system"""
        # Get the disk usage for the partition where the bot is running
        disk_usage = shutil.disk_usage(os.path.abspath(os.curdir))
        
        storage_info = {
            'total': disk_usage.total,
            'used': disk_usage.used,
            'free': disk_usage.free,
            'percent': (disk_usage.used / disk_usage.total) * 100
        }
        
        return storage_info

    def check_storage_threshold(self):
        """
        Check storage usage thresholds and take appropriate actions
        Returns a status dict with actions taken
        """
        storage_info = self.get_storage_info()
        usage_percent = storage_info["percent"]
        
        # Define thresholds
        warning_threshold = 75    # Warning level - log only
        action_threshold = 85     # Action level - delete old processed files
        critical_threshold = 95   # Critical level - aggressive cleanup + admin notification
        
        result = {
            "action": "none",
            "usage_percent": usage_percent,
            "usage_human": f"{usage_percent:.1f}%"
        }
        
        # No action needed
        if usage_percent < warning_threshold:
            return result
            
        # Warning threshold reached - just log
        if warning_threshold <= usage_percent < action_threshold:
            result["action"] = "warning"
            logger.warning(f"Storage warning: {usage_percent:.1f}% used")
            return result
            
        # Action threshold reached - clean processed files older than 7 days
        if action_threshold <= usage_percent < critical_threshold:
            removed_count, freed_space = self.cleanup_old_files(Config.PROCESSED_FOLDER, days=7)
            result.update({
                "action": "action_cleanup",
                "removed_files": removed_count,
                "freed_space": freed_space
            })
            logger.warning(f"Storage action: {usage_percent:.1f}% used - cleaned {removed_count} files, freed {self.format_size(freed_space)}")
            return result
            
        # Critical threshold reached - aggressive cleanup
        if usage_percent >= critical_threshold:
            # First clean temp files older than 6 hours
            removed_temp, freed_temp = self.cleanup_old_files(Config.TEMP_FOLDER, hours=6)
            
            # Then clean processed files older than 2 days
            removed_processed, freed_processed = self.cleanup_old_files(Config.PROCESSED_FOLDER, days=2)
            
            total_removed = removed_temp + removed_processed
            total_freed = freed_temp + freed_processed
            
            result.update({
                "action": "critical_cleanup",
                "removed_files": total_removed,
                "freed_space": total_freed
            })
            
            logger.critical(f"CRITICAL storage: {usage_percent:.1f}% used - cleaned {total_removed} files, freed {self.format_size(total_freed)}")
            return result
            
        return result
        
    def format_storage_report(self, storage_info):
        """Format storage information into a readable report"""
        total = storage_info['total']
        used = storage_info['used']
        free = storage_info['free']
        percent = storage_info['percent']
        
        # Format sizes
        total_human = self.format_size(total)
        used_human = self.format_size(used)
        free_human = self.format_size(free)
        
        # Calculate directory sizes
        dir_sizes = {}
        for directory in [Config.TEMP_DOWNLOAD_DIR, Config.PROCESSED_DIR, Config.DATA_DIR]:
            dir_size = self.get_directory_size(directory)
            dir_sizes[directory] = self.format_size(dir_size)
        
        # Create report
        report = f"📊 Storage Report:\n\n"
        report += f"📝 Usage Summary:\n"
        report += f"- Total Space: {total_human}\n"
        report += f"- Used: {used_human} ({percent:.1f}%)\n"
        report += f"- Free: {free_human}\n\n"
        
        report += f"📂 Directory Sizes:\n"
        for directory, size in dir_sizes.items():
            report += f"- {os.path.basename(directory)}: {size}\n"
            
        return report
        
    def get_directory_size(self, directory):
        """Calculate the total size of a directory in bytes"""
        total_size = 0
        if os.path.exists(directory):
            for dirpath, _, filenames in os.walk(directory):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if not os.path.islink(filepath):
                        total_size += os.path.getsize(filepath)
        return total_size
    
    def send_admin_message(self, message):
        """Send a message to the admin user"""
        admin_id = Config.ADMIN_USER_ID
        if admin_id:
            try:
                self.updater.bot.send_message(chat_id=admin_id, text=message, parse_mode="HTML")
                logger.info(f"Admin notification sent to {admin_id}")
            except Exception as e:
                logger.error(f"Failed to send admin message: {e}")
        else:
            logger.warning("Admin notification not sent: ADMIN_USER_ID not configured")
    
    def format_size(self, size_bytes):
        """Format size in bytes to human-readable format"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.2f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"

    def handle_storage_info(self, message):
        """Handle request for storage information"""
        try:
            storage_info = self.get_storage_info()
            report = self.format_storage_report(storage_info)
            markup = telebot.types.InlineKeyboardMarkup()
            markup.row(
                telebot.types.InlineKeyboardButton("🧹 Clean Temp", callback_data="admin_clean_temp"),
                telebot.types.InlineKeyboardButton("Back to Admin", callback_data="admin_panel")
            )
            self.bot.send_message(message.chat.id, report, reply_markup=markup, parse_mode="Markdown")
            logging.info(f"Storage report sent to {message.chat.id}")
        except Exception as e:
            error_msg = f"Error generating storage report: {e}"
            logging.error(error_msg)
            self.bot.send_message(message.chat.id, f"❌ {error_msg}")

    def show_admin_panel(self, message):
        """Display the admin panel with available admin actions"""
        markup = telebot.types.InlineKeyboardMarkup()
        
        # First row of buttons
        markup.row(
            telebot.types.InlineKeyboardButton("📊 Storage", callback_data="admin_storage"),
            telebot.types.InlineKeyboardButton("🧹 Clean Temp", callback_data="admin_clean_temp")
        )
        
        # Second row of buttons
        markup.row(
            telebot.types.InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"),
            telebot.types.InlineKeyboardButton("🔄 Restart", callback_data="admin_restart")
        )
        
        # Send the admin panel
        self.bot.send_message(
            message.chat.id,
            "🛠️ *Admin Panel*\nSelect an option:",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        logging.info(f"Admin panel shown to {message.chat.id}")

    def handle_storage_check(self):
        """Perform storage check and notify admin if thresholds are reached"""
        logger.info("Performing scheduled storage check")
        result = self.check_storage_threshold()
        
        # If we reached action or critical threshold, notify admin
        if result["action"] in ["action_cleanup", "critical_cleanup"]:
            storage_info = self.get_storage_info()
            report = self.format_storage_report(storage_info)
            
            # Add cleanup information
            if "removed_files" in result:
                freed_space = self.format_size(result["freed_space"])
                report += f"\n\n🧹 Auto-cleanup performed:\n- Removed {result['removed_files']} files\n- Freed {freed_space}"
            
            # Send notification to admin
            self.send_admin_message(f"⚠️ Storage Alert ({result['usage_human']} used)\n\n{report}")
        
        return result

    def ensure_directories(self):
        """Ensure all required directories exist"""
        directories = [
            Config.TEMP_DOWNLOAD_DIR,
            Config.PROCESSED_DIR,
            Config.DATA_DIR
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                logger.info(f"Creating directory: {directory}")
                os.makedirs(directory, exist_ok=True)

    def start(self):
        """Start the bot and scheduler"""
        try:
            # Ensure directories exist
            self.ensure_directories()
            
            # Initialize scheduler
            self.scheduler = BackgroundScheduler()
            
            # Perform initial cleanup of temp files
            logger.info("Performing initial cleanup of temporary files")
            count, freed = self.cleanup_old_files(Config.TEMP_FOLDER, hours=24)
            if count > 0:
                logger.info(f"Cleaned {count} old files, freed {self.format_size(freed)}")
            
            # Check storage status on startup
            logger.info("Checking storage status on startup")
            storage_result = self.check_storage_threshold()
            if storage_result["action"] != "none":
                logger.warning(f"Storage check on startup: {storage_result['usage_human']} used, action: {storage_result['action']}")
            
            # Schedule storage check every 2 hours
            self.scheduler.add_job(
                self.handle_storage_check, 
                'interval', 
                hours=2, 
                id='storage_check',
                next_run_time=datetime.now() + timedelta(minutes=5)  # First check after 5 minutes
            )
            
            # Start the scheduler
            self.scheduler.start()
            logger.info("Scheduler started")
            
            logger.info("Starting bot polling...")
            self.updater.start_polling()
            self.updater.idle()
            
        except Exception as e:
            logger.error(f"Error starting bot: {e}")

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(Config.LOG_FORMAT)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def main():
    try:
        logging.info("Starting Shorts Downloader Pro Bot")
        
        # Handle client_secret.json for Google OAuth
        if 'GOOGLE_CREDENTIALS' in os.environ:
            # Railway provides secrets as environment variables
            # Save the credentials to a file for oauth2client
            with open('client_secrets.json', 'w') as f:
                f.write(os.environ['GOOGLE_CREDENTIALS'])
            logging.info("Google credentials loaded from environment variable")
            
        bot_instance = ShortsDownloaderBot()
        logging.info("Bot initialized successfully")
        
        # Start the bot with our enhanced start method
        # This will initialize directories, scheduler, and storage checks
        bot_instance.start()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.critical(f"Fatal bot error: {e}")
        raise

if __name__ == "__main__":
    main()
