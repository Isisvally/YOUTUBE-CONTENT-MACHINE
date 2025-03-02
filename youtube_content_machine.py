"""
YouTube Shorts Automation Machine
Robust, scalable system for automated YouTube content creation
"""

import os
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
import requests
from moviepy.editor import VideoFileClip, TextClip, CompositeVideoClip, AudioFileClip
from PIL import Image, ImageDraw, ImageFont
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

# Configuration
load_dotenv()
BASE_DIR = Path(__file__).parent
CONTENT_DIR = BASE_DIR / "content"
TEMP_DIR = BASE_DIR / "temp"
LOG_DIR = BASE_DIR / "logs"

# Ensure directories exist
for d in [CONTENT_DIR, TEMP_DIR, LOG_DIR]:
    d.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "youtube_machine.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Config:
    """Configuration manager with validation"""
    def __init__(self):
        self.pexels_api_key = os.getenv("PEXELS_API_KEY")
        self.youtube_client_secret = os.getenv("YOUTUBE_CLIENT_SECRET")
        self.youtube_client_id = os.getenv("YOUTUBE_CLIENT_ID")
        self.youtube_refresh_token = os.getenv("YOUTUBE_REFRESH_TOKEN")
        self.default_hashtags = ["#Shorts", "#Viral", "#Trending"]
        
        self._validate()

    def _validate(self):
        """Ensure critical configuration exists"""
        if not all([self.pexels_api_key, self.youtube_client_id,
                   self.youtube_client_secret, self.youtube_refresh_token]):
            raise EnvironmentError("Missing required environment variables")

class ContentDownloader:
    """Handles media acquisition from various sources"""
    def __init__(self):
        self.config = Config()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Youtube Automation Machine/1.0"})

    def _download_file(self, url: str, filename: str) -> Path:
        """Generic file downloader with retry logic"""
        path = CONTENT_DIR / filename
        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                with open(path, "wb") as f:
                    f.write(response.content)
                return path
            except requests.exceptions.RequestException as e:
                logging.warning(f"Download attempt {attempt+1} failed: {str(e)}")
                continue
        raise ConnectionError(f"Failed to download {url} after 3 attempts")

    def get_pexels_video(self, query: str, duration: int = 15) -> Path:
        """Fetch random video from Pexels matching criteria"""
        params = {
            "query": query,
            "orientation": "portrait",
            "per_page": 20,
            "min_duration": duration-2,
            "max_duration": duration+2
        }
        try:
            response = self.session.get(
                "https://api.pexels.com/videos/search",
                headers={"Authorization": self.config.pexels_api_key},
                params=params
            )
            response.raise_for_status()
            videos = response.json().get("videos", [])
            if not videos:
                raise ValueError("No videos found matching criteria")
            
            video = random.choice(videos)
            video_file = video["video_files"][0]["link"]
            return self._download_file(video_file, f"pexels_{video['id']}.mp4")
        except Exception as e:
            logging.error(f"Pexels video fetch failed: {str(e)}")
            raise

class VideoEditor:
    """Handles video processing and editing tasks"""
    def __init__(self):
        self.config = Config()
        self.font = str(BASE_DIR / "assets" / "Roboto-Bold.ttf")  # Ensure font exists

    def process_video(self, input_path: Path, output_path: Path, text: str) -> Path:
        """Main video processing pipeline"""
        try:
            # Resize and format
            clip = VideoFileClip(str(input_path))
            clip = clip.resize(height=1920)  # Standard Shorts height
            clip = clip.crop(x1=clip.w/2-540, y1=0, x2=clip.w/2+540, y2=1920)

            # Add text overlay
            text_clip = TextClip(
                text,
                fontsize=60,
                color="white",
                font=self.font,
                stroke_color="black",
                stroke_width=2
            ).set_position("center").set_duration(clip.duration)
            
            final_clip = CompositeVideoClip([clip, text_clip])

            # Add background music
            if (CONTENT_DIR / "music").exists():
                music = random.choice(list((CONTENT_DIR / "music").glob("*.mp3")))
                audio_clip = AudioFileClip(str(music)).volumex(0.3)
                final_clip = final_clip.set_audio(audio_clip)

            final_clip.write_videofile(
                str(output_path),
                codec="libx264",
                audio_codec="aac",
                threads=4,
                preset="fast",
                logger=None
            )
            return output_path
        except Exception as e:
            logging.error(f"Video processing failed: {str(e)}")
            raise

class ThumbnailGenerator:
    """Automated thumbnail creation system"""
    def __init__(self):
        self.font = ImageFont.truetype(str(BASE_DIR / "assets" / "Roboto-Bold.ttf"), 100)
        self.text_color = (255, 255, 255)
        self.stroke_color = (0, 0, 0)

    def generate(self, video_path: Path, output_path: Path, text: str) -> Path:
        """Create thumbnail with text overlay"""
        try:
            # Extract frame
            clip = VideoFileClip(str(video_path))
            timestamp = random.uniform(1, clip.duration-1)
            frame = clip.get_frame(timestamp)
            
            # Process image
            img = Image.fromarray(frame)
            draw = ImageDraw.Draw(img)
            
            # Text positioning with stroke
            bbox = draw.textbbox((0,0), text, font=self.font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            position = ((img.width - text_width)//2, (img.height - text_height)//2)

            # Draw stroke multiple times for thickness
            for offset in [(-2,-2), (-2,2), (2,-2), (2,2)]:
                draw.text(
                    (position[0]+offset[0], position[1]+offset[1]),
                    text,
                    font=self.font,
                    fill=self.stroke_color
                )
            
            # Draw main text
            draw.text(position, text, font=self.font, fill=self.text_color)
            
            img.save(output_path)
            return output_path
        except Exception as e:
            logging.error(f"Thumbnail generation failed: {str(e)}")
            raise

class YouTubeUploader:
    """Handles YouTube API integration"""
    def __init__(self):
        self.config = Config()
        self.service = self._authenticate()

    def _authenticate(self):
        """OAuth2 authentication flow"""
        credentials = Credentials(
            token=None,
            refresh_token=self.config.youtube_refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=self.config.youtube_client_id,
            client_secret=self.config.youtube_client_secret
        )
        return build("youtube", "v3", credentials=credentials)

    def upload_video(self, video_path: Path, metadata: Dict, thumbnail_path: Optional[Path] = None) -> str:
        """Upload video with metadata handling"""
        try:
            body = {
                "snippet": {
                    "title": metadata["title"],
                    "description": self._build_description(metadata),
                    "categoryId": "24",  # Entertainment category
                    "tags": metadata.get("tags", [])
                },
                "status": {
                    "privacyStatus": "public",
                    "selfDeclaredMadeForKids": False
                }
            }

            media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
            request = self.service.videos().insert(
                part=",".join(body.keys()),
                body=body,
                media_body=media
            )
            response = self._execute_upload(request)
            
            if thumbnail_path:
                self.service.thumbnails().set(
                    videoId=response["id"],
                    media_body=MediaFileUpload(thumbnail_path)
                ).execute()

            return response["id"]
        except Exception as e:
            logging.error(f"Upload failed: {str(e)}")
            raise

    def _build_description(self, metadata: Dict) -> str:
        """Construct video description from template"""
        return "\n".join([
            metadata.get("description", ""),
            "",
            " ".join(self.config.default_hashtags),
            "#" + metadata.get("niche", "content").replace(" ", "")
        ])

    def _execute_upload(self, request) -> Dict:
        """Handle resumable upload with progress tracking"""
        response = None
        while not response:
            status, response = request.next_chunk()
            if status:
                logging.info(f"Upload progress: {int(status.progress() * 100)}%")
        return response

class ContentScheduler:
    """Orchestrates the entire automation pipeline"""
    def __init__(self):
        self.downloader = ContentDownloader()
        self.editor = VideoEditor()
        self.thumbnailer = ThumbnailGenerator()
        self.uploader = YouTubeUploader()
        self.scheduler = BackgroundScheduler()

    def run_pipeline(self, niche: str, query: str):
        """Complete content creation pipeline"""
        try:
            # Content creation
            video_path = self.downloader.get_pexels_video(query)
            edited_path = self.editor.process_video(
                video_path,
                TEMP_DIR / "processed.mp4",
                text="5 SECONDS HACKS! 🚀"
            )
            thumbnail_path = self.thumbnailer.generate(
                edited_path,
                TEMP_DIR / "thumbnail.jpg",
                text="WATCH NOW!"
            )

            # Metadata generation
            metadata = {
                "title": f"5 Second {niche.capitalize()} Hacks! 🚀",
                "description": f"Amazing {niche} tips you need to try!",
                "tags": [niche, "shorts", "viral"],
                "niche": niche
            }

            # Upload
            video_id = self.uploader.upload_video(edited_path, metadata, thumbnail_path)
            logging.info(f"Successfully uploaded video ID: {video_id}")

            # Cleanup
            for f in [video_path, edited_path, thumbnail_path]:
                f.unlink(missing_ok=True)

            return video_id
        except Exception as e:
            logging.error(f"Pipeline failed: {str(e)}")
            raise

    def schedule_daily_uploads(self, niches: List[Dict]):
        """Schedule regular uploads using APScheduler"""
        for niche_config in niches:
            self.scheduler.add_job(
                self.run_pipeline,
                "cron",
                **niche_config["schedule"],
                args=[niche_config["niche"], niche_config["query"]]
            )
        self.scheduler.start()

# Example Usage
if __name__ == "__main__":
    niches = [
        {
            "niche": "fitness",
            "query": "exercise motivation",
            "schedule": {"hour": 17, "minute": 0}
        },
        {
            "niche": "cooking",
            "query": "kitchen hacks",
            "schedule": {"hour": 9, "minute": 30}
        }
    ]

    scheduler = ContentScheduler()
    scheduler.schedule_daily_uploads(niches)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.scheduler.shutdown()