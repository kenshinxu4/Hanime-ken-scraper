import telebot
from telebot import types
import os
import subprocess
import tempfile
import time

TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
ADMIN_ID = int(os.environ['ADMIN_ID'])
GROUP_ID = os.environ.get('GROUP_CHAT_ID')
if GROUP_ID:
    GROUP_ID = int(GROUP_ID)

bot = telebot.TeleBot(TOKEN)
url_cache = {}  # temporary storage for admin's current URL

@bot.message_handler(commands=['start'])
def start(message):
    if message.from_user.id != ADMIN_ID:
        return
    bot.reply_to(message, "👋 Welcome to your **Personal Hentai Downloader Bot**!\nOnly works for you (admin). Use /help")

@bot.message_handler(commands=['help'])
def help_cmd(message):
    if message.from_user.id != ADMIN_ID:
        return
    text = """📋 Commands:
• /start - Welcome
• /help - This message
• /list - Latest uploaded hentai on hanime.tv (new uploads)
• /search <query> - Search hentai (example: /search sister breeder)
• /trending - Show new/trending hentai
• /download <full hanime.tv URL> - Start download (example: the sister-breeder-3 link)

After /download or /list → choose 360p or 480p → bot downloads, sends to you + group (if set), then deletes file from server.
"""
    bot.reply_to(message, text)

@bot.message_handler(commands=['list'])
def list_uploads(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        result = subprocess.run(['htv', 'new-uploads', '--metadata'], capture_output=True, text=True, timeout=60)
        output = result.stdout or result.stderr or "No results."
        bot.reply_to(message, f"📤 **Latest Uploaded Hentai on hanime.tv**:\n\n{output[:3000]}\n\nCopy any full URL and use /download <URL>")
    except Exception as e:
        bot.reply_to(message, f"❌ List error: {str(e)}")

@bot.message_handler(commands=['search'])
def search_hentai(message):
    if message.from_user.id != ADMIN_ID:
        return
    if len(message.text.split()) < 2:
        return bot.reply_to(message, "Usage: /search sister breeder")
    query = ' '.join(message.text.split()[1:])
    try:
        result = subprocess.run(['htv', query, '--metadata'], capture_output=True, text=True, timeout=60)
        output = result.stdout or result.stderr or "No results."
        bot.reply_to(message, f"🔍 Results for '{query}':\n\n{output[:3000]}\n\nCopy any full URL and use /download <URL>")
    except Exception as e:
        bot.reply_to(message, f"❌ Search error: {str(e)}")

@bot.message_handler(commands=['trending'])
def trending(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        result = subprocess.run(['htv', 'new-releases', '--metadata'], capture_output=True, text=True, timeout=60)
        output = result.stdout or result.stderr or "No results."
        bot.reply_to(message, f"🔥 Trending / New Releases:\n\n{output[:3000]}\n\nUse /download with any URL")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)}")

@bot.message_handler(commands=['download'])
def download_start(message):
    if message.from_user.id != ADMIN_ID:
        return
    if len(message.text.split()) < 2:
        return bot.reply_to(message, "Usage: /download https://hanime.tv/videos/hentai/sister-breeder-3")
    url = message.text.split(maxsplit=1)[1].strip()
    if 'hanime.tv' not in url:
        return bot.reply_to(message, "❌ Must be a hanime.tv video URL")
    
    url_cache[message.from_user.id] = url
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton("360p", callback_data="quality_360"),
               types.InlineKeyboardButton("480p", callback_data="quality_480"))
    bot.reply_to(message, "✅ URL saved!\nChoose quality:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("quality_"))
def quality_callback(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "Only admin!")
    
    quality = call.data.split("_")[1]  # 360 or 480
    url = url_cache.get(call.from_user.id)
    if not url:
        return bot.answer_callback_query(call.id, "URL expired — use /download again")
    
    bot.answer_callback_query(call.id, f"📥 Downloading in {quality}p... (can take 1-3 min)")
    
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            subprocess.run(['htv', url, '--resolution', quality], check=True, timeout=300)
            
            mp4_files = [f for f in os.listdir(tmpdir) if f.lower().endswith('.mp4')]
            if not mp4_files:
                raise Exception("No video file found")
            
            video_path = os.path.join(tmpdir, mp4_files[0])
            caption = f"🎥 Hentai from hanime.tv\nQuality: {quality}p\nURL: {url}\nDownloaded & auto-deleted from server"
            
            with open(video_path, 'rb') as video:
                bot.send_video(call.message.chat.id, video, caption=caption, supports_streaming=True)
            
            if GROUP_ID:
                with open(video_path, 'rb') as video:
                    bot.send_video(GROUP_ID, video, caption=caption, supports_streaming=True)
            
            bot.reply_to(call.message, f"✅ {quality}p video sent! File deleted from server to save storage.")
    
    except Exception as e:
        bot.reply_to(call.message, f"❌ Download failed: {str(e)}\n(try again or check if {quality}p is available)")
    
    url_cache.pop(call.from_user.id, None)

print("Bot started...")
bot.infinity_polling()
