import os
import sys
import pandas as pd
from yt_dlp import YoutubeDL
import subprocess
from multiprocessing import Process
from datetime import datetime
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
import time

from config import USE_TORSOCKS, TOR_PROXY, FFMPEG_PATH, renew_tor_ip

def set_mp3_metadata(filepath, title, artist, album):
    try:
        audio = MP3(filepath, ID3=EasyID3)
        audio['title'] = title
        audio['artist'] = artist
        audio['album'] = album
        audio.save()
        return True
    except Exception as e:
        print(f"⚠️ No se pudieron asignar metadatos a {filepath}: {e}")
        return False

# === Logging duplicado (consola + archivo) ===
class Tee:
    def __init__(self, logfile_path):
        self.terminal = sys.stdout
        self.log = open(logfile_path, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

# === Obtener duración real de archivo MP3 ===
def get_audio_duration(path):
    try:
        result = subprocess.run([
            FFMPEG_PATH.replace("ffmpeg.exe", "ffprobe.exe"),
            '-v', 'error', '-show_entries',
            'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1',
            path
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return float(result.stdout.strip())
    except:
        return None

# === Función principal para un CSV ===
def process_csv(file_name, max_retries=3):
    export_dir = 'exports'
    downloads_dir = 'downloads'
    logs_dir = 'logs'
    os.makedirs(downloads_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    name_without_ext = os.path.splitext(file_name)[0]
    download_path = os.path.join(downloads_dir, name_without_ext)
    os.makedirs(download_path, exist_ok=True)

    # Redirigir consola a log
    log_path = os.path.join(logs_dir, f"{name_without_ext}.log")
    sys.stdout = Tee(log_path)
    sys.stderr = sys.stdout  # Capturar también errores

    # Leer CSV
    df = pd.read_csv(os.path.join(export_dir, file_name))
    df_valid = df[df['YouTube Link'].notna() & (df['YouTube Link'] != 'NOT FOUND')]

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(download_path, '%(title)s.%(ext)s'),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'postprocessor_args': ['-ar', '44100', '-ac', '2'],
        'prefer_ffmpeg': True,
        'ffmpeg_location': FFMPEG_PATH,
        'quiet': False,
        'no_warnings': True,
    }
    
    if USE_TORSOCKS:
        ydl_opts['proxy'] = TOR_PROXY

    print(f"\n--- Procesando {file_name} ({len(df_valid)} canciones) ---")
    print(f"Hora de inicio: {datetime.now()}\n")

    with YoutubeDL(ydl_opts) as ydl:
        for idx, row in df_valid.iterrows():
            artist = row['Artist']
            title = row['Title']
            video_title = row.get('Video Title', '')
            expected_duration = row.get('Duration (s)', None)
            url = row['YouTube Link']

            if isinstance(video_title, str) and video_title.strip():
                filename = os.path.join(download_path, f"{video_title}.mp3")
            else:
                filename = None

            if filename and os.path.exists(filename) and expected_duration:
                actual_duration = get_audio_duration(filename)
                if actual_duration and abs(actual_duration - expected_duration) <= 3:
                    print(f"[{idx+1}] ⏩ Ya válido: {artist} - {title}")
                    continue
                else:
                    print(f"[{idx+1}] 🔁 Duración incorrecta. Se re-descarga.")

            print(f"[{idx+1}] ⬇️ Descargando: {artist} - {title}")
            
            attempt = 0
            while attempt < max_retries:
                try:
                    ydl.download([url])
                    
                    # Buscar archivo MP3 generado (basado en título del video)
                    mp3_path = os.path.join(download_path, f"{video_title}.mp3")
                    if os.path.exists(mp3_path):
                        set_mp3_metadata(
                            filepath=mp3_path,
                            title=title,
                            artist=artist,
                            album=name_without_ext
                        )
                except Exception as e:
                    attempt += 1
                    msg = str(e).lower()
                    print(f"❌ Error intento {attempt}: {e}")
                    if "429" in msg or "rate limit" in msg:
                        print("🔁 Rate limited, cambiando IP con Tor...")
                        renew_tor_ip()
                        time.sleep(5)
                        ydl.download([url])  # intento 2
                    else:
                        print(f"❌ Error al descargar {url}: {e}")

    print(f"\n✅ Finalizado: {file_name} — {datetime.now()}\n")

# === PROCESAMIENTO PARALELO POR CSV ===
if __name__ == "__main__":
    export_dir = 'exports'
    csv_files = [f for f in os.listdir(export_dir) if f.endswith('.csv')]

    if not csv_files:
        print("❌ No hay archivos CSV en 'exports/'")
        exit()

    processes = []

    for file in csv_files:
        p = Process(target=process_csv, args=(file,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("\n🎉 Todos los archivos han sido procesados en paralelo.")
