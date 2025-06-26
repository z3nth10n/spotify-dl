import os
import pandas as pd
import subprocess
from multiprocessing import Queue, Process, current_process
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from yt_dlp import YoutubeDL
from tqdm import tqdm
from time import sleep, time
from threading import Thread

# CONFIGURACIÓN
EXPORTS_DIR = 'exports'
DOWNLOADS_DIR = 'downloads'
LOGS_DIR = 'logs'
FFMPEG_PATH = r'D:\APPS\ffmpeg\bin\ffmpeg.exe'  # Asegúrate de ajustar según tu sistema

CONCURRENCY = 5  # máximo de descargas simultáneas

# Función para obtener la duración real de un archivo
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

# Establece metadatos ID3 al archivo mp3
def set_mp3_metadata(filepath, title, artist, album):
    try:
        audio = MP3(filepath, ID3=EasyID3)
        audio['title'] = title
        audio['artist'] = artist
        audio['album'] = album
        audio.save()
    except Exception as e:
        print(f"[{filepath}] ⚠️ Error asignando metadatos: {e}")

# Worker para descargar en paralelo
def download_worker(q, progress_q, idx):
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': None,  # se asigna por tarea
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'postprocessor_args': ['-ar', '44100', '-ac', '2'],
        'prefer_ffmpeg': True,
        'ffmpeg_location': FFMPEG_PATH,
        'quiet': True,
        'no_warnings': True,
    }

    while not q.empty():
        try:
            task = q.get_nowait()
        except:
            break

        artist, title, url, video_title, expected_duration, outdir = task
        filename = f"{video_title}.mp3"
        filepath = os.path.join(outdir, filename)

        # Validación previa
        if os.path.exists(filepath) and expected_duration:
            actual_duration = get_audio_duration(filepath)
            if actual_duration and abs(actual_duration - expected_duration) <= 3:
                progress_q.put((idx, f"✅ Ya existe: {title}"))
                continue

        # Asignar plantilla de salida al worker
        ydl_opts['outtmpl'] = os.path.join(outdir, '%(title)s.%(ext)s')

        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        except Exception as e:
            progress_q.put((idx, f"❌ Error: {e}"))
            continue

        # Metadatos
        final_path = os.path.join(outdir, f"{video_title}.mp3")
        if os.path.exists(final_path):
            set_mp3_metadata(final_path, title, artist, os.path.basename(outdir))

        progress_q.put((idx, f"⬇️ {title}"))

# Monitor para mostrar barras de progreso
def progress_monitor(total_tasks, progress_q):
    bars = [tqdm(total=0, position=i, leave=False, bar_format="{l_bar}{bar} {r_bar}") for i in range(CONCURRENCY)]
    overall = tqdm(total=total_tasks, desc="Progreso total", position=CONCURRENCY, bar_format="{l_bar}{bar} {r_bar}")

    counters = [0] * CONCURRENCY
    start_time = time()

    while overall.n < total_tasks:
        try:
            idx, msg = progress_q.get(timeout=0.1)
        except:
            sleep(0.05)
            continue

        counters[idx] += 1
        bars[idx].total = counters[idx]
        bars[idx].update(1)
        bars[idx].set_description(f"Worker {idx+1}")
        bars[idx].set_postfix_str(msg)
        overall.update(1)

    elapsed = time() - start_time
    overall.set_postfix_str(f"✅ Tiempo total: {elapsed:.1f}s")
    for bar in bars:
        bar.close()
    overall.close()

# Main
def main():
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)

    csv_files = [f for f in os.listdir(EXPORTS_DIR) if f.endswith('.csv')]
    tasks = []

    for file in csv_files:
        df = pd.read_csv(os.path.join(EXPORTS_DIR, file))
        df_valid = df[df['YouTube Link'].notna() & (df['YouTube Link'] != 'NOT FOUND')]

        outdir = os.path.join(DOWNLOADS_DIR, os.path.splitext(file)[0])
        os.makedirs(outdir, exist_ok=True)

        for _, row in df_valid.iterrows():
            artist = row['Artist']
            title = row['Title']
            url = row['YouTube Link']
            video_title = row.get('Video Title', f"{title}")
            duration = row.get('Duration (s)', None)
            tasks.append((artist, title, url, video_title, duration, outdir))

    task_q = Queue()
    progress_q = Queue()

    for t in tasks:
        task_q.put(t)

    # Lanzar procesos de descarga
    workers = []
    for i in range(CONCURRENCY):
        p = Process(target=download_worker, args=(task_q, progress_q, i))
        p.start()
        workers.append(p)

    # Iniciar monitor de progreso
    monitor = Thread(target=progress_monitor, args=(len(tasks), progress_q))
    monitor.start()

    # Esperar a que terminen
    for p in workers:
        p.join()
    monitor.join()

    print("\n✅ Todas las descargas finalizadas.")

if __name__ == '__main__':
    main()
