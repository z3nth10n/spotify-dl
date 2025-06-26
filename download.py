import os
import pandas as pd
from yt_dlp import YoutubeDL
import subprocess

# Función para obtener duración en segundos de un archivo de audio (usando ffprobe)
def get_audio_duration(path):
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'error', '-show_entries',
            'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', path
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return float(result.stdout.strip())
    except:
        return None

# Configuración de carpetas
export_dir = 'exports'
downloads_dir = 'downloads'
os.makedirs(downloads_dir, exist_ok=True)

# CSVs disponibles
csv_files = [f for f in os.listdir(export_dir) if f.endswith('.csv')]
if not csv_files:
    print("❌ No hay archivos CSV en 'exports/'.")
    exit()

# Configuración base yt_dlp
base_ydl_opts = {
    'format': 'bestaudio/best',
    'outtmpl': os.path.join(downloads_dir, '%(title)s.%(ext)s'),
    'postprocessors': [
        {
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }
    ],
    'ffmpeg_location': 'ffmpeg',  # si ffmpeg está en el PATH del sistema
    'postprocessor_args': [
        '-ar', '44100',  # frecuencia de muestreo estándar
        '-ac', '2',      # canales estéreo
    ],
    'prefer_ffmpeg': True,
    'quiet': False,
    'no_warnings': True,
}

# Procesar cada CSV
for selected_file in csv_files:
    name_without_ext = os.path.splitext(selected_file)[0]
    download_path = os.path.join(downloads_dir, name_without_ext)
    os.makedirs(download_path, exist_ok=True)

    ydl_opts = base_ydl_opts.copy()
    ydl_opts['outtmpl'] = os.path.join(download_path, '%(title)s.%(ext)s')

    df = pd.read_csv(os.path.join(export_dir, selected_file))
    df_valid = df[df['YouTube Link'].notna() & (df['YouTube Link'] != 'NOT FOUND')]

    print(f"\n📥 Procesando: {selected_file} ({len(df_valid)} enlaces válidos)")

    with YoutubeDL(ydl_opts) as ydl:
        for idx, row in df_valid.iterrows():
            url = row['YouTube Link']
            artist = row['Artist']
            title = row['Title']
            expected_duration = row.get('Duration (s)', None)
            video_title = row.get('Video Title', None)

            if not video_title or not isinstance(video_title, str):
                print(f"[{idx+1}] ⚠️ Título no válido, se descargará igualmente.")
                skip_check = False
            else:
                filename = os.path.join(download_path, f"{video_title}.mp3")
                if os.path.exists(filename) and expected_duration:
                    actual_duration = get_audio_duration(filename)
                    if actual_duration and abs(actual_duration - expected_duration) <= 3:
                        print(f"[{idx+1}/{len(df_valid)}] ⏩ Ya existe y es válido: {artist} - {title}")
                        continue
                    else:
                        print(f"[{idx+1}/{len(df_valid)}] 🔁 Duración incorrecta, se volverá a descargar.")
                else:
                    print(f"[{idx+1}/{len(df_valid)}] ⬇️ Preparando descarga: {artist} - {title}")

            try:
                ydl.download([url])
            except Exception as e:
                print(f"❌ Error al descargar {url}: {e}")

    print(f"✅ Finalizado: {download_path}")

print("\n🎵 Todos los archivos han sido procesados.")
