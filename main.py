import os
import pandas as pd
from yt_dlp import YoutubeDL
import csv

# --- Configuración de carpetas ---
spotify_dir = 'exportify'
export_dir = 'exports'
os.makedirs(export_dir, exist_ok=True)

# --- Listar archivos disponibles ---
csv_files = [f for f in os.listdir(spotify_dir) if f.endswith('.csv')]
if not csv_files:
    print("❌ No se encontraron archivos .csv en la carpeta 'spotify/'.")
    exit()

print("Archivos disponibles:")
for i, file in enumerate(csv_files):
    print(f"{i + 1}. {file}")

choice = input("Selecciona el número del archivo que deseas procesar: ")
try:
    selected_file = csv_files[int(choice) - 1]
except (ValueError, IndexError):
    print("❌ Selección inválida.")
    exit()

# --- Cargar CSV original ---
file_path = os.path.join(spotify_dir, selected_file)
df = pd.read_csv(file_path)
df_unique = df[['Artist Name(s)', 'Track Name']].drop_duplicates()
df_unique['Artist'] = df_unique['Artist Name(s)'].apply(lambda x: x.split(',')[0].strip())
df_unique['search_query'] = df_unique['Artist'] + " - " + df_unique['Track Name'] + " audio"

# --- Leer cache si existe ---
output_path = os.path.join(export_dir, selected_file)
if os.path.exists(output_path):
    processed_df = pd.read_csv(output_path)
    processed_keys = set(zip(processed_df['Artist'], processed_df['Title']))
else:
    processed_keys = set()
    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Artist', 'Title', 'YouTube Link', 'Video Title', 'Uploader', 'Duration (s)'])

# --- yt_dlp settings ---
ydl_opts = {
    'quiet': True,
    'skip_download': True,
    'extract_flat': 'in_playlist',
    'default_search': 'ytsearch1',
}

# --- Procesamiento con escritura incremental y caché ---
with open(output_path, mode='a', newline='', encoding='utf-8', buffering=1) as f:
    writer = csv.writer(f)
    with YoutubeDL(ydl_opts) as ydl:
        for idx, row in df_unique.iterrows():
            key = (row['Artist'], row['Track Name'])

            if key in processed_keys:
                print(f"[{idx+1}/{len(df_unique)}] ⏩ Ya procesado: {row['search_query']}")
                continue

            query = row['search_query']
            try:
                info = ydl.extract_info(query, download=False)
                video = info['entries'][0] if 'entries' in info else info

                writer.writerow([
                    row['Artist'],
                    row['Track Name'],
                    f"https://www.youtube.com/watch?v={video['id']}",
                    video.get('title', ''),
                    video.get('uploader', ''),
                    video.get('duration', '')
                ])
                f.flush()
                print(f"[{idx+1}/{len(df_unique)}] ✅ {query}")
            except Exception as e:
                writer.writerow([
                    row['Artist'],
                    row['Track Name'],
                    'NOT FOUND',
                    '',
                    '',
                    ''
                ])
                f.flush()
                print(f"[{idx+1}/{len(df_unique)}] ❌ {query} — {e}")

print(f"\n✅ Resultados guardados en: {output_path}")
