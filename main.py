import csv
import os

import pandas as pd
from yt_dlp import YoutubeDL

from tqdm import tqdm

import time

from config import USE_TORSOCKS, TOR_PROXY, EXPORT_DIR, EXPORT_RESULT_DIR, DEBUG_MODE, renew_tor_ip

# --- Configuración de carpetas ---
os.makedirs(EXPORT_RESULT_DIR, exist_ok=True)

# --- yt_dlp settings ---
ydl_opts = {
    'quiet': True,
    'skip_download': True,
    'extract_flat': 'in_playlist',
    'default_search': 'ytsearch10',
    'extract_flat': True  # solo metadatos, no descarga
}

if USE_TORSOCKS:
    ydl_opts['proxy'] = TOR_PROXY

def choose_best_video(results, expected_duration=None):
    best = None
    for entry in results:
        title = entry.get('title', '').lower()
        channel = entry.get('uploader', '').lower()
        duration = entry.get('duration')

        if not duration:
            continue

        # Penalizar ciertos patrones
        if any(skip in title for skip in ['live', 'lyric', 'cover', 'remix', 'nightcore', 'sped up']):
            continue

        is_official = (
            any(kw in title for kw in ['official', 'audio', 'video']) or
            'vevo' in channel or 'topic' in channel
        )

        is_duration_ok = (
            expected_duration is None or
            abs(duration - expected_duration) <= 3
        )

        if is_official and is_duration_ok:
            return entry  # match ideal

        if not best and is_duration_ok:
            best = entry  # fallback si nada mejor

    return best or results[0]  # si nada cumple, se queda con algo

def process_file_sequential(file_or_df, name_override=None, max_retries=3):
    df = pd.DataFrame()
    out_name = ""
    
    if isinstance(file_or_df, str):
        df = pd.read_csv(file_or_df)
        out_name = os.path.splitext(os.path.basename(file_or_df))[0]
    else:
        df = file_or_df
        out_name = name_override or "combined"
    
    # --- Cargar CSV original ---
    if 'Artist' not in df.columns:
        df = df[['Artist Name(s)', 'Track Name', 'Duration (ms)']].drop_duplicates()
        df['Artist'] = df['Artist Name(s)'].apply(lambda x: str(x).split(',')[0].strip() if pd.notna(x) else 'Unknown')

    output_csv = os.path.join(EXPORT_RESULT_DIR, f"{out_name}.csv")

    already_done = set()
    if os.path.exists(output_csv):
        df_done = pd.read_csv(output_csv)
        already_done = set(zip(df_done['Artist'], df_done['Title']))

    results = []
    to_process = df[~df.apply(lambda row: (row['Artist'], row['Track Name']) in already_done, axis=1)]

    print(f"🔍 Encontradas {len(to_process)} canciones nuevas para buscar (de {len(df)} totales).\n")

    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'extract_flat': False,
        'default_search': 'ytsearch10',
    }
    
    if USE_TORSOCKS:
        ydl_opts['proxy'] = TOR_PROXY
        
    if DEBUG_MODE:
        ydl_opts['quiet'] = False
        ydl_opts['verbose'] = True

    print(f"🔍 Buscando {len(to_process)} canciones una por una...\n")

    with YoutubeDL(ydl_opts) as ydl:
        for _, row in tqdm(to_process.iterrows(), total=len(to_process)):
            artist = row['Artist']
            title = row['Track Name']
            query = f"{artist} - {title}"
            duration = row['Expected Duration (s)']

            attempt = 0
            while attempt < max_retries:
                try:
                    info = ydl.extract_info(query, download=False)
                    if 'entries' in info:
                        video = choose_best_video(info['entries'], duration)
                    else:
                        video = info

                    results.append({
                        'Artist': artist,
                        'Title': title,
                        'YouTube Link': f"https://www.youtube.com/watch?v={video['id']}",
                        'Video Title': video.get('title', ''),
                        'Uploader': video.get('uploader', ''),
                        'Duration (s)': video.get('duration', '')
                    })
                except Exception as e:
                    msg = str(e).lower()
                    print(f"❌ Error intento {attempt}: {e}")
                    if "429" in msg or "rate limit" in msg:
                        print("🔁 Rate limited, cambiando IP con Tor...")
                        renew_tor_ip()
                        time.sleep(5)
                        attempt += 1
                    else:
                        results.append({
                            'Artist': artist,
                            'Title': title,
                            'YouTube Link': 'NOT FOUND',
                            'Video Title': '',
                            'Uploader': '',
                            'Duration (s)': ''
                        })

    df_new = pd.DataFrame(results)
    if os.path.exists(output_csv):
        df_combined = pd.concat([df_done, df_new], ignore_index=True).drop_duplicates(subset=['Artist', 'Title'])
    else:
        df_combined = df_new

    df_combined.to_csv(output_csv, index=False)
    print(f"\n✅ Resultados actualizados en: {output_csv}")

def get_cached_pairs():
    """Lee todos los archivos en EXPORT_RESULT_DIR y devuelve un set de (Artist, Title) ya procesados"""
    cached = set()
    for f in os.listdir(EXPORT_RESULT_DIR):
        if not f.endswith('.csv'):
            continue
        df = pd.read_csv(os.path.join(EXPORT_RESULT_DIR, f))
        for _, row in df.iterrows():
            cached.add((row['Artist'], row['Title']))
    return cached

def infer_artist(row):
    if pd.notna(row['Artist Name(s)']):
        return str(row['Artist Name(s)']).split(',')[0].strip()
    elif pd.notna(row['Track Name']) and '-' in row['Track Name']:
        return row['Track Name'].split('-')[0].strip()
    else:
        return 'Unknown'

def concatenate_all_exports():
    dfs = []
    for file in os.listdir(EXPORT_DIR):
        if not file.endswith('.csv'):
            continue
        df = pd.read_csv(os.path.join(EXPORT_DIR, file))
        df = df[['Artist Name(s)', 'Track Name', 'Duration (ms)']].drop_duplicates()
        df['Artist'] = df['Artist Name(s)'].apply(lambda x: str(x).split(',')[0].strip() if pd.notna(x) else 'Unknown')
        df['Expected Duration (s)'] = df['Duration (ms)'] / 1000
        df['Artist'] = df.apply(infer_artist, axis=1)
        
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)

def main():
    files = [f for f in os.listdir(EXPORT_DIR) if f.endswith('.csv')]
    if not files:
        print("❌ No se encontraron archivos CSV en la carpeta 'spotify/'.")
        return

    print("Opciones disponibles:")
    print("0. 🔄 Procesar todos los CSV")
    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")

    choice = input("Selecciona una opción: ")

    cached_pairs = get_cached_pairs()

    if choice == '0':
        df_all = concatenate_all_exports()
        df_all = df_all[~df_all.apply(lambda row: (str(row['Artist Name(s)']).split(',')[0].strip(), row['Track Name']) in cached_pairs, axis=1)]

        if df_all.empty:
            print("✅ Todo ya está procesado según la caché.")
            return

        # Guardar archivo temporal para procesar
        temp_path = os.path.join(EXPORT_DIR, "_all_combined_filtered.csv")
        
        # Eliminar archivo temporal si ya existe
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        df_all.to_csv(temp_path, index=False)
        process_file_sequential(df_all, name_override="combined")
        os.remove(temp_path)

    else:
        try:
            idx = int(choice)
            if idx < 1 or idx > len(files):
                raise ValueError
            selected = files[idx - 1]
            process_file_sequential(os.path.join(EXPORT_DIR, selected))
        except (IndexError, ValueError):
            print("❌ Selección inválida.")

if __name__ == '__main__':
    main()