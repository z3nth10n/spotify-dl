import os

import pandas as pd
from yt_dlp import YoutubeDL

from tqdm import tqdm

import time
import logging

import csv
from collections import defaultdict

from config import LOGS_DIR, EXPORT_DIR, EXPORT_RESULT_DIR, USE_TORSOCKS, renew_tor_ip, ydl_opts

# import sys
# import traceback
# from datetime import datetime


# Mapeo de source_file -> (file_handle, csv_writer)
open_writers = {}

def setup_logger(out_name):
    log_path = os.path.join(LOGS_DIR, f"{out_name}.log")
    
    logger = logging.getLogger(log_path)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Archivo
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))

        # Consola
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(message)s'))

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

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

def process(file_or_df, name_override=None, max_retries=3):
    df = pd.DataFrame()
    out_name = ""
    is_combined = False
    source_file = ""
    
    if isinstance(file_or_df, str):
        df = pd.read_csv(file_or_df)
        out_name = os.path.splitext(os.path.basename(file_or_df))[0]
        source_file = out_name
    else:
        df = file_or_df
        out_name = name_override or "combined"
        is_combined = True
        
    logger = setup_logger(out_name)
    
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

    logger.info(f"🔍 Encontradas {len(to_process)} canciones nuevas para buscar (de {len(df)} totales).\n")
    logger.info(f"🔍 Buscando {len(to_process)} canciones una por una...\n")

    try:
        with YoutubeDL(ydl_opts) as ydl:
            for _, row in tqdm(to_process.iterrows(), total=len(to_process)):
                artist = row['Artist']
                title = row['Track Name']
                query = f"{artist} - {title}"
                duration = row['Expected Duration (s)']
                
                if is_combined:
                    source_file = row['Source File']
                    
                output_path = os.path.join(EXPORT_RESULT_DIR, f"{source_file}.csv")
                
                # Abrir escritor si no existe
                if source_file not in open_writers:
                    f = open(output_path, mode='w', newline='', encoding='utf-8')
                    writer = csv.writer(f)
                    # Escribir cabecera solo si el archivo está vacío
                    if os.stat(output_path).st_size == 0:
                        writer.writerow(['Artist', 'Title', 'YouTube Link', 'Video Title', 'Uploader', 'Duration (s)'])
                    open_writers[source_file] = (f, writer)

                attempt = 0
                while attempt < max_retries:
                    try:
                        info = ydl.extract_info(query, download=False)
                        if 'entries' in info:
                            video = choose_best_video(info['entries'], duration)
                        else:
                            video = info
                            
                        video_link = f"https://www.youtube.com/watch?v={video['id']}"

                        open_writers[source_file][1].writerow({
                            artist,
                            title,
                            video_link,
                            video.get('title', ''),
                            video.get('uploader', ''),
                            video.get('duration', '')
                        })
                        break
                    except Exception as e:
                        # print(e)
                        # traceback.print_exc() 
                        # sys.exit(1)
                        
                        msg = str(e).lower()
                        logger.error(f"❌ Error intento {attempt}: {e} - {query}")
                        if "429" in msg or "rate limit" in msg and USE_TORSOCKS:
                            logger.warning("🔁 Rate limited, cambiando IP con Tor...")
                            renew_tor_ip()
                            time.sleep(5)
                            attempt += 1
                        else:
                            open_writers[source_file][1].writerow({
                                artist, title, 'NOT FOUND', '', '', ''
                            })
                            break
                        break
    finally:
        # ✅ Cerrar todos los ficheros
        for f, _ in open_writers.values():
            f.close()

    df_new = pd.DataFrame(results)

    if os.path.exists(output_csv):
        df_done = pd.read_csv(output_csv)
        df_combined = pd.concat([df_done, df_new], ignore_index=True).drop_duplicates(subset=['Artist', 'Title'])
        df_combined.to_csv(output_csv, index=False)  # 🔁 Sobrescribe limpio
    else:
        df_new.to_csv(output_csv, index=False)
        
    logger.info(f"\n✅ Resultados actualizados en: {output_csv}")

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
        df["Source File"] = os.path.splitext(file)[0]
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

    # Guardar archivo temporal para procesar
    temp_path = os.path.join(EXPORT_DIR, "_all_combined_filtered.csv")
    
    # Eliminar archivo temporal si ya existe
    if os.path.exists(temp_path):
        os.remove(temp_path)
        assert not os.path.exists(temp_path), "❌ El archivo no fue eliminado correctamente"

    print("Opciones disponibles:")
    print("0. 🔄 Procesar todos los CSV")
    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")

    choice = input("Selecciona una opción: ")

    cached_pairs = get_cached_pairs()

    if choice == '0':
        df_all = concatenate_all_exports()
        df_all['__cache_key__'] = df_all.apply(lambda row: (infer_artist(row), row['Track Name']), axis=1)
        df_all = df_all[~df_all['__cache_key__'].isin(cached_pairs)].drop(columns='__cache_key__')

        if df_all.empty:
            print("✅ Todo ya está procesado según la caché.")
            return
        
        df_all.to_csv(temp_path, index=False)
        process(df_all, name_override="combined")
        os.remove(temp_path)

    else:
        try:
            idx = int(choice)
            if idx < 1 or idx > len(files):
                raise ValueError
            selected = files[idx - 1]
            process(os.path.join(EXPORT_DIR, selected))
        except (IndexError, ValueError):
            print("❌ Selección inválida.")

if __name__ == '__main__':
    main()