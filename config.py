import stem, stem.control

# config.py

# Habilita o deshabilita el uso de torsocks + privoxy + tor
USE_TORSOCKS = False

# Dirección del proxy HTTP local provisto por Privoxy (redirige a Tor)
TOR_PROXY = 'http://127.0.0.1:8118'

EXPORT_DIR = "exportify"
EXPORT_RESULT_DIR = "exports"
DOWNLOADS_DIR = 'downloads'
LOGS_DIR = 'logs'
FFMPEG_PATH = r"/usr/bin/ffmpeg"

DEBUG_MODE = True

def renew_tor_ip():
    with stem.control.Controller.from_port(port=9051) as controller:
        controller.authenticate()
        controller.signal(stem.Signal.NEWNYM)