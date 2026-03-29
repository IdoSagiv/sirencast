import json
import logging
import math
import os
import httpx
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from web import db
from collector import config

logger = logging.getLogger(__name__)
app = FastAPI(title='SirenCast')

CITIES_URL = 'https://www.tzevaadom.co.il/static/cities.json'
cities_cache: dict = {}  # name -> {lat, lng}

def haversine_km(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))

async def load_cities():
    global cities_cache
    # Try local file first (data/cities.json), then fetch from tzevaadom
    local_path = os.path.join(config.DATA_DIR, 'cities.json')
    try:
        if os.path.exists(local_path):
            with open(local_path, encoding='utf-8') as f:
                data = json.load(f)
        else:
            async with httpx.AsyncClient() as client:
                r = await client.get(CITIES_URL, headers={'User-Agent': 'SirenCast/1.0'}, timeout=10)
            data = r.json()
            with open(local_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        cities_cache = {
            name: {'lat': c['lat'], 'lng': c['lng']}
            for name, c in data.get('cities', {}).items()
            if 'lat' in c and 'lng' in c
        }
        logger.info(f'Loaded {len(cities_cache)} cities for location lookup')
    except Exception as e:
        logger.warning(f'Failed to load cities data: {e}')

@app.on_event('startup')
async def startup():
    await load_cities()

OREF_URL = 'https://www.oref.org.il/warningMessages/alert/alerts.json'
OREF_HEADERS = {
    'Referer': 'https://www.oref.org.il/',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

@app.get('/api/areas')
def list_areas():
    return {'areas': db.get_all_known_areas()}

@app.get('/api/history')
def get_history(areas: str = Query('')):
    area_list = [a.strip() for a in areas.split(',') if a.strip()]
    return db.query_historical_counts(area_list)

@app.get('/api/locate')
def locate(lat: float = Query(...), lng: float = Query(...)):
    if not cities_cache:
        return {'area': None, 'distance_km': None}
    best_name, best_dist = None, float('inf')
    for name, c in cities_cache.items():
        d = haversine_km(lat, lng, c['lat'], c['lng'])
        if d < best_dist:
            best_dist = d
            best_name = name
    return {'area': best_name, 'distance_km': round(best_dist, 2)}

@app.get('/api/area-stats')
def get_area_stats(area: str = Query('')):
    if not area.strip():
        return {'total_incidents': 0, 'had_siren_incidents': 0, 'area_siren_count': 0, 'area_siren_pct': 0.0}
    return db.get_area_stats(area.strip())

@app.get('/api/incidents')
def get_incidents(area: str = Query('')):
    if not area.strip():
        return {'incidents': []}
    return {'incidents': db.get_incidents_for_area(area.strip())}

@app.get('/api/live')
async def get_live():
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(OREF_URL, headers=OREF_HEADERS, timeout=5)
        text = r.content.decode('utf-8-sig').strip()
        if not text:
            return {'active': False}
        data = json.loads(text)
        if isinstance(data, list):
            data = data[0] if data else None
        if not data:
            return {'active': False}
        cat = str(data.get('cat', ''))
        if cat not in {'1', '10'}:
            return {'active': False}
        return {
            'active': True,
            'cat': cat,
            'oref_id': str(data.get('id', '')),
            'areas': sorted(list(data.get('data', [])))
        }
    except Exception as e:
        logger.warning(f'Live poll error: {e}')
        return {'active': False}

@app.get('/')
def index():
    import os
    return FileResponse(os.path.join(os.path.dirname(__file__), 'static', 'index.html'))

app.mount('/static', StaticFiles(directory='web/static'), name='static')
