import json
import logging
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx

from web import db

logger = logging.getLogger(__name__)
app = FastAPI(title='SirenCast')

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
