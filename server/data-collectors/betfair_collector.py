"""
Betfair Exchange API Collector
Real exchange prices, true probabilities (no overround)
Historical BSP (Betfair Starting Price) data
Requires: Betfair account + App Key
"""
import requests, json, os, time, clickhouse_connect
from datetime import datetime, timedelta

BETFAIR_URL = 'https://api.betfair.com/exchange/betting/rest/v1.0/'
APP_KEY = os.getenv('BETFAIR_APP_KEY','')
SESSION_TOKEN = os.getenv('BETFAIR_SESSION_TOKEN','')

def get_session_token(username, password):
    r = requests.post('https://identitysso.betfair.com/api/login',
        data={'username':username,'password':password},
        headers={'X-Application':APP_KEY,'Content-Type':'application/x-www-form-urlencoded','Accept':'application/json'})
    return r.json().get('token')

def list_events(event_type_ids, from_date, to_date):
    body = {'filter':{'eventTypeIds':event_type_ids,'marketStartTime':{'from':from_date,'to':to_date}},'maxResults':1000}
    r = requests.post(BETFAIR_URL+'listEvents', json=body,
        headers={'X-Application':APP_KEY,'X-Authentication':SESSION_TOKEN,'Content-Type':'application/json'})
    return r.json() if r.status_code==200 else []

def list_market_catalogue(event_ids):
    body = {'filter':{'eventIds':event_ids},'marketProjection':['EVENT','MARKET_START_TIME','RUNNER_DESCRIPTION','RUNNER_METADATA'],'maxResults':1000}
    r = requests.post(BETFAIR_URL+'listMarketCatalogue', json=body,
        headers={'X-Application':APP_KEY,'X-Authentication':SESSION_TOKEN,'Content-Type':'application/json'})
    return r.json() if r.status_code==200 else []

def collect_historical_prices(market_id):
    """Collect BSP (Betfair Starting Price) — true market probability"""
    body = {'marketIds':[market_id],'priceProjection':{'priceData':['SP_TRADED','EX_BEST_OFFERS'],'virtualise':False},'orderProjection':'ALL','matchProjection':'NO_ROLLUP'}
    r = requests.post(BETFAIR_URL+'listMarketBook', json=body,
        headers={'X-Application':APP_KEY,'X-Authentication':SESSION_TOKEN,'Content-Type':'application/json'})
    return r.json() if r.status_code==200 else []

# Event Type IDs for major sports:
SPORT_IDS = {
    'football': '1',
    'tennis': '2',
    'horse_racing': '7',
    'basketball': '7522',
    'cricket': '4',
    'rugby': '1477',
    'baseball': '7511',
    'hockey': '7524',
}

if __name__ == '__main__':
    print("Betfair Exchange Collector")
    print("API Docs: https://developer.betfair.com/exchange-api/")
    print("Set env vars: BETFAIR_APP_KEY, BETFAIR_SESSION_TOKEN")
    print(f"Sport IDs: {SPORT_IDS}")
