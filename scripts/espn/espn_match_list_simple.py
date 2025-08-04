# espn_match_list_simple.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from urllib.parse import urljoin
import itertools

class ESPNScraper:
    def __init__(self):
        self.session = requests.Session()
        
        # Free proxy list (you can add more or use a paid service)
        # self.proxies_list = [
            # {"http": "http://proxy1.example.com:8080", "https": "https://proxy1.example.com:8080"},
            # {"http": "http://proxy2.example.com:8080", "https": "https://proxy2.example.com:8080"},
            # Add more proxies here
        # ]
        
        self.proxies_list = self.get_free_proxies()
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        self.proxy_cycle = itertools.cycle(self.proxies_list) if self.proxies_list else None
        self.user_agent_cycle = itertools.cycle(self.user_agents)
        
    def get_free_proxies(self):
        """Fetch free proxies from a public API"""
        try:
            # Using correct proxyscrape API v4
            response = requests.get('https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text')
            proxies = response.text.strip().split('\n')
            
            proxy_list = []
            for proxy in proxies[:15]:  # Use first 15 proxies
                if '://' in proxy and ':' in proxy:
                    # Format: http://ip:port or https://ip:port
                    if proxy.startswith('http://'):
                        proxy_dict = {
                            "http": proxy,
                            "https": proxy.replace('http://', 'https://')
                        }
                    elif proxy.startswith('https://'):
                        proxy_dict = {
                            "http": proxy.replace('https://', 'http://'),
                            "https": proxy
                        }
                    else:
                        continue
                    proxy_list.append(proxy_dict)
            
            print(f"✅ Loaded {len(proxy_list)} free proxies")
            return proxy_list
            
        except Exception as e:
            print(f"❌ Failed to fetch free proxies: {e}")
            return []
    
    def test_proxy(self, proxy):
        """Test if a proxy is working"""
        try:
            test_response = requests.get(
                'http://httpbin.org/ip', 
                proxies=proxy, 
                timeout=10,
                headers={'User-Agent': random.choice(self.user_agents)}
            )
            if test_response.status_code == 200:
                return True
        except:
            pass
        return False

    def get_matches_for_year(self, year, match_class, match_type):
        """
        match_class: 1 = Test, 2 = ODI, 3 = T20I
        match_type: 'test', 'odi', 't20i'
        """
        url = f"https://stats.espncricinfo.com/ci/engine/records/team/match_results.html?class={match_class};id={year};type=year"
        print(f"Fetching {match_type.upper()} matches for {year}")
        
        # Get fresh proxies if list is empty
        if not self.proxies_list:
            self.proxies_list = self.get_free_proxies()
            self.proxy_cycle = itertools.cycle(self.proxies_list) if self.proxies_list else None
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                # Rotate user agent
                user_agent = next(self.user_agent_cycle)
                
                # Rotate proxy
                current_proxy = None
                if self.proxy_cycle:
                    current_proxy = next(self.proxy_cycle)
                    print(f"� Attempt {attempt + 1}: Using proxy {current_proxy['http'].split('@')[-1] if '@' in current_proxy['http'] else current_proxy['http']}")
                
                headers = {
                    'User-Agent': user_agent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'cross-site',
                    'Cache-Control': 'max-age=0',
                }
                
                # Add random delay
                time.sleep(random.uniform(3, 8))
                
                response = requests.get(
                    url, 
                    headers=headers,
                    proxies=current_proxy,
                    timeout=30,
                    allow_redirects=True
                )
                
                print(f"📡 HTTP Status: {response.status_code}")
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Try to find the table with different selectors
                    table = None
                    selectors = [
                        'table.ds-w-full.ds-table.ds-table-xs.ds-table-auto',
                        'table[class*="ds-table"]',
                        'table.engineTable',
                        'table'
                    ]
                    
                    for selector in selectors:
                        table = soup.select_one(selector)
                        if table:
                            print(f"✅ Found table with selector: {selector}")
                            break
                    
                    if not table:
                        print(f"❌ No table found for {match_type} {year}")
                        continue
                    
                    # Parse table manually
                    rows = []
                    for tr in table.find_all('tr')[1:]:  # Skip header row
                        cells = tr.find_all(['td', 'th'])
                        if len(cells) >= 6:  # Minimum required columns
                            row_data = [cell.get_text(strip=True) for cell in cells]
                            rows.append(row_data)
                    
                    if not rows:
                        print(f"❌ No data rows found for {match_type} {year}")
                        continue
                    
                    # Create DataFrame with common column names
                    columns = ['Team1', 'Team2', 'Winner', 'Margin', 'Ground', 'Date', 'Scorecard']
                    df = pd.DataFrame(rows, columns=columns[:len(rows[0])])
                    df['Year'] = year
                    df['Format'] = match_type
                    
                    print(f"✅ Found {len(df)} matches for {match_type} {year}")
                    return df
                
                elif response.status_code == 403:
                    print(f"❌ 403 Forbidden for {match_type} {year} (attempt {attempt + 1})")
                    if attempt < max_retries - 1:
                        print("🔄 Trying with different proxy...")
                        continue
                else:
                    print(f"❌ HTTP {response.status_code} for {match_type} {year}")
                    if attempt < max_retries - 1:
                        continue
                        
            except Exception as e:
                print(f"❌ Error on attempt {attempt + 1} for {match_type} {year}: {e}")
                if attempt < max_retries - 1:
                    print("🔄 Retrying with different proxy...")
                    continue
        
        print(f"❌ Failed to fetch {match_type} {year} after {max_retries} attempts")
        return pd.DataFrame()

def main():
    scraper = ESPNScraper()
    all_data = []
    formats = [(1, "test"), (2, "odi"), (3, "t20i")]
    
    # Test with a smaller range first
    for match_class, match_type in formats:
        for year in range(2023, 2025):  # Recent years only
            df = scraper.get_matches_for_year(year, match_class, match_type)
            if not df.empty:
                all_data.append(df)
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv("../espn_match_list.csv", index=False)
        print(f"✅ Saved {len(final_df)} matches to espn_match_list.csv")
        print(f"📊 Formats: {final_df['Format'].value_counts().to_dict()}")
    else:
        print("❌ No data collected")

if __name__ == "__main__":
    main()
