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
        
        # Get free proxies
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
            response = requests.get('https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text', timeout=10)
            proxies = response.text.strip().split('\n')
            
            proxy_list = []
            for proxy in proxies[:10]:  # Use first 10 proxies
                proxy = proxy.strip()
                if proxy and '://' in proxy:
                    # Format: http://ip:port or https://ip:port
                    proxy_dict = {
                        "http": proxy,
                        "https": proxy
                    }
                    proxy_list.append(proxy_dict)
                    print(f"📡 Added proxy: {proxy}")
            
            print(f"✅ Loaded {len(proxy_list)} free proxies")
            return proxy_list
            
        except Exception as e:
            print(f"❌ Failed to fetch free proxies: {e}")
            return []
    
    def get_matches_for_year(self, year, match_class, match_type):
        """
        match_class: 1 = Test, 2 = ODI, 3 = T20I
        match_type: 'test', 'odi', 't20i'
        """
        # Use the new URL format
        format_mapping = {
            1: "test-matches-1",
            2: "odi-matches-2", 
            3: "t20i-matches-3"
        }
        
        format_suffix = format_mapping.get(match_class, "test-matches-1")
        url = f"https://www.espncricinfo.com/records/year/team-match-results/{year}-{year}/{format_suffix}"
        print(f"Fetching {match_type.upper()} matches for {year}")
        print(f"URL: {url}")
        
        # Get fresh proxies if list is empty
        if not self.proxies_list:
            self.proxies_list = self.get_free_proxies()
            self.proxy_cycle = itertools.cycle(self.proxies_list) if self.proxies_list else None
        
        max_retries = 5  # Increased retries
        for attempt in range(max_retries):
            try:
                # Rotate user agent
                user_agent = next(self.user_agent_cycle)
                
                # Rotate proxy (try without proxy on last attempts)
                current_proxy = None
                if attempt < 3 and self.proxy_cycle and self.proxies_list:
                    current_proxy = next(self.proxy_cycle)
                    print(f"🔄 Attempt {attempt + 1}: Using proxy {current_proxy['http']}")
                else:
                    print(f"🔄 Attempt {attempt + 1}: No proxy (direct connection)")
                
                headers = {
                    'User-Agent': user_agent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Referer': 'https://www.espncricinfo.com/',
                }
                
                # Add random delay
                time.sleep(random.uniform(2, 5))
                
                response = requests.get(
                    url, 
                    headers=headers,
                    proxies=current_proxy,
                    timeout=20,
                    allow_redirects=True,
                    verify=True  # Enable SSL verification
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
    formats = [(1, "test")]  # Start with just test matches
    
    # Test with a smaller range first
    for match_class, match_type in formats:
        for year in [2023]:  # Test with just one year
            df = scraper.get_matches_for_year(year, match_class, match_type)
            if not df.empty:
                all_data.append(df)
                break  # Stop after first successful fetch
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv("../espn_match_list.csv", index=False)
        print(f"✅ Saved {len(final_df)} matches to espn_match_list.csv")
        print(f"📊 Sample data:")
        print(final_df.head())
    else:
        print("❌ No data collected")

if __name__ == "__main__":
    main()
