# espn_match_list_playwright.py
import asyncio
import pandas as pd
import random
from playwright.async_api import async_playwright

async def fetch_espn_matches(year, match_class, match_type):
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

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        page = await browser.new_page()
        
        # Set realistic user agent and headers
        await page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Set viewport to common desktop size
        await page.set_viewport_size({"width": 1366, "height": 768})
        
        # Remove automation indicators
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Remove chrome automation extension
            window.chrome = {
                runtime: {},
            };
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
        """)
        
        try:
            # Add random delay before visiting page
            import random
            await asyncio.sleep(random.uniform(1, 3))
            
            response = await page.goto(url, timeout=60000, wait_until='networkidle')
            print(f"📡 HTTP Status: {response.status}")
            
            if response.status == 403:
                print(f"❌ 403 Forbidden - Request blocked for {match_type} {year}")
                await browser.close()
                return pd.DataFrame()
            elif response.status != 200:
                print(f"❌ HTTP {response.status} - Failed to load page for {match_type} {year}")
                await browser.close()
                return pd.DataFrame()
            
            print(f"✅ Page loaded successfully for {match_type} {year}")
            
            # Add some human-like behavior
            await asyncio.sleep(random.uniform(0.5, 2))
            
            # Simulate mouse movement
            await page.mouse.move(random.randint(100, 500), random.randint(100, 400))
            await asyncio.sleep(random.uniform(0.2, 0.8))
            
            # Scroll down a bit to simulate reading
            await page.mouse.wheel(0, random.randint(100, 300))
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Try multiple selectors - ESPN might have different table classes
            selectors_to_try = [
                "table.ds-w-full.ds-table.ds-table-xs.ds-table-auto",
                "table[class*='ds-table']",
                "table.ds-table",
                "table.engineTable",
                "table[class*='engine']",
                "table",
                ".table-responsive table",
                "[data-testid='table']"
            ]
            
            table_found = False
            for selector in selectors_to_try:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    table_found = True
                    print(f"Found table with selector: {selector}")
                    break
                except:
                    continue
            
            if not table_found:
                print(f"❌ No table found for {match_type} {year} - tried all selectors")
                # Save page content for debugging
                content = await page.content()
                print(f"📄 Page title: {await page.title()}")
                print(f"📄 Page contains 'table': {'table' in content.lower()}")
                await browser.close()
                return pd.DataFrame()

            print(f"🔍 Extracting table data for {match_type} {year}")
            # Extract HTML and read with pandas
            html = await page.content()
            tables = pd.read_html(html)
            await browser.close()

            if len(tables) > 0:
                print(f"📊 Found {len(tables)} table(s), using first one with {len(tables[0])} rows")
                df = tables[0]
                df["Year"] = year
                df["Format"] = match_type
                return df
            else:
                print(f"❌ No tables found in HTML for {match_type} {year}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error fetching {match_type} {year}: {e}")
            await browser.close()
            return pd.DataFrame()

async def main():
    import random
    all_data = []
    formats = [(1, "test"), (2, "odi"), (3, "t20i")]

    # Start with a smaller range to test
    for match_class, match_type in formats:
        for year in range(2020, 2025):  # Start with recent years only
            print(f"Fetching {match_type.upper()} matches for {year}")
            df = await fetch_espn_matches(year, match_class, match_type)
            if not df.empty:
                all_data.append(df)
                print(f"✅ Found {len(df)} matches for {match_type} {year}")
            else:
                print(f"❌ No data for {match_type} {year}")
            
            # Add random delay between requests to avoid rate limiting
            delay = random.uniform(3, 8)  # 3-8 seconds between requests
            print(f"⏳ Waiting {delay:.1f} seconds before next request...")
            await asyncio.sleep(delay)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv("output/espn_match_list.csv", index=False)
        print(f"✅ Saved {len(final_df)} matches to espn_match_list.csv")
    else:
        print("❌ No data collected")

if __name__ == "__main__":
    asyncio.run(main())
