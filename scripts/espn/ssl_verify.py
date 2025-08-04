import requests
import certifi

# Test with requests library
url = "https://stats.espncricinfo.com/ci/engine/records/team/match_results.html?class=1;id=2000;type=year"

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

try:
    response = requests.get(url, headers=headers, verify=certifi.where())
    print("✅ SSL connection successful!")
    print("Status Code:", response.status_code)
    print("Response:", response.text[:200])
except Exception as e:
    print(f"❌ Error: {e}")