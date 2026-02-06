import json

import requests  # Install requests module first.

# Use this url to get the USDT active instruments
url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"

# Use this url to get the INR active instruments
#url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=INR"

response = requests.get(url)
data = response.json()
print(json.dumps(data, indent=2))