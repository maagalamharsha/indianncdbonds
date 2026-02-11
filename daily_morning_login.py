from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect

API_KEY = "tu4kpuy8ikx7jge3"
API_SECRET = "tvs0tax9n8j9g8mat20ajpxabdmyb20f"

# Generate login URL
kite = KiteConnect(api_key=API_KEY)
login_url = kite.login_url()
print(f"🔗 Click to login: {login_url}")

# Ask user to paste redirect URL
url = input("\n📥 Paste the full redirect URL after login: ")

# Extract request_token
request_token = parse_qs(urlparse(url).query)["request_token"][0]

# Get access token
data = kite.generate_session(request_token, API_SECRET)
access_token = data["access_token"]

# Show and save access token
print("✅ Access Token:", access_token)
with open("access_token.txt", "w") as f:
    f.write(access_token)