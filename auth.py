"""One-time Google login. Works on a VPS with no browser."""
import os
from google_auth_oauthlib.flow import Flow

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
SCOPES = ["https://www.googleapis.com/auth/drive"]

flow = Flow.from_client_secrets_file(
    "credentials.json", SCOPES, redirect_uri="http://localhost:8080/"
)
url, _ = flow.authorization_url(access_type="offline", prompt="consent")

print("\n1) Open this link in the browser on ANY device and log in:\n")
print(url)
print("\n2) After you allow access, the page will fail to load (that is normal).")
print("   Copy the FULL address from the browser's address bar and paste it here.\n")
redirect = input("Paste URL here: ").strip()

flow.fetch_token(authorization_response=redirect)
with open("token.json", "w") as f:
    f.write(flow.credentials.to_json())
print("\nSaved token.json - Google Drive is connected.")
