import requests

resp = requests.get("http://localhost:5000/api/v1/getPages", params={"query": "showmethecats"})
print(resp.json())