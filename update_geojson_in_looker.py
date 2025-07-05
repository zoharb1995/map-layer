import os
import json
import base64
import requests
import mysql.connector
from utils.utils import conn_repl


# === GitHub Config ===
REPO = "fieldin/offline_processes"
# REPO = "zoharb1995/map-layer-test"
FILE_PATH = "all_plot.json"
BRANCH = "master"
user = os.environ.get("GITHUB_USER")
token = os.environ.get("GITHUB_TOKEN")

# === Database Config ===
db_config = {
    "host": conn_repl["host"],
    "user": conn_repl["user"],
    "password": conn_repl["password"],
    "database": conn_repl["database"],
}

# === Query ===
query = """
SELECT JSON_OBJECT(
    'type','FeatureCollection',
    'features',
    COALESCE(
      JSON_ARRAYAGG(
        JSON_OBJECT(
          'type','Feature',
          'id', t.id,
          'geometry', t.geometry
        )
      ),
      JSON_ARRAY()
    )
) AS featureCollection
FROM (
  SELECT DISTINCT p.id,  p.geometry
  FROM companies c
  INNER JOIN (
      SELECT DISTINCT c.id
      FROM companies AS c
      JOIN company_properties AS cp
        ON cp.company_id = c.id
        AND cp.name = 'is_active'
        AND cp.value = 1
        AND cp.latest = 1
      LEFT JOIN company_properties AS cp2
        ON cp2.company_id = c.id
        AND cp2.name = 'has_yield_access'
        AND cp2.value = 1
        AND cp2.latest = 1
      WHERE cp2.id IS NULL
  ) c2 ON c2.id = c.id
  INNER JOIN `groups` g ON c.id = g.company_id
  INNER JOIN group_polygons gp ON g.id = gp.group_id
  INNER JOIN polygons p ON gp.polygon_id = p.id
  WHERE p.archived_at IS NULL
    AND p.deleted_at IS NULL
    AND p.type = 'plot'
    AND p.area > 0.1
    AND p.geometry IS NOT NULL
    AND p.id IS NOT NULL
    AND c.id != 5010
) AS t
"""

# === Run Query ===
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor(dictionary=True)
cursor.execute(query)

result = cursor.fetchone()
geojson_data = result["featureCollection"]
conn.close()

# === GitHub API Headers ===
headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

# === Step 1: Get file SHA (for updating)
url = f"https://api.github.com/repos/{REPO}/contents/{FILE_PATH}"
params = {"ref": BRANCH}
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    sha = response.json()["sha"]
    print(f"📄 Found existing file, SHA: {sha}")
else:
    sha = None
    print("🆕 File not found, will create new.")

# === Step 2: Upload (create or update)
payload = {
    "message": "Update all_plot.json from script",
    "content": base64.b64encode(geojson_data.encode("utf-8")).decode("utf-8"),
    "branch": BRANCH,
}
if sha:
    payload["sha"] = sha

upload_response = requests.put(url, headers=headers, json=payload)

if upload_response.status_code in [200, 201]:
    print("✅ File successfully uploaded to GitHub!")
    print(f"🔗 Use in Looker: https://fieldin.github.io/offline_processes/{FILE_PATH}")
else:
    print(f"❌ Upload failed: {upload_response.status_code}")
    print(upload_response.json())
print(os.getcwd())