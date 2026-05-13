#!/usr/bin/env bash
# Refresh the Databricks OAuth token in ~/.cursor/mcp.json.
# OAuth tokens expire after 1 hour — run this before each Cursor session
# or whenever the MCP servers show as disconnected.
#
# Usage: scripts/refresh_mcp_token.sh

set -euo pipefail

HOST="https://dbc-08214e28-2988.cloud.databricks.com"
MCP_JSON="$HOME/.cursor/mcp.json"

TOKEN_JSON=$(databricks auth token --host "$HOST")
TOKEN=$(echo "$TOKEN_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['access_token'])")
EXPIRY=$(echo "$TOKEN_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['expiry'])")

python3 - "$MCP_JSON" "$TOKEN" <<'EOF'
import sys, json

path, token = sys.argv[1], sys.argv[2]
with open(path) as f:
    config = json.load(f)

for server in config.get('mcpServers', {}).values():
    if 'headers' in server and 'Authorization' in server['headers']:
        server['headers']['Authorization'] = f'Bearer {token}'

with open(path, 'w') as f:
    json.dump(config, f, indent=2)
    f.write('\n')
EOF

echo "✓ MCP token refreshed (expires $EXPIRY)"
echo "  Reload MCP servers in Cursor: Settings → MCP → refresh icon"
