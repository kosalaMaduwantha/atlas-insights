#!/bin/bash
set -euo pipefail

# Directory of this script
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JSON_FILE="$DIR/click_streams_sink.json"

if [ ! -f "$JSON_FILE" ]; then
  echo "JSON file not found: $JSON_FILE" >&2
  exit 1
fi

TMPRESP=$(mktemp /tmp/connector_response.XXXXXX)

HTTP_STATUS=$(curl -sS -o "$TMPRESP" -w "%{http_code}" --location 'http://localhost:8083/connectors' \
  --header 'Content-Type: application/json' \
  --data @"$JSON_FILE")

echo "Connect server HTTP status: $HTTP_STATUS"
echo "Response body:"
cat "$TMPRESP"

rm -f "$TMPRESP"