#!/usr/bin/env bash
# Build and prepare site for deployment
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Rebuild mdBook ==="
mdbook build site

echo "=== Deploy structure ==="
echo "  Landing page: donge.org/zighouse/index.html  (manual rsync)"
echo "  mdBook docs:   site/book/ → donge.org/zighouse/docs/"
echo ""
echo "To deploy:"
echo "  rsync -avz site/book/ user@server:/var/www/zighouse/docs/"
echo "  rsync -avz donge.org/zighouse/index.html user@server:/var/www/zighouse/"
