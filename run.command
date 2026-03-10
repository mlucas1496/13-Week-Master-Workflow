#!/bin/bash

# 13WCF Unified Workflow Launcher
# ================================

APP_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "=========================================="
echo "  13WCF Unified Workflow"
echo "=========================================="
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed!"
    echo "Please install Python 3 from https://www.python.org/"
    read -p "Press Enter to exit..."
    exit 1
fi

# Install dependencies
echo "Installing Python dependencies..."
python3 -m pip install -q -r "$APP_DIR/requirements.txt"

# Create required directories
mkdir -p "$APP_DIR/uploads" "$APP_DIR/outputs"

echo ""
echo "Starting unified workflow app..."
echo "  Main app:          http://localhost:8888"
echo ""
echo "  Press Ctrl+C to stop the server"
echo "=========================================="
echo ""

# Open browser
(sleep 2 && open "http://localhost:8888") &

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down..."
    exit 0
}
trap cleanup INT TERM

# Start Flask app
cd "$APP_DIR"
python3 app.py

# Cleanup on exit
cleanup
