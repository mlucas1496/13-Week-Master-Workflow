#!/bin/bash

# 13WCF Unified Workflow Launcher
# ================================
# Starts the Weekly Balances Vite server (Step 1) and the unified Flask app.

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

# Create virtual environment if needed
if [ ! -d "$APP_DIR/venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$APP_DIR/venv"
fi

# Activate venv
source "$APP_DIR/venv/bin/activate"

# Install dependencies
echo "Installing Python dependencies..."
pip install -q --upgrade pip
pip install -q -r "$APP_DIR/requirements.txt"

# Also install deps for sub-apps (Activity Aggregator Update)
if [ -f "$HOME/Documents/Activity Aggregator Update/app/requirements.txt" ]; then
    pip install -q -r "$HOME/Documents/Activity Aggregator Update/app/requirements.txt"
fi

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
    deactivate 2>/dev/null
    exit 0
}
trap cleanup INT TERM

# Start Flask app
cd "$APP_DIR"
python3 app.py

# Cleanup on exit
cleanup
