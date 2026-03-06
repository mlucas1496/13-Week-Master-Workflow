#!/bin/bash

# 13WCF Unified Workflow Launcher
# ================================
# Starts the Weekly Balances Vite server (Step 1) and the unified Flask app.

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
VITE_DIR="$HOME/13wcf-rollforward"

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

# Start the Vite dev server for Step 1 (Weekly Balances Rollforward) in background
VITE_PID=""
if [ -d "$VITE_DIR" ]; then
    echo "Starting Weekly Balances app (Vite) on port 5174..."
    (
        export PATH="$HOME/.nvm/versions/node/$(ls $HOME/.nvm/versions/node/ 2>/dev/null | tail -1)/bin:$HOME/.nvm/nvm.sh:$PATH"
        # Source nvm if available
        if [ -s "$HOME/.nvm/nvm.sh" ]; then
            source "$HOME/.nvm/nvm.sh"
        fi
        cd "$VITE_DIR"
        npm run dev -- --port 5174 > /tmp/13wcf-vite.log 2>&1
    ) &
    VITE_PID=$!
    sleep 2
    echo "  Vite server started (PID: $VITE_PID)"
else
    echo "  Warning: $VITE_DIR not found. Step 1 (Weekly Balances) will need to be run separately."
fi

# Create required directories
mkdir -p "$APP_DIR/uploads" "$APP_DIR/outputs"

echo ""
echo "Starting unified workflow app..."
echo "  Main app:          http://localhost:8888"
echo "  Weekly Balances:   http://localhost:5174"
echo ""
echo "  Press Ctrl+C to stop all servers"
echo "=========================================="
echo ""

# Open browser
(sleep 2 && open "http://localhost:8888") &

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down..."
    if [ -n "$VITE_PID" ]; then
        kill $VITE_PID 2>/dev/null
        # Also kill any node processes on port 5174
        lsof -ti TCP:5174 | xargs kill 2>/dev/null
    fi
    deactivate 2>/dev/null
    exit 0
}
trap cleanup INT TERM

# Start Flask app
cd "$APP_DIR"
python3 app.py

# Cleanup on exit
cleanup
