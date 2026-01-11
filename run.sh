#!/bin/bash
# Quick start script for the MS MQTT GUI application

cd /opt/lqit/ms-mqtt-gui || {
    echo "Error: Could not change directory to /opt/lqit/ms-mqtt-gui"
    exit 1
}

echo "======================================"
echo "MS MQTT GUI Application"
echo "======================================"
echo

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed."
    echo "Please install Python 3.7 or higher."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "Python version: $PYTHON_VERSION"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check if dependencies are installed
if ! python -c "import PyQt5" &> /dev/null; then
    echo
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    echo "Dependencies already installed."
fi

echo
echo "======================================"
echo "Starting application..."
echo "======================================"
echo

# Run the application
python main.py /etc/lqit/ms-mqtt-gui-config.yaml