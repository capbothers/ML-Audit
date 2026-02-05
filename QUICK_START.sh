#!/bin/bash
# Quick Start Script for ML-Audit Platform

echo "╔════════════════════════════════════════════════════════════╗"
echo "║   ML-Audit Growth Intelligence Platform - Quick Start     ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python $python_version found"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate
echo "✓ Virtual environment created"
echo ""

# Install dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Setup environment
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "⚠ Please edit .env with your API credentials before running!"
    echo ""
fi

# Create directories
echo "Creating required directories..."
mkdir -p credentials data models logs
echo "✓ Directories created"
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                     Setup Complete!                        ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your API credentials"
echo "2. Place credential files in credentials/ directory"
echo "3. Run: python app/main.py"
echo "4. Visit: http://localhost:8000/docs"
echo ""
echo "For detailed setup instructions, see SETUP_GUIDE.md"
echo ""
