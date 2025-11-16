#!/bin/bash

# start_localhost.sh - Quick Start Script for Dynamic ETL Pipeline

echo "======================================================================"
echo "🚀 Dynamic ETL Pipeline - AI-Powered Multi-Database Categorization"
echo "======================================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check Python
echo -e "${BLUE}📌 Step 1: Checking Python...${NC}"
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✅ Found: $PYTHON_VERSION${NC}"
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_VERSION=$(python --version)
    echo -e "${GREEN}✅ Found: $PYTHON_VERSION${NC}"
    PYTHON_CMD="python"
else
    echo -e "${RED}❌ Python not found! Please install Python 3.7+${NC}"
    exit 1
fi
echo ""

# Check MongoDB
echo -e "${BLUE}📌 Step 2: Checking MongoDB...${NC}"
if command -v mongod &> /dev/null; then
    echo -e "${GREEN}✅ MongoDB installed${NC}"

    # Check if MongoDB is running
    if pgrep -x "mongod" > /dev/null; then
        echo -e "${GREEN}✅ MongoDB is running${NC}"
    else
        echo -e "${YELLOW}⚠️  MongoDB not running. Starting MongoDB...${NC}"
        # Try to start MongoDB in background
        mongod --fork --logpath /tmp/mongodb.log --dbpath ~/data/db 2>/dev/null || {
            echo -e "${YELLOW}ℹ️  Could not auto-start MongoDB. Please start it manually:${NC}"
            echo -e "${YELLOW}   mongod${NC}"
            echo -e "${YELLOW}   OR use MongoDB Compass${NC}"
        }
    fi
else
    echo -e "${YELLOW}⚠️  MongoDB not found. Install from: https://www.mongodb.com/try/download/community${NC}"
    echo -e "${YELLOW}   OR use MongoDB Atlas (cloud): https://www.mongodb.com/cloud/atlas${NC}"
fi
echo ""

# Check Ollama
echo -e "${BLUE}📌 Step 3: Checking Ollama (for AI features)...${NC}"
if command -v ollama &> /dev/null; then
    echo -e "${GREEN}✅ Ollama installed${NC}"

    # Check if Ollama is running
    if curl -s http://localhost:11434/api/tags &> /dev/null; then
        echo -e "${GREEN}✅ Ollama is running${NC}"

        # Check if llama2 model is available
        if ollama list | grep -q "llama2"; then
            echo -e "${GREEN}✅ llama2 model available${NC}"
        else
            echo -e "${YELLOW}⚠️  llama2 model not found. Pulling model (this may take a few minutes)...${NC}"
            ollama pull llama2
        fi
    else
        echo -e "${YELLOW}⚠️  Ollama not running. Starting Ollama...${NC}"
        ollama serve > /tmp/ollama.log 2>&1 &
        sleep 3
        echo -e "${GREEN}✅ Ollama started${NC}"

        # Pull llama2 if needed
        if ! ollama list | grep -q "llama2"; then
            echo -e "${YELLOW}📥 Pulling llama2 model (first time setup)...${NC}"
            ollama pull llama2
        fi
    fi
else
    echo -e "${YELLOW}⚠️  Ollama not installed (AI features will be disabled)${NC}"
    echo -e "${YELLOW}   Install from: https://ollama.ai${NC}"
    echo -e "${YELLOW}   OR: brew install ollama${NC}"
    echo ""
    echo -e "${BLUE}ℹ️  The app will still work without Ollama, but AI features won't be available${NC}"
fi
echo ""

# Install Python dependencies
echo -e "${BLUE}📌 Step 4: Installing Python dependencies...${NC}"
if [ -f "requirements.txt" ]; then
    $PYTHON_CMD -m pip install -q -r requirements.txt
    echo -e "${GREEN}✅ Dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠️  requirements.txt not found${NC}"
fi
echo ""

# Choose which app to run
echo -e "${BLUE}📌 Step 5: Choose application to run:${NC}"
echo ""
echo "  1) 🎯 Categorized App (Multi-Database with AI)"
echo "  2) 🤖 Original App (Single Database)"
echo "  3) 📊 Analytics Dashboard (Dash)"
echo ""
read -p "Enter choice (1-3) [default: 1]: " CHOICE
CHOICE=${CHOICE:-1}

echo ""
echo "======================================================================"
echo -e "${GREEN}🚀 Starting Dynamic ETL Pipeline...${NC}"
echo "======================================================================"
echo ""

case $CHOICE in
    1)
        echo -e "${BLUE}Starting Categorized App with AI support...${NC}"
        echo -e "${GREEN}📍 Access at: http://localhost:5001${NC}"
        echo ""
        $PYTHON_CMD app_categorized.py
        ;;
    2)
        echo -e "${BLUE}Starting Original App...${NC}"
        echo -e "${GREEN}📍 Access at: http://localhost:5001${NC}"
        echo ""
        $PYTHON_CMD app.py
        ;;
    3)
        echo -e "${BLUE}Starting Analytics Dashboard...${NC}"
        echo -e "${GREEN}📍 Access at: http://localhost:8050${NC}"
        echo ""
        $PYTHON_CMD dashboard.py
        ;;
    *)
        echo -e "${RED}❌ Invalid choice${NC}"
        exit 1
        ;;
esac
