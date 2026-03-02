#!/bin/bash
# ============================================================
# SET Financial Analyzer — Local Build Script
# ============================================================
# Usage:
#   ./build_app.sh              # Build for current architecture
#   ./build_app.sh universal2   # Build universal binary (Intel + Apple Silicon)
#   ./build_app.sh arm64        # Build for Apple Silicon only
#   ./build_app.sh x86_64       # Build for Intel only
#   ./build_app.sh clean        # Clean build artifacts
#
# Prerequisites:
#   pip install pyinstaller
#   pip install -r requirements.txt
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  SET Financial Analyzer — Build Script${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# --- Clean mode ---
if [ "$1" = "clean" ]; then
    echo -e "${YELLOW}Cleaning build artifacts...${NC}"
    rm -rf build/ dist/ __pycache__/
    rm -f *.pyc
    echo -e "${GREEN}Done!${NC}"
    exit 0
fi

# --- Check prerequisites ---
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: python3 not found. Please install Python 3.10+${NC}"
    exit 1
fi

if ! python3 -c "import PyInstaller" &> /dev/null; then
    echo -e "${RED}ERROR: PyInstaller not found. Install it with:${NC}"
    echo "  pip install pyinstaller"
    exit 1
fi

if ! python3 -c "import streamlit" &> /dev/null; then
    echo -e "${RED}ERROR: Streamlit not found. Install dependencies with:${NC}"
    echo "  pip install -r requirements.txt"
    exit 1
fi

echo -e "${GREEN}All prerequisites OK${NC}"
echo ""

# --- Determine target architecture ---
TARGET_ARCH="$1"
ARCH_FLAG=""

if [ "$(uname)" = "Darwin" ]; then
    CURRENT_ARCH=$(uname -m)
    echo -e "${BLUE}Platform: macOS (${CURRENT_ARCH})${NC}"

    if [ -n "$TARGET_ARCH" ]; then
        ARCH_FLAG="--target-arch $TARGET_ARCH"
        echo -e "${BLUE}Target architecture: ${TARGET_ARCH}${NC}"

        if [ "$TARGET_ARCH" = "universal2" ] || [ "$TARGET_ARCH" = "arm64" ]; then
            # Check if Python is universal2
            PYTHON_ARCH=$(file "$(which python3)" | grep -c "universal")
            if [ "$PYTHON_ARCH" -eq 0 ] && [ "$TARGET_ARCH" != "$CURRENT_ARCH" ]; then
                echo -e "${YELLOW}WARNING: Your Python may not support ${TARGET_ARCH} architecture.${NC}"
                echo -e "${YELLOW}For universal2/arm64 builds from Intel, install universal2 Python from python.org${NC}"
                echo ""
                read -p "Continue anyway? [y/N] " -n 1 -r
                echo
                if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                    exit 1
                fi
            fi
        fi
    else
        echo -e "${BLUE}Target architecture: ${CURRENT_ARCH} (native)${NC}"
    fi
else
    echo -e "${BLUE}Platform: $(uname)${NC}"
fi

echo ""

# --- Build ---
echo -e "${YELLOW}Building SET Financial Analyzer...${NC}"
echo -e "${YELLOW}This may take 2-5 minutes...${NC}"
echo ""

python3 -m PyInstaller $ARCH_FLAG SET-Financial-Analyzer.spec

echo ""

# --- Results ---
if [ "$(uname)" = "Darwin" ]; then
    if [ -d "dist/SET-Financial-Analyzer.app" ]; then
        echo -e "${GREEN}============================================${NC}"
        echo -e "${GREEN}  BUILD SUCCESS!${NC}"
        echo -e "${GREEN}============================================${NC}"
        echo ""
        echo -e "  Output: ${BLUE}dist/SET-Financial-Analyzer.app${NC}"
        echo ""
        SIZE=$(du -sh "dist/SET-Financial-Analyzer.app" | cut -f1)
        echo -e "  Size: ${SIZE}"
        echo ""
        echo -e "  To run:"
        echo -e "    open dist/SET-Financial-Analyzer.app"
        echo ""
        echo -e "  To distribute:"
        echo -e "    cd dist && zip -r SET-Financial-Analyzer-macOS.zip SET-Financial-Analyzer.app"
        echo ""
    else
        echo -e "${RED}BUILD FAILED — .app not found in dist/${NC}"
        exit 1
    fi
else
    if [ -f "dist/SET-Financial-Analyzer" ] || [ -f "dist/SET-Financial-Analyzer.exe" ]; then
        echo -e "${GREEN}============================================${NC}"
        echo -e "${GREEN}  BUILD SUCCESS!${NC}"
        echo -e "${GREEN}============================================${NC}"
        echo ""
        echo -e "  Output: ${BLUE}dist/SET-Financial-Analyzer${NC}"
        echo ""
    else
        echo -e "${RED}BUILD FAILED — executable not found in dist/${NC}"
        exit 1
    fi
fi
