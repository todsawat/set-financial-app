# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for SET Financial Analyzer
=================================================
Build commands:

  macOS (Apple Silicon .app from Intel, requires universal2 Python):
    pyinstaller --target-arch universal2 SET-Financial-Analyzer.spec

  macOS (native arch):
    pyinstaller SET-Financial-Analyzer.spec

  Windows:
    pyinstaller SET-Financial-Analyzer.spec
"""

import sys
import os
from PyInstaller.utils.hooks import collect_all, collect_submodules

# --- Collect Streamlit and Plotly fully ---
streamlit_datas, streamlit_binaries, streamlit_hiddenimports = collect_all("streamlit")
plotly_datas, plotly_binaries, plotly_hiddenimports = collect_all("plotly")

# --- App source files ---
app_datas = [
    ("app.py", "."),
    ("financial_data.py", "."),
    ("set_scraper.py", "."),
    ("version.py", "."),
]

# --- Hidden imports that PyInstaller may miss ---
extra_hidden = [
    "openpyxl",
    "xlrd",
    "requests",
    "pandas",
    "plotly",
    "streamlit",
    "streamlit.web.cli",
    "streamlit.runtime.scriptrunner",
    "streamlit.runtime.caching",
    "PIL",              # sometimes needed by Streamlit
    "pkg_resources",
    "importlib_metadata",
]

a = Analysis(
    ["launcher.py"],
    pathex=[],
    binaries=streamlit_binaries + plotly_binaries,
    datas=app_datas + streamlit_datas + plotly_datas,
    hiddenimports=(
        extra_hidden
        + streamlit_hiddenimports
        + plotly_hiddenimports
        + collect_submodules("streamlit")
        + collect_submodules("plotly")
    ),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "tkinter",       # not needed, saves ~10 MB
        "matplotlib",    # not used
        "scipy",         # not used
        "numpy.tests",
        "pandas.tests",
    ],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

# --- Platform-specific settings ---
if sys.platform == "darwin":
    # macOS: create .app bundle
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        name="SET-Financial-Analyzer",
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=False,       # UPX breaks macOS code signing
        console=False,   # No terminal window
    )
    coll = COLLECT(
        exe,
        a.binaries,
        a.datas,
        strip=False,
        upx=False,
        name="SET-Financial-Analyzer",
    )
    app = BUNDLE(
        coll,
        name="SET-Financial-Analyzer.app",
        icon=None,       # Add your .icns file path here if you have one
        bundle_identifier="com.set-financial.analyzer",
        info_plist={
            "CFBundleDisplayName": "SET Financial Analyzer",
            "CFBundleShortVersionString": "1.0.0",
            "NSHighResolutionCapable": True,
            "LSMinimumSystemVersion": "10.15",
        },
    )
else:
    # Windows / Linux: create single executable
    exe = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.datas,
        [],
        name="SET-Financial-Analyzer",
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        console=False,   # No console window on Windows
        icon=None,       # Add your .ico file path here if you have one
    )
