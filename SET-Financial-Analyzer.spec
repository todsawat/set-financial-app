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
import re
from PyInstaller.utils.hooks import collect_all, collect_submodules

# --- Read version from version.py ---
_version_file = os.path.join(SPECPATH, "version.py")
_app_version = "1.0.0"
try:
    with open(_version_file) as _f:
        _m = re.search(r'__version__\s*=\s*"(.+?)"', _f.read())
        if _m:
            _app_version = _m.group(1)
except Exception:
    pass

# --- Collect Streamlit, Plotly, pywebview, and certifi fully ---
streamlit_datas, streamlit_binaries, streamlit_hiddenimports = collect_all("streamlit")
plotly_datas, plotly_binaries, plotly_hiddenimports = collect_all("plotly")
webview_datas, webview_binaries, webview_hiddenimports = collect_all("webview")
certifi_datas, certifi_binaries, certifi_hiddenimports = collect_all("certifi")

# --- App source files ---
app_datas = [
    ("app.py", "."),
    ("financial_data.py", "."),
    ("set_scraper.py", "."),
    ("version.py", "."),
    ("streamlit_runner.py", "."),
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
    "webview",          # pywebview native window
    "multiprocessing",  # used by launcher for child process
    "certifi",          # SSL CA certificates for requests
]

# --- Platform-specific hidden imports for pywebview backends ---
if sys.platform == "win32":
    # Windows: pywebview uses EdgeChromium via pythonnet/clr_loader
    for pkg in ["clr_loader", "pythonnet"]:
        extra_hidden.append(pkg)
        try:
            _d, _b, _h = collect_all(pkg)
            webview_datas += _d
            webview_binaries += _b
            webview_hiddenimports += _h
        except Exception:
            pass

# --- pyobjc frameworks needed by pywebview on macOS ---
if sys.platform == "darwin":
    for fw in [
        "objc", "Foundation", "AppKit", "WebKit",
        "Cocoa", "Quartz", "UniformTypeIdentifiers", "Security",
    ]:
        extra_hidden.append(fw)
        try:
            _d, _b, _h = collect_all(fw)
            webview_datas += _d
            webview_binaries += _b
            webview_hiddenimports += _h
        except Exception:
            pass

a = Analysis(
    ["launcher.py"],
    pathex=[],
    binaries=streamlit_binaries + plotly_binaries + webview_binaries + certifi_binaries,
    datas=app_datas + streamlit_datas + plotly_datas + webview_datas + certifi_datas,
    hiddenimports=(
        extra_hidden
        + webview_hiddenimports
        + certifi_hiddenimports
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
        icon="assets/icon.icns",
        bundle_identifier="com.set-financial.analyzer",
        info_plist={
            "CFBundleDisplayName": "SET Financial Analyzer",
            "CFBundleShortVersionString": _app_version,
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
        icon="assets/icon.ico",
    )
