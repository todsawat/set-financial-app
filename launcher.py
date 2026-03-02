"""
SET Financial Analyzer — Desktop Launcher
==========================================
Wrapper script that launches the Streamlit app as a standalone desktop application.
Used by PyInstaller to create .app (macOS) and .exe (Windows) bundles.

This script:
1. Determines the correct path to app.py (works both in dev and frozen mode)
2. Finds a free port to avoid conflicts
3. Launches Streamlit in headless mode
4. Opens the default browser automatically
"""

import os
import sys
import socket
import webbrowser
import threading


def get_base_path():
    """
    Get the base path for bundled resources.
    When frozen by PyInstaller, files are extracted to a temp dir (_MEIPASS).
    In dev mode, use the script's directory.
    """
    if getattr(sys, "frozen", False):
        # Running as a PyInstaller bundle
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))


def find_free_port(start=8501, end=8600):
    """Find a free port in the given range."""
    for port in range(start, end):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return port
        except OSError:
            continue
    return 8501  # fallback


def open_browser(port):
    """Open the browser after a short delay to let Streamlit start."""
    import time
    time.sleep(2.5)
    webbrowser.open(f"http://localhost:{port}")


def main():
    base_path = get_base_path()
    app_path = os.path.join(base_path, "app.py")

    # Verify app.py exists
    if not os.path.exists(app_path):
        print(f"ERROR: app.py not found at {app_path}")
        print(f"Base path: {base_path}")
        print(f"Contents: {os.listdir(base_path)}")
        sys.exit(1)

    port = find_free_port()

    # Open browser in a background thread
    browser_thread = threading.Thread(target=open_browser, args=(port,), daemon=True)
    browser_thread.start()

    # Launch Streamlit
    sys.argv = [
        "streamlit",
        "run",
        app_path,
        f"--server.port={port}",
        "--server.headless=true",
        "--server.address=localhost",
        "--browser.gatherUsageStats=false",
        "--global.developmentMode=false",
        "--client.toolbarMode=minimal",
    ]

    from streamlit.web import cli as stcli
    stcli.main()


if __name__ == "__main__":
    main()
