"""
SET Financial Analyzer — Desktop Launcher
==========================================
Launches the Streamlit app inside a native desktop window using pywebview.
No external browser needed — the app runs entirely within its own window.

Flow:
1. Find a free port
2. Start Streamlit server via multiprocessing (avoids fork-bomb in frozen app)
3. Wait for the server to be ready
4. Open a native webview window pointing at the local server
5. When window closes, terminate the server process and exit
"""

import os
import sys
import socket
import time
import atexit
import multiprocessing


def get_base_path():
    """
    Get the base path for bundled resources.
    When frozen by PyInstaller, files are extracted to a temp dir (_MEIPASS).
    In dev mode, use the script's directory.
    """
    if getattr(sys, "frozen", False):
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
    return 8501


def wait_for_server(port, timeout=45):
    """Block until the Streamlit server is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect(("127.0.0.1", port))
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


def _run_streamlit(app_path, port):
    """
    Target function for the Streamlit child process.
    Runs in its own process so signal handlers work correctly.
    """
    sys.argv = [
        "streamlit",
        "run",
        app_path,
        f"--server.port={port}",
        "--server.headless=true",
        "--server.address=localhost",
        "--browser.serverAddress=localhost",
        "--browser.gatherUsageStats=false",
        "--global.developmentMode=false",
        "--client.toolbarMode=minimal",
    ]
    from streamlit.web import cli as stcli
    stcli.main()


def main():
    # Required for PyInstaller frozen multiprocessing on Windows/macOS
    multiprocessing.freeze_support()

    base_path = get_base_path()
    app_path = os.path.join(base_path, "app.py")

    if not os.path.exists(app_path):
        print(f"ERROR: app.py not found at {app_path}")
        print(f"Base path: {base_path}")
        print(f"Contents: {os.listdir(base_path)}")
        sys.exit(1)

    port = find_free_port()
    url = f"http://localhost:{port}"

    # --- Start Streamlit in a child process ---
    proc = multiprocessing.Process(
        target=_run_streamlit, args=(app_path, port), daemon=True
    )
    proc.start()

    def cleanup():
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=3)
            if proc.is_alive():
                proc.kill()

    atexit.register(cleanup)

    # --- Wait for server to be ready ---
    print(f"Starting Streamlit server on port {port}...")
    if not wait_for_server(port, timeout=45):
        print("ERROR: Streamlit server did not start in time.")
        cleanup()
        sys.exit(1)
    print("Server ready!")

    # --- Open native window (must be in main thread on macOS) ---
    import webview

    window = webview.create_window(
        title="SET Financial Analyzer",
        url=url,
        width=1400,
        height=900,
        min_size=(900, 600),
        text_select=True,
    )
    webview.start()

    # Window closed — clean up and exit
    cleanup()
    os._exit(0)


if __name__ == "__main__":
    main()
