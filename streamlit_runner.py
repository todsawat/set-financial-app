"""
Streamlit server runner — called by launcher.py via multiprocessing.
This runs in a separate process so that Streamlit's signal handlers work
correctly (they require the main thread of a process).
"""

import sys
import os


def run_streamlit(app_path, port):
    """Start the Streamlit server. This function blocks."""
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


if __name__ == "__main__":
    # Called directly: python streamlit_runner.py <app_path> <port>
    run_streamlit(sys.argv[1], int(sys.argv[2]))
