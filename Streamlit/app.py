"""
Streamlit app for viewing Overture Maps buildings using DuckDB-generated vector tiles.
"""
import streamlit as st
import time
import threading
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Streamlit Buildings Viewer",
    page_icon="🏠",
    layout="wide"
)

TILE_SERVER_HOST = "127.0.0.1"
TILE_SERVER_PORT = 8080


def start_tile_server():
    """Start the tile server in a background thread."""
    from tile_server import run_server
    run_server(host=TILE_SERVER_HOST, port=TILE_SERVER_PORT)


def is_tile_server_running():
    """Check if the tile server is already running."""
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(1)
        result = sock.connect_ex((TILE_SERVER_HOST, TILE_SERVER_PORT))
        return result == 0
    except:
        return False
    finally:
        sock.close()


def ensure_tile_server_running():
    """Ensure the tile server is running, start if not."""
    if is_tile_server_running():
        return True

    if 'tile_server_started' not in st.session_state:
        thread = threading.Thread(target=start_tile_server, daemon=True)
        thread.start()
        st.session_state.tile_server_started = True
        time.sleep(2)
        return is_tile_server_running()
    return False


def get_view_bounds():
    """Fetch current view bounds from tile server."""
    import requests
    try:
        response = requests.get(f"http://{TILE_SERVER_HOST}:{TILE_SERVER_PORT}/get-bounds", timeout=1)
        if response.ok:
            return response.json().get('bounds')
    except:
        pass
    return None


import duckdb
import os

@st.cache_resource
def get_db_path():
    """Get the database path."""
    return os.path.join(os.path.dirname(__file__), "..", "nl_buildings.duckdb")


def query_stats(bounds):
    """Query DuckDB directly for building stats in bounds."""
    if not bounds:
        return {'count': 0, 'area': 0}

    north = bounds.get('north')
    south = bounds.get('south')
    east = bounds.get('east')
    west = bounds.get('west')

    try:
        with duckdb.connect(get_db_path(), read_only=True) as con:
            con.execute("LOAD spatial;")
            query = f"""
                SELECT
                    COUNT(*) as count,
                    COALESCE(SUM(ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857', TRUE))), 0) as area
                FROM buildings
                WHERE bbox.xmin <= {east}
                  AND bbox.xmax >= {west}
                  AND bbox.ymin <= {north}
                  AND bbox.ymax >= {south}
            """
            result = con.execute(query).fetchone()
            if result is None:
                return {'count': 0, 'area': 0}
            return {'count': result[0] or 0, 'area': result[1] or 0}
    except Exception as e:
        print(f"Error querying stats: {e}")
        return {'count': 0, 'area': 0}


def main():
    st.title("Streamlit Buildings Viewer")

    server_running = ensure_tile_server_running()

    if not server_running:
        st.warning(f"""
        Tile server not detected on port {TILE_SERVER_PORT}.
        Please run the tile server separately:
        ```
        python tile_server.py
        ```
        Then refresh this page.
        """)
        st.stop()

    with st.sidebar:
        st.header("Map Settings")

        st.subheader("Buildings Style")
        building_color = st.color_picker("Building Color", value="#3388ff")
        building_opacity = st.slider("Building Opacity", min_value=0.1, max_value=1.0, value=0.6)

        st.divider()
        st.caption(f"Tile server: http://{TILE_SERVER_HOST}:{TILE_SERVER_PORT}")
        st.caption("Data: Overture Maps Foundation")

    # Initialize session state first
    if 'last_bounds' not in st.session_state:
        st.session_state.last_bounds = None
    if 'stats' not in st.session_state:
        st.session_state.stats = {'count': 0, 'area': 0}

    # Poll for bounds changes (lightweight check)
    # DuckDB is only queried when bounds actually change
    st_autorefresh(interval=1500, key="bounds_check")

    # Get current bounds from tile server
    bounds = get_view_bounds()

    # Create a stable key by rounding coordinates to avoid floating-point precision issues
    def make_bounds_key(b):
        if not b:
            return None
        return (
            round(b.get('north', 0), 4),
            round(b.get('south', 0), 4),
            round(b.get('east', 0), 4),
            round(b.get('west', 0), 4),
            round(b.get('zoom', 0), 1)
        )

    bounds_key = make_bounds_key(bounds)

    # Only query DuckDB when bounds have changed
    if bounds_key != st.session_state.last_bounds:
        st.session_state.last_bounds = bounds_key
        st.session_state.stats = query_stats(bounds)

    stats = st.session_state.stats
    st.markdown(f"**{stats['count']:,}** buildings | **{stats['area']:,.0f}** m² total area")

    # Embed map via iframe
    import urllib.parse
    params = urllib.parse.urlencode({
        'color': building_color,
        'opacity': building_opacity
    })
    flask_map_url = f"http://{TILE_SERVER_HOST}:{TILE_SERVER_PORT}/?{params}"

    iframe_html = f'''
    <iframe
        src="{flask_map_url}"
        width="100%"
        height="700"
        style="border: none; border-radius: 4px;"
    ></iframe>
    '''
    st.components.v1.html(iframe_html, height=720)


if __name__ == "__main__":
    main()
