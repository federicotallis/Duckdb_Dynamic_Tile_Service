"""
Tile server for serving Overture Maps buildings as vector tiles using DuckDB ST_AsMVT.
"""
import duckdb
from flask import Flask, Response
from flask_cors import CORS
import math
import os
import threading

app = Flask(__name__)
CORS(app)

# Store current view stats (updated by map, read by Streamlit)
current_stats = {'count': 0, 'area': 0, 'bounds': None}

# DuckDB database path
_base_dir = os.path.dirname(__file__)
DB_FILE = os.path.join(_base_dir, "..", "nl_buildings.duckdb")

# Thread-local storage for DuckDB connections
_thread_local = threading.local()


def get_connection():
    """Get or create a thread-local DuckDB connection."""
    if not hasattr(_thread_local, 'connection'):
        con = duckdb.connect(DB_FILE, read_only=True)
        con.execute("LOAD spatial;")
        _thread_local.connection = con
    return _thread_local.connection


def tile_to_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert tile coordinates to WGS84 bounding box."""
    n = 2.0 ** z
    min_lon = x / n * 360.0 - 180.0
    max_lon = (x + 1) / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    max_lat = math.degrees(lat_rad)
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
    min_lat = math.degrees(lat_rad)
    return (min_lon, min_lat, max_lon, max_lat)


@app.route('/tiles/<int:z>/<int:x>/<int:y>.pbf')
def get_tile(z: int, x: int, y: int):
    """Serve a vector tile for the given z/x/y coordinates."""
    import time

    total_start = time.perf_counter()

    if z < 10:
        return Response(b'', mimetype='application/x-protobuf')

    min_lon, min_lat, max_lon, max_lat = tile_to_bbox(z, x, y)
    con = get_connection()

    try:
        with con.cursor() as cursor:
            query_start = time.perf_counter()

            query = f"""
                SELECT ST_AsMVT(tile, 'buildings') as mvt
                FROM (
                    SELECT
                        ST_AsMVTGeom(
                            ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857', TRUE),
                            ST_Extent(ST_TileEnvelope({z}, {x}, {y}))
                        ) as geometry,
                        id,
                        name,
                        height,
                        class
                    FROM buildings
                    WHERE bbox.xmin <= {max_lon}
                      AND bbox.xmax >= {min_lon}
                      AND bbox.ymin <= {max_lat}
                      AND bbox.ymax >= {min_lat}
                ) as tile
                WHERE geometry IS NOT NULL
            """
            result = cursor.execute(query).fetchone()
            query_time = time.perf_counter() - query_start

            tile_data = result[0] if result and result[0] else b''
            total_time = time.perf_counter() - total_start

            print(f"[TIMING] Tile {z}/{x}/{y}: "
                  f"total={total_time:.2f}s, "
                  f"query={query_time:.2f}s, "
                  f"size={len(tile_data)}bytes")

            response = Response(tile_data, mimetype='application/vnd.mapbox-vector-tile')
            response.headers['Cache-Control'] = 'public, max-age=3600'
            return response

    except Exception as e:
        print(f"Error generating tile {z}/{x}/{y}: {e}")
        import traceback
        traceback.print_exc()
        return Response(b'', mimetype='application/x-protobuf')


@app.route('/health')
def health():
    """Health check endpoint."""
    return Response('OK', mimetype='text/plain')


@app.route('/update-view', methods=['POST'])
def update_view():
    """Receive view bounds from map, store for Streamlit to use."""
    from flask import jsonify, request

    data = request.get_json()
    bounds = data.get('bounds')
    zoom = data.get('zoom')

    if not bounds:
        return jsonify({'status': 'error', 'message': 'No bounds provided'}), 400

    # Store bounds with zoom included
    bounds['zoom'] = zoom
    current_stats['bounds'] = bounds
    return jsonify({'status': 'ok'})


@app.route('/get-bounds')
def get_bounds():
    """Return current view bounds for Streamlit to query."""
    from flask import jsonify
    return jsonify({'bounds': current_stats.get('bounds')})


@app.route('/')
def index():
    """Serve the map HTML."""
    from flask import request

    center_lng = request.args.get('lng', '5.12')
    center_lat = request.args.get('lat', '52.09')
    zoom = request.args.get('zoom', '15')
    min_zoom = request.args.get('minzoom', '10')
    color = request.args.get('color', '#3388ff')
    opacity = request.args.get('opacity', '0.6')

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Overture Maps Buildings</title>
    <script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
    <link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet" />
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        const MIN_ZOOM = {min_zoom};

        const map = new maplibregl.Map({{
            container: 'map',
            style: {{
                version: 8,
                sources: {{
                    'osm': {{
                        type: 'raster',
                        tiles: ['https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png'],
                        tileSize: 256
                    }},
                    'buildings': {{
                        type: 'vector',
                        tiles: [window.location.origin + '/tiles/{{z}}/{{x}}/{{y}}.pbf'],
                        minzoom: MIN_ZOOM,
                        maxzoom: 16
                    }}
                }},
                layers: [
                    {{
                        id: 'osm-layer',
                        type: 'raster',
                        source: 'osm'
                    }},
                    {{
                        id: 'buildings-fill',
                        type: 'fill',
                        source: 'buildings',
                        'source-layer': 'buildings',
                        minzoom: MIN_ZOOM,
                        paint: {{
                            'fill-color': '{color}',
                            'fill-opacity': {opacity}
                        }}
                    }},
                    {{
                        id: 'buildings-outline',
                        type: 'line',
                        source: 'buildings',
                        'source-layer': 'buildings',
                        minzoom: MIN_ZOOM,
                        paint: {{
                            'line-color': '#333',
                            'line-width': 0.5
                        }}
                    }}
                ]
            }},
            center: [{center_lng}, {center_lat}],
            zoom: {zoom}
        }});

        map.addControl(new maplibregl.NavigationControl());

        // Send view bounds to server on load and after panning/zooming
        function updateViewStats() {{
            const bounds = map.getBounds();
            fetch('/update-view', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{
                    bounds: {{
                        north: bounds.getNorth(),
                        south: bounds.getSouth(),
                        east: bounds.getEast(),
                        west: bounds.getWest()
                    }},
                    zoom: map.getZoom()
                }})
            }});
        }}

        map.on('load', updateViewStats);
        map.on('moveend', updateViewStats);

        map.on('click', 'buildings-fill', (e) => {{
            const props = e.features[0].properties;
            let html = '<h3>Building</h3>';
            for (const [k, v] of Object.entries(props)) {{
                if (v && v !== 'null') html += '<p><b>' + k + ':</b> ' + v + '</p>';
            }}
            new maplibregl.Popup().setLngLat(e.lngLat).setHTML(html).addTo(map);
        }});

        map.on('mouseenter', 'buildings-fill', () => map.getCanvas().style.cursor = 'pointer');
        map.on('mouseleave', 'buildings-fill', () => map.getCanvas().style.cursor = '');
    </script>
</body>
</html>
"""
    return Response(html, mimetype='text/html')


def run_server(host: str = '127.0.0.1', port: int = 8080):
    """Run the tile server using waitress."""
    from waitress import serve
    print(f"Starting tile server on http://{host}:{port}")
    print(f"Using DuckDB: {DB_FILE}")
    serve(app, host=host, port=port, threads=4)


if __name__ == '__main__':
    print(f"Starting tile server on http://127.0.0.1:8080")
    print(f"Using DuckDB: {DB_FILE}")
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)
