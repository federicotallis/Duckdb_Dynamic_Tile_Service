# DuckDB Dynamic Tile Service

A comparison of **Streamlit** (Python) and **R Shiny** for building interactive map dashboards with DuckDB's `ST_AsMVT` function.

## Overview

This project demonstrates on-demand vector tile generation from DuckDB to visualize millions of data points without crashing the browser or pre-generating tile pyramids. The test dataset contains **21 million buildings** from Overture Maps (Netherlands).

### Why ST_AsMVT?

Traditional approaches to mapping large datasets face challenges:
- **GeoJSON**: Crashes browsers with millions of features
- **Pre-generated tiles**: Expensive preprocessing, stale when data changes
- **Tile servers (PostGIS)**: Requires running a separate database server

DuckDB's `ST_AsMVT` generates tiles on-demand in **milliseconds** from an embedded database file.

## The Dashboard

Both implementations display:
- Full-screen map with vector tiles served from DuckDB
- Reactive building count and total area statistics
- Style controls (color, opacity)

### R Shiny

![R Shiny Demo](Shiny/RShiny.gif)

[Download full quality video](Shiny/RShiny.mp4)

### Streamlit

![Streamlit Demo](Streamlit/Streamlit.gif)

[Download full quality video](Streamlit/Streamlit.mp4)

## Project Structure

```
├── Shiny/                    # R Shiny implementation
│   └── app.R
├── Streamlit/                # Python Streamlit implementation
│   ├── app.py
│   ├── tile_server.py
│   └── requirements.txt
├── download_nl_buildings.py  # Download data from Overture Maps
├── build_indexed_db.py       # Build indexed DuckDB database
└── README.md
```

## Comparison

| Aspect | Streamlit | Shiny |
|--------|-----------|-------|
| MapGL Support | Iframe workaround | Native via `mapgl` |
| Tile Server | Separate Flask process | Integrated `httpuv` |
| Map-App Communication | HTTP polling (1.5s delay) | Reactive `input$map_bbox` |
| Architecture | 2 processes | 1 process |

**Conclusion**: For map-centric dashboards, Shiny provided a more intuitive and reliable experience.

## Getting Started

### 1. Download Data

```bash
python download_nl_buildings.py
python build_indexed_db.py
```

### 2. Run Shiny App

```r
# Install dependencies
install.packages(c("shiny", "bslib", "colourpicker", "mapgl", "duckdb", "httpuv", "DBI"))

# Run app
shiny::runApp("Shiny")
```

### 3. Run Streamlit App

```bash
cd Streamlit
pip install -r requirements.txt

# Terminal 1: Start tile server
python tile_server.py

# Terminal 2: Start Streamlit
streamlit run app.py
```

## Credits

- [Kyle Walker](https://walker-data.com/mapgl/) - `mapgl` R package
- [Max Gabrielsson](https://github.com/Maxxen) - DuckDB spatial extension
- [Overture Maps Foundation](https://overturemaps.org/) - Building data

## References

- [DuckDB + Flask tile serving](https://gist.github.com/Maxxen/37e4a9f8595ea5e6a20c0c8fbbefe955)
- [DuckDB + mapgl integration](https://gist.github.com/walkerke/c90ab6b8f403169e615eabeb0339b15b)
