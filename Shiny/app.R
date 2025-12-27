# Overture Maps Buildings Viewer - Shiny App
# Serves vector tiles from DuckDB and displays reactive statistics

library(shiny)
library(bslib)
library(colourpicker)
library(mapgl)
library(duckdb)
library(httpuv)
library(DBI)

# ============================================================================
# Configuration
# ============================================================================

DB_PATH <- "../nl_buildings.duckdb"
TILE_SERVER_PORT <- 8081
MIN_ZOOM <- 10

# ============================================================================
# Tile Server Functions
# ============================================================================

#' Parse tile coordinates from URL path
#' @param path URL path like "/tiles/15/16892/10895.pbf"
#' @return List with z, x, y or NULL if invalid
parse_tile_path <- function(path) {
  pattern <- "^/tiles/(\\d+)/(\\d+)/(\\d+)\\.pbf$"
  matches <- regmatches(path, regexec(pattern, path))[[1]]

  if (length(matches) == 4) {
    list(
      z = as.integer(matches[2]),
      x = as.integer(matches[3]),
      y = as.integer(matches[4])
    )
  } else {
    NULL
  }
}

#' Convert tile coordinates to WGS84 bounding box
#' @param z Zoom level
#' @param x Tile column
#' @param y Tile row
#' @return Named vector with min_lon, min_lat, max_lon, max_lat
tile_to_bbox <- function(z, x, y) {
  n <- 2^z
  min_lon <- x / n * 360 - 180
  max_lon <- (x + 1) / n * 360 - 180

  lat_rad_max <- atan(sinh(pi * (1 - 2 * y / n)))
  max_lat <- lat_rad_max * 180 / pi

  lat_rad_min <- atan(sinh(pi * (1 - 2 * (y + 1) / n)))
  min_lat <- lat_rad_min * 180 / pi

  c(min_lon = min_lon, min_lat = min_lat, max_lon = max_lon, max_lat = max_lat)
}

#' Generate a vector tile from DuckDB
#' @param con DuckDB connection
#' @param z Zoom level
#' @param x Tile column
#' @param y Tile row
#' @return Raw vector containing the MVT tile data
generate_tile <- function(con, z, x, y) {
  if (z < MIN_ZOOM) {
    return(raw(0))
  }

  bbox <- tile_to_bbox(z, x, y)

  query <- sprintf("
    SELECT ST_AsMVT(tile, 'buildings') as mvt
    FROM (
      SELECT
        ST_AsMVTGeom(
          ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857', TRUE),
          ST_Extent(ST_TileEnvelope(%d, %d, %d))
        ) AS geometry,
        id,
        name,
        height,
        class
      FROM buildings
      WHERE bbox.xmin <= %f
        AND bbox.xmax >= %f
        AND bbox.ymin <= %f
        AND bbox.ymax >= %f
    ) AS tile
    WHERE geometry IS NOT NULL
  ", z, x, y, bbox["max_lon"], bbox["min_lon"], bbox["max_lat"], bbox["min_lat"])

  tryCatch({
    result <- dbGetQuery(con, query)
    if (nrow(result) > 0 && !is.null(result$mvt[[1]])) {
      result$mvt[[1]]
    } else {
      raw(0)
    }
  }, error = function(e) {
    message("Tile error: ", e$message)
    raw(0)
  })
}

#' Start the tile server
#' @param con DuckDB connection
#' @param port Port to listen on
#' @return httpuv server handle
start_tile_server <- function(con, port = TILE_SERVER_PORT) {
  tile_app <- list(
    call = function(req) {
      path <- req$PATH_INFO

      # Handle CORS preflight
      if (req$REQUEST_METHOD == "OPTIONS") {
        return(list(
          status = 200L,
          headers = list(
            "Access-Control-Allow-Origin" = "*",
            "Access-Control-Allow-Methods" = "GET, OPTIONS",
            "Access-Control-Allow-Headers" = "*"
          ),
          body = ""
        ))
      }

      # Parse tile coordinates
      coords <- parse_tile_path(path)

      if (!is.null(coords)) {
        tile_blob <- generate_tile(con, coords$z, coords$x, coords$y)

        list(
          status = 200L,
          headers = list(
            "Content-Type" = "application/vnd.mapbox-vector-tile",
            "Access-Control-Allow-Origin" = "*",
            "Cache-Control" = "public, max-age=3600"
          ),
          body = tile_blob
        )
      } else {
        # TileJSON endpoint for mapgl
        if (path == "/" || path == "/tiles.json") {
          tilejson <- sprintf('{
            "tilejson": "3.0.0",
            "tiles": ["http://127.0.0.1:%d/tiles/{z}/{x}/{y}.pbf"],
            "minzoom": %d,
            "maxzoom": 16,
            "vector_layers": [{"id": "buildings"}]
          }', port, MIN_ZOOM)
          list(
            status = 200L,
            headers = list(
              "Content-Type" = "application/json",
              "Access-Control-Allow-Origin" = "*"
            ),
            body = tilejson
          )
        } else if (path == "/health") {
          list(status = 200L, headers = list("Content-Type" = "text/plain"), body = "OK")
        } else {
          list(status = 404L, headers = list("Content-Type" = "text/plain"), body = "Not Found")
        }
      }
    }
  )

  message("Starting tile server on http://127.0.0.1:", port)
  startDaemonizedServer("127.0.0.1", port, tile_app)
}

# ============================================================================
# Database Functions
# ============================================================================

#' Query building statistics for a bounding box
#' @param con DuckDB connection
#' @param bbox List with xmin, xmax, ymin, ymax
#' @return Data frame with count and area
query_stats <- function(con, bbox) {
  if (is.null(bbox)) {
    return(data.frame(count = 0, area = 0))
  }

  query <- sprintf("
    SELECT
      COUNT(*) as count,
      COALESCE(SUM(ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857', TRUE))), 0) as area
    FROM buildings
    WHERE bbox.xmin <= %f
      AND bbox.xmax >= %f
      AND bbox.ymin <= %f
      AND bbox.ymax >= %f
  ", bbox$xmax, bbox$xmin, bbox$ymax, bbox$ymin)

  tryCatch({
    dbGetQuery(con, query)
  }, error = function(e) {
    message("Stats error: ", e$message)
    data.frame(count = 0, area = 0)
  })
}

# ============================================================================
# Initialize Database and Tile Server
# ============================================================================

# Connect to DuckDB
message("Connecting to DuckDB: ", DB_PATH)
con <- dbConnect(duckdb::duckdb(), DB_PATH, read_only = TRUE)
dbExecute(con, "LOAD spatial;")

# Start tile server
tile_server <- start_tile_server(con, TILE_SERVER_PORT)

# Cleanup on exit
onStop(function() {
  message("Stopping tile server...")
  stopDaemonizedServer(tile_server)
  message("Disconnecting from DuckDB...")
  dbDisconnect(con)
})

# ============================================================================
# Shiny UI
# ============================================================================

ui <- page_fillable(
  theme = bs_theme(
    version = 5,
    bootswatch = "flatly",
    primary = "#3388ff"
  ),
  padding = 0,

  # Full-screen map
  maplibreOutput("map", height = "100%"),

  # Floating sidebar panel
  absolutePanel(
    top = 20, left = 20,
    width = 320,
    class = "card shadow",
    style = "background: white; border-radius: 8px; padding: 20px;",

    h4("RShiny Buildings Viewer", class = "card-title mb-3"),

    hr(),

    h5("Style", class = "text-muted"),

    colourInput("color", "Building Color", value = "#3388ff",
                showColour = "both", palette = "limited"),

    sliderInput("opacity", "Building Opacity",
                min = 0.1, max = 1.0, value = 0.6, step = 0.1),

    hr(),

    h5("Statistics", class = "text-muted"),
    tags$div(
      class = "fs-5",
      tags$div(class = "mb-1", textOutput("building_count")),
      tags$div(textOutput("total_area"))
    ),

    hr(),

    tags$small(
      class = "text-muted",
      "Data: Overture Maps Foundation"
    )
  )
)

# ============================================================================
# Shiny Server
# ============================================================================

server <- function(input, output, session) {

  # Reactive: Get current map bounds and query stats
  stats <- reactive({
    bbox <- input$map_bbox
    req(bbox)  # Wait until bbox is available
    query_stats(con, bbox)
  })

  # Output: Building count
  output$building_count <- renderText({
    s <- stats()
    paste(format(s$count, big.mark = ","), "buildings")
  })

  # Output: Total area
  output$total_area <- renderText({
    s <- stats()
    paste(format(round(s$area), big.mark = ","), "m\u00B2 total area")
  })

  # Output: MapLibre map
  output$map <- renderMaplibre({
    maplibre(
      center = c(5.12, 52.09),
      zoom = 15
    ) |>
      add_vector_source(
        id = "buildings",
        tiles = paste0("http://127.0.0.1:", TILE_SERVER_PORT, "/tiles/{z}/{x}/{y}.pbf"),
        minzoom = MIN_ZOOM,
        maxzoom = 16
      ) |>
      add_fill_layer(
        id = "buildings-fill",
        source = "buildings",
        source_layer = "buildings",
        fill_color = input$color,
        fill_opacity = input$opacity
      ) |>
      add_line_layer(
        id = "buildings-outline",
        source = "buildings",
        source_layer = "buildings",
        line_color = "#333333",
        line_width = 0.5
      ) |>
      add_navigation_control()
  })

  # Update fill color when input changes
  observeEvent(input$color, {
    maplibre_proxy("map") |>
      set_paint_property("buildings-fill", "fill-color", input$color)
  }, ignoreInit = TRUE)

  # Update fill opacity when input changes
  observeEvent(input$opacity, {
    maplibre_proxy("map") |>
      set_paint_property("buildings-fill", "fill-opacity", input$opacity)
  }, ignoreInit = TRUE)
}

# ============================================================================
# Run App
# ============================================================================

shinyApp(ui = ui, server = server)
