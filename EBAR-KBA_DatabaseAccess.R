# Packages
library(httr)
library(tidyverse)
library(geojsonsf)
library(sf)
library(magrittr)

# BEWARE character encoding! #2

# Input parameters
username <- "kbapipeline"
password
address <- "KBA_View/FeatureServer/0"
spatial <- F

# Get token
response <- httr::POST("https://gis.natureserve.ca/portal/sharing/rest/generateToken",
                       body = list(username=username, password=password, referer=":6443/arcgis/admin",f="json"),
                       encode = "form")
token <- content(response)$token

# Get GeoJSON
url <- parse_url("https://gis.natureserve.ca/arcgis/rest/services")
url$path <- paste(url$path, paste0("EBAR-KBA/", address, "/query"), sep = "/")
url$query <- list(where = "OBJECTID >= 0",
                  outFields = "*",
                  returnGeometry = "true",
                  f = "geojson")
request <- build_url(url)
response <- VERB(verb = "GET",
                 url = request,
                 add_headers(`Authorization` = paste("Bearer ", token)))
data <- content(response, as="text") %>%
  geojson_sf()

# If non-spatial, drop geometry
if(!spatial){
  data %<>% st_drop_geometry()
}
