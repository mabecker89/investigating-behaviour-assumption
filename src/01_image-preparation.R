#-----------------------------------------------------------------------------------------------------------------------

# Title:  Image preparation
# Date:   July 2022
# Author: Marcus Becker

# Description: Connect to the WildTrax database and create a dataframe of downloadable links to images of various
#              species. Sample from these images, download them, and store them for supplemental tagging (in Timelapse).

#-----------------------------------------------------------------------------------------------------------------------

# Attach required packages:
library(RPostgreSQL) # WildTrax is a PostgreSQL database
library(DBI)
library(glue) # Compose SQL query
library(keyring) # Store credentials
library(readr)
library(dplyr)
library(tidyr)
library(purrr)
library(lubridate)

# Set path to Google Drive
## root <- "G:/Shared drives/ABMI Camera Mammals/"

#-----------------------------------------------------------------------------------------------------------------------

# Define functions:

# Establish connection to WildTrax database


wt_conn <- function(username, password) {

  conn <- DBI::dbConnect(drv = DBI::dbDriver("PostgreSQL"),
                         dbname = "wildtrax",
                         host = "prod.wildtrax.ca",
                         port = "5432",
                         user = username,
                         password = password)

  return(conn)

}

# Create dataframe of all images of a certain species in a project

wt_get_links <- function(project, common_name) {

  # Generate SQL query
  query <- glue::glue_sql(
    "SELECT project_full_nm,
            location_name,
            species_common_name,
            image_date,
            'https://' || storage_cam_name || '.s3.amazonaws.com/' || deployment_uuid || '/large/' || image_id || '.' || image_extension
     FROM camera.project_user_deployment
     JOIN common.project on project_id = pud_project_id
     JOIN camera.deployment on deployment_id = pud_deployment_id
     JOIN camera.species_tag on st_project_id = pud_project_id and st_user_id = pud_user_id and st_deployment_id = pud_deployment_Id  and st_is_active
     JOIN common.location  on location_id = deployment_location_id
     JOIN camera.image on image_date = st_image_date and image_deployment_id = st_deployment_id
     JOIN common.lu_storage on image_storage_id = storage_id
     JOIN camera.species on species_id = st_species_id
     WHERE 1=1
     AND project_full_nm IN ({project})
     AND species_common_name = {common_name}
     ORDER BY location_name;",
    project = project, common_name = common_name, .con = wt, # Connection has to be named 'wt'
  )

  # Send query to WildTrax
  send_query <- DBI::dbSendQuery(conn = wt, statement = query)

  # Fetch results
  x <- DBI::dbFetch(send_query) |>
    dplyr::rename(link = `?column?`)

  # Clear query
  DBI::dbClearResult(send_query)

  return(x)

}

#-----------------------------------------------------------------------------------------------------------------------

# Connect to the WT database
wt <- wt_conn(username = key_get("db_username", keyring = "wildtrax"),
              password = key_get("db_password", keyring = "wildtrax"))

# Projects - leaving out 2015 for now
project <- c("ABMI Ecosystem Health 2016",
             "ABMI Ecosystem Health 2017",
             "ABMI Ecosystem Health 2018",
             "ABMI Ecosystem Health 2019",
             "ABMI Ecosystem Health 2020",
             "ABMI Ecosystem Health 2021")

# Species - we'll start with these 4
sp <- c("Moose", "White-tailed Deer", "Black Bear", "Woodland Caribou")

# Inputs
inputs <- data.frame(crossing(project, common_name = sp))

# Get links as dataframe - this takes a while to run
df_links <- inputs |>
  mutate(results = pmap(
    list(
      project = project,
      common_name = common_name
    ),
    .f = wt_get_links
  )) |>
  # Check if any project/species combos came up empty, and remove if so
  mutate(n_obs = map(.x = results, .f = ~ nrow(.))) |>
  filter(n_obs > 0) |>
  # Unnest
  unnest(cols = c(results)) |>
  # Correct date time column - this is a quirk of pulling data from the database into R
  mutate(date_detected = ymd_hms(image_date)) |>
  select(project, common_name, location = location_name, date_detected, link)

# Save results
write_csv(df_links, paste0(root, "data/supplemental/assumptions-tests/investigation-behaviour-by-veghf/data/image_links_from_wt.csv"))

#-----------------------------------------------------------------------------------------------------------------------

# Now we need to sample at the series level, being mindful of what was done in the past.

# Images by series (processed beforehand in main ABMI scripts)
df_images_by_series <- read_csv(
  paste0(root, "data/processed/series-summary/abmi-cmu_all-years_images-by-series_2022-07-19.csv")) |>
  # We're only going to sample from EH projects
  filter(str_detect(project, "^ABMI Ecosystem Health"))

# Previously tagged series - this is what was done for the methods paper ~2 years ago
prev_sample <- read_csv(
  paste0(root, "data/supplemental/assumptions-tests/investigation-behaviour-by-veghf/samples/all-series-sampled_2022-07-19.csv")) |>
  # We're going to focus on upping the sample size for the four species listed above
  filter(common_name %in% sp,
         !str_detect(location, "^OG|-CL$"))

# Join to our links df
df_links_series <- df_links |>
  # Join - some NAs here. I think it's timelapse/out of range photos that weren't previously processed.
  left_join(df_images_by_series, by = c("project", "location", "date_detected", "common_name")) |>
  filter(!is.na(series_num)) |>
  distinct() |>
  left_join(prev_sample, by = c("location", "common_name", "date_detected")) |>
  group_by(series_num) |>
  filter(all(is.na(n_images))) |>
  ungroup() |>
  arrange(project, location, date_detected, common_name) |>
  select(-n_images)

# Now, let's take a sample.

set.seed(12345) # For reproducibility - we want to be able to take the same sample if we run the script again.

sample <- df_links_series |>
  group_by(common_name, series_num) |>
  nest() |>
  ungroup() |>
  # Differing fractions per species - starting with a reasonable (?) amount.
  mutate(fraction = case_when(
    common_name == "Black Bear" ~ 0.02,
    common_name == "Moose" ~ 0.02,
    common_name == "White-tailed Deer" ~ 0.01,
    common_name == "Woodland Caribou" ~ 0.5
  )) |>
  group_by(common_name) |>
  sample_frac(fraction[1]) |>
  ungroup() |>
  unnest(cols = data) |>
  group_by(series_num) |>
  mutate(image_number = row_number()) |>
  ungroup() |>
  select(-fraction)

# Check number of series to sample - seems reasonable.
check <- sample |>
  group_by(common_name) |>
  tally()

# Now let's download these images

folder <- "G:/Shared drives/ABMI Camera Mammals/data/base/sample-images/investigation-behaviour/"

for (i in 1:nrow(sample)) {

  myurl <- paste(sample[i,6], sep = "")
  filename <- paste(sample[i,1], sample[i,2], sample[i,10], sep = "_")
  print(filename)
  z <- tempfile()
  download.file(myurl, z, mode = "wb")
  pic <- jpeg::readJPEG(z)
  jpeg::writeJPEG(pic, target = paste(folder, filename, ".jpg", sep = ""))
  file.remove(z)

}

#-----------------------------------------------------------------------------------------------------------------------
