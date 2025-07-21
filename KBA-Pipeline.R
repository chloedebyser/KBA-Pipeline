#### KBA Canada Pipeline
#### Wildlife Conservation Society Canada and Birds Canada
#### 2023

#### INSTRUCTIONS #################################################################################
# This code should never be edited directly on the server.                                        #
# Instead, please edit the code locally and push your edits to the GitHub repository.             #
###################################################################################################

#### Workspace ####
# Packages
library(httr)
library(tidyverse)
library(geojsonsf)
library(sf)
library(magrittr)
library(devtools)
library(stringi)
library(RPostgres)
library(DBI)
library(mailR)
library(jsonlite)
library(openxlsx)

# Functions
      # Key KBA functions
source_url("https://github.com/chloedebyser/KBA-Public/blob/main/KBA%20Functions.R?raw=TRUE")

      # Pipeline functions
source("functions.R")

# Date of last pipeline run
lastPipelineRun <- readRDS("lastPipelineRun.RDS")

# Environment variables 
env_vars <- c("kbapipeline_pswd", "postgres_user", "postgres_pass", "database_name", "database_host", "mailtrap_pass", "database_port", "geoserver_pass", "docker_env")

for(env in env_vars){
  
  # Get variable
  var <- Sys.getenv(toupper(env))
  
  # Assign variable
  assign(env, var)
}
rm(env, env_vars, var)

# Variable to inform if error occurs in the data prep phase 
initError<- FALSE
tryCatch({
  
# Coordinate reference system
crs <- readRDS("crs.RDS")

# COSEWIC Links
COSEWICLinks <- read.xlsx("COSEWIC_Links.xlsx")

# KBA-EBAR database information
databaseAttempt <- 1

while(databaseAttempt <= 5){
  
tryCatch({
Sys.sleep(20)
read_KBAEBARDatabase(datasetNames=c("KBASite", "SpeciesAtSite", "Species", 'BIOTICS_ELEMENT_NATIONAL', "SpeciesAssessment", "PopSizeCitation", "EcosystemAtSite", "Ecosystem", "BIOTICS_ECOSYSTEM", "EcosystemAssessment", "ExtentCitation", "KBACitation", "KBAThreat", "KBAAction", "KBALandCover", "KBAProtectedArea", "OriginalDelineation", "BiodivElementDistribution", "KBACustomPolygon", "KBAInputPolygon"),
                     type="include",
                     account="kbapipeline",
                     epsg=4326) %>%
  suppressWarnings()
break
},

error=function(cond){databaseAttempt <<- databaseAttempt + 1})
}
 
if(databaseAttempt <= 5){
  message(paste("Reading KBA-EBAR Database succeeded after", databaseAttempt, "attempt(s)."))
  
}else{
  stop("Reading KBA-EBAR Database unsuccessful after 5 attempts.")
}

# KBA Registry database information
      # Registry database connection
registryDB <- dbConnect(
  Postgres(), 
  user = postgres_user,
  password = postgres_pass,
  dbname = database_name,
  host = database_host,
  port = database_port
)

      # Lookup tables
lookupTables <- c("KBA_Province", "KBA_Level", "Threats", "Conservation", "System", "Habitat", "AssessmentParameter", "COSEWICStatus", "IUCNStatus", "Criterion", "Subcriterion", "Ecosystem_Category")

for(lookupTable in lookupTables){
  
  # Get data
  data <- registryDB %>% tbl(lookupTable) %>% collect()
  
  # Assign data
  assign(paste0("REG_", lookupTable), data)
  rm(data)
}
rm(lookupTable, lookupTables)

      # Data tables
dataTables <- list(c("KBA_Site", T), c("KBA_Website", F), c("KBA_Citation", F), c("KBA_Conservation", F), c("KBA_Threats", F), c("KBA_System", F), c("KBA_Habitat", F), c("KBA_ProtectedArea", F), c("Species", F), c("Species_Citation", F), c("KBA_SpeciesAssessments", F), c("Ecosystem", F), c("Ecosystem_Citation", F), c("KBA_EcosystemAssessments", F), c("SpeciesAssessment_Subcriterion", F), c("EcosystemAssessment_Subcriterion", F), c("Footnote", F), c("InternalBoundary", T), c("Species_Link", F),c("BackupDate",F))

for(i in 1:length(dataTables)){
  
  # Get data
        # If spatial
  if(dataTables[[i]][2]){
    data <- registryDB %>% read_sf(dataTables[[i]][1])
    
        # If non-spatial
  }else{
    data <- registryDB %>% tbl(dataTables[[i]][1]) %>% collect()
  }
  
  # Assign data
  assign(paste0("REG_", dataTables[[i]][1]), data)
  rm(data)
}
rm(i)

# Check if backup date is greater than last pipeline run
backupdate <- REG_BackupDate %>% filter(datetime==max(datetime)) %>% pull(datetime) %>% force_tz(.,tzone="Canada/Eastern")

# Force error if backup has not occurred since the last pipeline run
if(backupdate < lastPipelineRun){
  stop("There has not been a backup since the last pipeline run. Please check if backups are still working.")
}

#### SPECIES - Update all species ####
# Read in Bird-specific data
Bird_Species <- fromJSON("https://kba-maps.deanrobertevans.ca/api/species") %>%
  select(nselementcode,birdalphacode,bcspeciesid,nationaltrend,nationaltrendreference) %>% 
  rename(NSElementCode=nselementcode,
         BirdAlphaCode=birdalphacode,
         BCSpeciesID=bcspeciesid,
         NationalTrend=nationaltrend,
         CitationNationalTrend=nationaltrendreference) %>%
  mutate(NationalTrend_FR = case_when(NationalTrend == "Increasing" ~ "En augmentation",
                                      NationalTrend == "Decreasing"  ~ "En diminution",
                                      NationalTrend == "Stable" ~ "Stable",
                                      NationalTrend == "Unknown" ~ "Inconnue",
                                      .default = NA))

# Create initial dataframe
REGA_Species <- DB_BIOTICS_ELEMENT_NATIONAL %>%
  left_join(., DB_Species, by="speciesid") %>%
  rename(Order = tax_order,
         ScientificName = national_scientific_name,
         InformalTaxonomicGroup = kba_group,
         CommonName_EN = national_engl_name,
         CommonName_FR = national_fr_name,
         Kingdom = kingdom,
         Phylum = phylum,
         Class = class,
         Family = family,
         WDKBASpecRecID = wdkbaid,
         IUCNTaxonID = iucn_internaltaxonid,
         IUCNAssessmentID = iucn_assessmentid,
         IUCNAssessmentDate = iucn_assessmentdate,
         IUCNStatusCriteria = iucn_criteria,
         IUCNPopulationTrend = iucn_populationtrend,
         NSElementCode = element_code,
         Systems = major_habitat,
         NSGlobalUniqueID = global_unique_identifier,
         NSElementGlobalID = element_global_id,
         Endemism = n_endemism_desc,
         COSEWICTaxonID = cosewic_id,
         COSEWICAssessmentDate = cosewic_date,
         COSEWICStatusCriteria = cosewic_assess_criteria,
         SARAStatus = sara_status,
         SARAAssessmentDate = sara_status_date,
         NSGRank = g_rank,
         NSGRankReviewDate = g_rank_review_date,
         NSNRank = n_rank,
         NSNRankReviewDate = n_rank_review_date,
         NSLink = nsx_url) %>%
  left_join(., Bird_Species, by="NSElementCode") %>%
  mutate(NationalName_EN = CommonName_EN,
         NationalName_FR = CommonName_FR,
         Subspecies_EN = NA,
         Subspecies_FR = NA,
         Population_EN = NA,
         Population_FR = NA,
         IUCNLink = ifelse(!is.na(IUCNTaxonID), paste0("https://www.iucnredlist.org/species/", IUCNTaxonID,"/", IUCNAssessmentID), NA),
         Sensitive = 0,
         TaxonomicLevel = case_when(bcd_style_n_rank == "NSYN" ~ "None",
                                    inactive_ind == "Y" ~ "None",
                                    .default = as.character(ca_nname_level)),
         Endemism = ifelse(endemism == "Yes (the element is endemic)",
                           "Y",
                           ifelse(endemism == "No (the element is not endemic)",
                                  "N",
                                  ifelse(endemism == "Breeding (endemic as a breeder only)",
                                         "B",
                                         ifelse(endemism == "Probable",
                                                "P",
                                                endemism)))),
         iucn_cd = ifelse(is.na(iucn_cd), "NE", iucn_cd),
         cosewic_status = ifelse(is.na(cosewic_status) | (cosewic_status == "Non-active/Nonactive"), "NA", cosewic_status),
         IUCNPopulationTrend_FR = case_when(IUCNPopulationTrend == "Increasing" ~ "En augmentation",
                                            IUCNPopulationTrend == "Decreasing"  ~ "En diminution",
                                            IUCNPopulationTrend == "Stable" ~ "Stable",
                                            IUCNPopulationTrend == "Unknown" ~ "Inconnue",
                                            .default = NA),
         NSGRank_Precautionary = ifelse(is.na(precautionary_g_rank), ifelse(ca_nname_level == "Species", "GNR", "TNR"), precautionary_g_rank),
         InformalTaxonomicGroup_FR = case_when(InformalTaxonomicGroup == "Amphibians and Reptiles" ~ "Amphibiens et reptiles",
                                               InformalTaxonomicGroup == "Birds" ~ "Oiseaux",
                                               InformalTaxonomicGroup == "Fishes" ~ "Poissons",
                                               InformalTaxonomicGroup == "Fungi and Lichens" ~ "Champignons et lichens",
                                               InformalTaxonomicGroup == "Invertebrates" ~ "Invertébrés",
                                               InformalTaxonomicGroup == "Mammals" ~ "Mammifères",
                                               InformalTaxonomicGroup == "Nonvascular Plants" ~ "Plantes non vasculaires",
                                               InformalTaxonomicGroup == "Vascular Plants" ~ "Plantes vasculaires",
                                               InformalTaxonomicGroup == "Sensitive Species" ~ "Espèces sensibles",
                                               .default = NA)) %>%
  rowwise() %>%
  mutate(NSNRank_Precautionary = ifelse(is.na(precautionary_n_rank), ifelse(is.na(NSNRank), "NNR", c(precautionary_n_rank_breeding, precautionary_n_rank_nonbreed, precautionary_n_rank_migrant)[order(match(c(precautionary_n_rank_breeding, precautionary_n_rank_nonbreed, precautionary_n_rank_migrant), c("NU", "NNR", "N1", "N2", "N3", "N4", "N5", "NH", "NX", "NNA")))][1]), precautionary_n_rank)) %>%
  ungroup() %>%
  left_join(., REG_IUCNStatus, by=c("iucn_cd" = "Nomenclature")) %>%
  left_join(., REG_COSEWICStatus, by=c("cosewic_status" = "Nomenclature")) %>%
  left_join(., COSEWICLinks[,c("ELEMENT_NATIONAL_ID", "Link")], by=c("element_national_id" = "ELEMENT_NATIONAL_ID")) %>%
  rename(COSEWICLink = Link)

# Add range information
      # Prepare range information
speciesRange <- DB_BIOTICS_ELEMENT_NATIONAL %>%
  select(speciesid, current_distribution) %>%
  filter(!is.na(current_distribution)) %>%
  separate_rows(., current_distribution, sep=", ") %>%
  mutate(current_distribution = ifelse(current_distribution %in% c("LB", "NF"), "NL", current_distribution)) %>%
  left_join(., REG_KBA_Province, by=c("current_distribution" = "Abbreviation")) %>%
  distinct() %>%
  group_by(speciesid)

      # Compute Range field
range <- speciesRange %>%
  arrange(speciesid, current_distribution) %>%
  summarise(Range = paste(current_distribution, collapse = ", "))

      # Compute Range_EN field
range_en <- speciesRange %>%
  arrange(speciesid, Province_EN) %>%
  summarise(Range_EN = paste(Province_EN, collapse = ", "))

      # Compute Range_FR field
range_fr <- speciesRange %>%
  arrange(speciesid, Province_FR) %>%
  summarise(Range_FR = paste(Province_FR, collapse = ", "))

      # Add to dataframe
REGA_Species %<>%
  left_join(., range, by="speciesid") %>%
  left_join(., range_en, by="speciesid") %>%
  left_join(., range_fr, by="speciesid")
rm(speciesRange, range, range_en, range_fr)

# Add reference population size
      # Only retain assessments based on number of mature individuals and with a best estimate
relevantReferenceEstimates_spp <- DB_SpeciesAssessment %>%
  left_join(., DB_SpeciesAtSite[,c("speciesatsiteid", "speciesid")], by="speciesatsiteid") %>%
  filter(assessmentparameter == "(i) number of mature individuals") %>% # Only retain the record if the assessment parameter = number of mature individuals
  filter(!is.na(referenceestimate_best)) # Only retain record if there is a best reference estimate

      # For birds assessed under nD1, the reference population corresponds to the continental population
relevantReferenceEstimates_spp %<>%
  left_join(., DB_BIOTICS_ELEMENT_NATIONAL[,c("speciesid", "kba_group")], by="speciesid") %>%
  mutate(kbalevel = replace(kbalevel, (kba_group == "Birds") & str_detect(criteriamet, "nD1"), "Continental"))

      # Get most recent estimate per species and per level
relevantReferenceEstimates_spp %<>%
  group_by(speciesid, kbalevel) %>%
  arrange(speciesid, kbalevel, desc(dateassessed)) %>%
  filter(row_number()==1)

      # Format
relevantReferenceEstimates_spp %<>%
  select(speciesid, kbalevel, referenceestimate_best, referenceestimate_sources) %>%
  rename(PopulationSize = referenceestimate_best,
         Citation = referenceestimate_sources) %>%
  pivot_wider(names_from = kbalevel, values_from = c(PopulationSize, Citation), names_glue = "{kbalevel}{.value}") %>%
  mutate(GlobalCitation = ifelse('GlobalCitation' %in% names(.), GlobalCitation, NA),
         ContinentalCitation = ifelse('ContinentalCitation' %in% names(.), ContinentalCitation, NA),
         NationalCitation = ifelse('NationalCitation' %in% names(.), NationalCitation, NA),
         GlobalPopulationSize = ifelse('GlobalPopulationSize' %in% names(.), GlobalPopulationSize, NA),
         ContinentalPopulationSize = ifelse('ContinentalPopulationSize' %in% names(.), ContinentalPopulationSize, NA),
         NationalPopulationSize = ifelse('NationalPopulationSize' %in% names(.), NationalPopulationSize, NA)) %>%
  rename(CitationGlobalPopulation = GlobalCitation,
         CitationContinentalPopulation = ContinentalCitation,
         CitationNationalPopulation = NationalCitation)

      # Add to dataframe
REGA_Species %<>% left_join(., relevantReferenceEstimates_spp, by="speciesid")

# Remove records with no NatureServe Element Code
REGA_Species %<>%
  filter(!is.na(NSElementCode))

# Assign SpeciesID from the Registry
REGA_Species %<>%
  rename(DB_SpeciesID = speciesid) %>%
  left_join(., REG_Species[,c("SpeciesID", "NSElementCode")], by="NSElementCode")

# Create crosswalk from KBA-EBAR SpeciesID to Registry SpeciesID
crosswalk_SpeciesID <- REGA_Species %>%
  rename(REG_SpeciesID = SpeciesID) %>%
  select(DB_SpeciesID, REG_SpeciesID, NSElementCode)

# Select final columns
REGA_Species %<>%
  select(all_of(colnames(REG_Species)))

# Update species names
REGA_Species %<>% updateSpeciesNames(.)

# Only retain species that are in the Registry database
REGU_Species <- REGA_Species %>%
  filter(NSElementCode %in% REG_Species$NSElementCode)

# Update all species that are currently on the Registry (excluding sensitive species)
registryDB %>% update.table("Species", "SpeciesID", REGU_Species, REG_Species)

#### ECOSYSTEMS - Update all ecosystems ####
REGA_Ecosystem <- DB_BIOTICS_ECOSYSTEM %>%
  left_join(., DB_Ecosystem, by="ecosystemid") %>%
  rename(EcosystemClassificationSystem = ecosystemclassificationsystem,
         EcosystemLevelJustification = ecosystemleveljustification,
         NSElementGlobalID = element_global_id,
         NSGlobalUniqueID = global_unique_identifier,
         NSLink = nsx_url,
         NSElementCode_IVC = ivc_elcode,
         NSElementCode_CNVC = cnvc_elcode,
         ScientificName_EN = ivc_scientific_name,
         ScientificName_EN_HTML = ivc_formatted_scientific_name,
         ScientificName_FR = ivc_scientific_name_fr,
         ScientificName_FR_HTML = ivc_formatted_scientific_name_fr,
         EcosystemType_EN = cnvc_english_name,
         EcosystemLevel = classification_level_name,
         Biome_EN = biome_name,
         Biome_FR = biome_name_fr,
         Subbiome_EN = subbiome_name,
         Subbiome_FR = subbiome_name_fr,
         Formation_EN = formation_name,
         Formation_FR = formation_name_fr,
         Division_EN = division_name,
         Division_FR = division_name_fr,
         Macrogroup_EN = cnvc_mg_englishname,
         Group_EN = cnvc_group_englishname,
         NSGRank = g_rank,
         NSGRankReviewDate = g_rank_review_date,
         NSNRank = n_rank,
         NSNRankReviewDate = n_rank_review_date,
         NSConceptSentence_EN = g_concept_sentence,
         NSConceptSentence_FR = g_concept_sentence_fr,
         WDKBAEcoRecID = wdkbaid,
         IUCNAssessmentDate = iucn_assessmentdate,
         IUCNStatusCriteria = iucn_criteria) %>%
  mutate(EcosystemClassificationSystem_FR = case_when(EcosystemClassificationSystem == "International Vegetation Classification Hierarchy" ~ "Hiérarchie de la classification internationale de la végétation",
                                                      EcosystemClassificationSystem == "Canadian National Vegetation Classification" ~ "Classification nationale de la végétation du Canada",
                                                      .default = NA),
         EcosystemType_FR = ifelse(is.na(cnvc_french_name), ivc_name_fr, cnvc_french_name),
         Macrogroup_FR = ifelse(is.na(cnvc_mg_frenchname), ivc_mg_name_fr, cnvc_mg_frenchname),
         Group_FR = ifelse(is.na(cnvc_group_frenchname), ivc_group_name_fr, cnvc_group_frenchname),
         IUCNLink = ifelse(ecosystemid == 113, "https://assessments.iucnrle.org/assessments/12", NA),
         iucn_cd = ifelse(is.na(iucn_cd), "NE", iucn_cd),
         EcosystemLevel_FR = case_when(EcosystemLevel == "Group" ~ "Groupe",
                                       .default = EcosystemLevel),
         NSGRank_Precautionary = ifelse(is.na(precautionary_g_rank), "GNR", precautionary_g_rank),
         NSNRank_Precautionary = ifelse(is.na(precautionary_n_rank), "NNR", precautionary_n_rank)) %>%
  left_join(., REG_IUCNStatus, by=c("iucn_cd" = "Nomenclature")) %>%
  left_join(., REG_Ecosystem_Category[,c("EcosystemCategoryID", "CategoryName_EN")], by=c("kba_group" = "CategoryName_EN"))

# Add range information
      # Prepare range information
ecosystemRange <- DB_BIOTICS_ECOSYSTEM %>%
  select(ecosystemid, ca_subnations) %>%
  filter(!is.na(ca_subnations)) %>%
  separate_rows(., ca_subnations, sep=", ") %>%
  mutate(question = ifelse(grepl("?", ca_subnations, fixed=T), "?", ""),
         ca_subnations = gsub("?", "", ca_subnations, fixed=T),
         ca_subnations = ifelse(ca_subnations %in% c("LB", "NF"), "NL", ca_subnations)) %>%
  left_join(., REG_KBA_Province, by=c("ca_subnations" = "Abbreviation")) %>%
  distinct() %>%
  group_by(ecosystemid)

      # Compute Range field
range <- ecosystemRange %>%
  arrange(ecosystemid, ca_subnations) %>%
  summarise(Range = paste(paste0(ca_subnations, question), collapse = ", "))

      # Compute Range_EN field
range_en <- ecosystemRange %>%
  arrange(ecosystemid, Province_EN) %>%
  summarise(Range_EN = paste(paste0(Province_EN, question), collapse = ", "))

      # Compute Range_FR field
range_fr <- ecosystemRange %>%
  arrange(ecosystemid, Province_FR) %>%
  summarise(Range_FR = paste(paste0(Province_FR, question), collapse = ", "))

      # Add to dataframe
REGA_Ecosystem %<>%
  left_join(., range, by="ecosystemid") %>%
  left_join(., range_en, by="ecosystemid") %>%
  left_join(., range_fr, by="ecosystemid")
rm(ecosystemRange, range, range_en, range_fr)

# Add reference population size
      # Get estimates that are relevant to the KBA Registry (i.e. most recent global/national estimate, across all EcosystemAssessment records in the KBA-EBAR database)
relevantReferenceEstimates_eco <- DB_EcosystemAssessment %>%
  left_join(., DB_EcosystemAtSite[,c("ecosystematsiteid", "ecosystemid")], by="ecosystematsiteid") %>%
  filter(!is.na(referenceestimate_best)) %>% # Only retain record if there is a best reference estimate
  group_by(ecosystemid, kbalevel) %>%
  arrange(ecosystemid, kbalevel, desc(dateassessed)) %>%
  filter(row_number()==1) # Only retain the most recent EcosystemAssessment per ecosystem and per KBALevel

      # Format
relevantReferenceEstimates_eco %<>%
  select(ecosystemid, kbalevel, referenceestimate_best, referenceestimate_sources) %>%
  rename(Extent = referenceestimate_best,
         Citation = referenceestimate_sources) %>%
  pivot_wider(names_from = kbalevel, values_from = c(Extent, Citation), names_glue = "{kbalevel}{.value}") %>%
  mutate(GlobalCitation = ifelse('GlobalCitation' %in% names(.), GlobalCitation, NA),
         NationalCitation = ifelse('NationalCitation' %in% names(.), NationalCitation, NA),
         GlobalExtent = ifelse('GlobalExtent' %in% names(.), GlobalExtent, NA),
         NationalExtent = ifelse('NationalExtent' %in% names(.), NationalExtent, NA)) %>%
  rename(CitationGlobalExtent = GlobalCitation,
         CitationNationalExtent = NationalCitation)

      # Add to dataframe
REGA_Ecosystem %<>% left_join(., relevantReferenceEstimates_eco, by="ecosystemid")

# Remove records with no NatureServe Element Code
REGA_Ecosystem %<>%
  filter(!is.na(NSElementCode_IVC))

# Assign EcosystemID from the Registry
REGA_Ecosystem %<>%
  rename(DB_EcosystemID = ecosystemid) %>%
  left_join(., REG_Ecosystem[,c("EcosystemID", "NSElementCode_IVC")], by="NSElementCode_IVC")

# Create crosswalk from KBA-EBAR EcosystemID to Registry EcosystemID
crosswalk_EcosystemID <- REGA_Ecosystem %>%
  rename(REG_EcosystemID = EcosystemID) %>%
  select(DB_EcosystemID, REG_EcosystemID, NSElementCode_IVC)

# Select final columns
REGA_Ecosystem %<>%
  select(all_of(colnames(REG_Ecosystem)))

# Only retain ecosystems that are in the Registry database
REGU_Ecosystem <- REGA_Ecosystem %>%
  filter(NSElementCode_IVC %in% REG_Ecosystem$NSElementCode_IVC)

# Update all ecosystems that are currently on the Registry
registryDB %>% update.table("Ecosystem", "EcosystemID", REGU_Ecosystem, REG_Ecosystem)

},error=function(e){
  initError<<- TRUE # error happened
  
  # Send email about error to Dean and Chloé
  pipeline.email(to=c("devans@birdscanada.org","cdebyser@wcs.org"),
                password = mailtrap_pass,
                message = paste0("The following error occured during the data prep phase of the KBA Pipeline: ",
                                 e[["message"]]))
})

# Only proceed if there was no initial errors
if(!initError){

#### SITES - Add & update sites in need of publishing, including related records ####
# Initialize sensitive species information
sensitiveSpecies <- data.frame(speciesatsiteid = integer(),
                               speciesid = integer(),
                               display_alternativename = character(),
                               display_taxonomicgroup = character(),
                               display_assessmentinfo = character(),
                               display_biodivelementdist = character(),
                               newid = integer(),
                               kba_group = character(),
                               display_alternativegroup = character())

# Initialize site notification information
siteNotifications <- data.frame(sitecode = character(),
                                sitename = character(),
                                jurisdiction = character(),
                                type = character(),
                                leademail = character())

# Initialize error information
siteErrors <- data.frame(site=character(),
                         sitecode=character(),
                         error=character())

# Variable to record if transaction is occuring
transaction <- FALSE

# Site processing
for(id in DB_KBASite %>% arrange(nationalname) %>% pull(kbasiteid)){
  
  tryCatch({

  ### Only process sites that are ready for and in need of publishing ###
  # Only process sites with status = "Publication of National Site" or beyond
  accepted <- DB_KBASite %>%
    filter(kbasiteid == id) %>%
    pull(sitestatus) %>%
    {ifelse(is.na(.), F, ifelse(. %in% 6:8, T, F))}
  
  obsolete <- DB_KBASite %>%
    filter(kbasiteid == id) %>%
    pull(sitestatus) %>%
    {ifelse(is.na(.), F, ifelse(. %in% 9:10, T, F))}
  
  if(!(accepted | obsolete)){next}
  
  # Filter KBA-EBAR data
  filter_KBAEBARDatabase(KBASiteIDs = id, RMUnfilteredDatasets = F)
  
  # Do not process site if there are multiple accepted versions of the same site
  acceptedVersions <- DB_KBASite %>%
    filter(sitecode == DBS_KBASite$sitecode, sitestatus %in% 6:8) %>%
    filter(!year(confirmdate) == 1900) # Only retain confirmed sites
  
  if(nrow(acceptedVersions) > 1){
    stop("There are multiple accepted versions of this site.")
  }
  
  # If obsolete, check whether the site is a superseded version
  if(obsolete){
    
    # If the site is a superseded version, do not process
    if((DBS_KBASite$sitestatus == 9) && (nrow(acceptedVersions) > 0) && (acceptedVersions$version > DBS_KBASite$version)){
      next
    
    # Otherwise, get obsolete reason
    }else{
      
      if(docker_env == "Production"){
        
        # Get all accepted sites other than the focal site
        otherAcceptedSites <- DB_KBASite %>%
          filter(!kbasiteid == id, sitestatus %in% 6:8) %>%
          filter(!year(confirmdate) == 1900) %>% # Only retain confirmed sites
          st_make_valid()
        
        # Get accepted sites that intersect the focal site
        replacementSite <- otherAcceptedSites[unlist(st_intersects(DBS_KBASite, otherAcceptedSites)[[1]]),]
        
        # Check that there is exactly one accepted site intersecting the focal site
        if(!nrow(replacementSite) == 1){
          stop("Site is obsolete and replacement site was not found.")
        }
        
        # Compute obsolete reason
        ObsoleteReason_EN <- paste0(DBS_KBASite$nationalname,
                                    " KBA (",
                                    DBS_KBASite$sitecode,
                                    ") has been incorporated into another KBA (",
                                    replacementSite$nationalname,
                                    " - ",
                                    replacementSite$sitecode,
                                    ") and is no longer a standalone KBA. Click <a href='https://kbacanada.org/site/?SiteCode=",
                                    replacementSite$sitecode,
                                    "'>here</a>",
                                    " for more information about the ",
                                    replacementSite$nationalname,
                                    " KBA.")
        
        ObsoleteReason_FR <- paste0("La KBA de ",
                                    ifelse(is.na(DBS_KBASite$nationalname_fr), DBS_KBASite$nationalname, DBS_KBASite$nationalname_fr),
                                    " (",
                                    DBS_KBASite$sitecode,
                                    ") n'est plus une KBA indépendante et a été incorporée au sein d'une autre KBA (",
                                    ifelse(is.na(replacementSite$nationalname_fr), replacementSite$nationalname, replacementSite$nationalname_fr),
                                    " - ",
                                    replacementSite$sitecode,
                                    "). Davantage d'informations au sujet de la KBA de ",
                                    ifelse(is.na(replacementSite$nationalname_fr), replacementSite$nationalname, replacementSite$nationalname_fr),
                                    " sont disponibles <a href='https://kbacanada.org/fr/site/?SiteCode=",
                                    replacementSite$sitecode,
                                    "'>ici</a>",
                                    ".")
        
      }else{
        ObsoleteReason_EN <- NA
        ObsoleteReason_FR <- NA
      }
    }
    
  }else{
    ObsoleteReason_EN <- NA
    ObsoleteReason_FR <- NA
  }
  
  # Check for unconfirmed edits
        # Compute overall_last_edited_date (date of last edit of the site or any of its related records)
  overall_last_edited_date <- c(DBS_KBASite$last_edited_date,
                                DBS_SpeciesAtSite$last_edited_date,
                                DBS_SpeciesAssessment$last_edited_date,
                                DBS_PopSizeCitation$last_edited_date,
                                DBS_EcosystemAtSite$last_edited_date,
                                DBS_EcosystemAssessment$last_edited_date,
                                DBS_ExtentCitation$last_edited_date,
                                DBS_KBACitation$last_edited_date,
                                DBS_KBAThreat$last_edited_date,
                                DBS_KBAAction$last_edited_date,
                                DBS_KBALandCover$last_edited_date,
                                DBS_OriginalDelineation$last_edited_date,
                                DBS_BiodivElementDistribution$last_edited_date,
                                DBS_KBACustomPolygon$last_edited_date,
                                DBS_KBAInputPolygon$last_edited_date)
  overall_last_edited_date <- max(overall_last_edited_date)
  
        # Check for additions and deletions
  addition_deletion <- ifelse((!nrow(DBS_SpeciesAtSite) == DBS_KBASite$n_speciesatsite) | (!nrow(DBS_EcosystemAtSite) == DBS_KBASite$n_ecosystematsite) | (!nrow(DBS_OriginalDelineation) == DBS_KBASite$n_originaldelineation) | (!nrow(DBS_BiodivElementDistribution) == DBS_KBASite$n_biodivelementdistribution) | (!nrow(DBS_KBAInputPolygon) == DBS_KBASite$n_kbainputpolygon) | (!nrow(DBS_KBACustomPolygon) == DBS_KBASite$n_kbacustompolygon), T, F)
  
  # Check whether site should be processed in the current pipeline run
  processSite <- (overall_last_edited_date <= DBS_KBASite$confirmdate) & (!addition_deletion) & (overall_last_edited_date >= lastPipelineRun)
  
  # TEMP - Stop 5 bird sites from being processed in the production environment until proposal forms are ready (TO DO: Remove once proposal forms are ready)
  if(docker_env=="Production"){
    
    if(DBS_KBASite$sitecode %in% c("AB002", "BC017", "NT002", "NU007")){
      processSite <- F
    }
  }
  
  # Only proceed with the rest of the loop if the site is ready for processing
  if(!processSite){next}
  
  ### Load Registry data ###
        # If database connection lost, try to reconnect
  if(!dbIsValid(registryDB)){
    registryDB <- dbConnect(
      Postgres(), 
      user = postgres_user,
      password = postgres_pass,
      dbname = database_name,
      host = database_host,
      port = database_port
    )
  }
  
        # Load data
  for(i in 1:length(dataTables)){
    
              # Get data
                    # If spatial
    if(dataTables[[i]][2]){
      data <- registryDB %>% read_sf(dataTables[[i]][1])
        
                    # If non-spatial
    }else{
      data <- registryDB %>% tbl(dataTables[[i]][1]) %>% collect()
    }
      
              # Assign data
    assign(paste0("REG_", dataTables[[i]][1]), data)
    rm(data)
  }
  rm(i)
  
  ### Only retain biodiversity elements that meet KBA criteria ###
  # Species
        # SpeciesAtSite
  DBS_SpeciesAtSite %<>%
    filter(meetscriteria == "Y") # Only retain species that meet KBA criteria
  
        # SpeciesAssessment
  DBS_SpeciesAssessment %<>%
    filter(speciesatsiteid %in% DBS_SpeciesAtSite$speciesatsiteid)
  
        # PopSizeCitation
  DBS_PopSizeCitation %<>%
    filter(speciesassessmentid %in% DBS_SpeciesAssessment$speciesassessmentid)
  
        # Species
  DBS_Species %<>%
    filter(speciesid %in% DBS_SpeciesAtSite$speciesid)
  
        # BIOTICS_ELEMENT_NATIONAL
  DBS_BIOTICS_ELEMENT_NATIONAL %<>%
    filter(speciesid %in% DBS_SpeciesAtSite$speciesid)
  
  # Ecosystems
        # EcosystemAtSite
  DBS_EcosystemAtSite %<>%
    filter(meetscriteria == "Y") # Only retain ecosystems that meet KBA criteria
  
        # EcosystemAssessment
  DBS_EcosystemAssessment %<>%
    filter(ecosystematsiteid %in% DBS_EcosystemAtSite$ecosystematsiteid)
  
        # ExtentCitation
  DBS_ExtentCitation %<>%
    filter(ecosystemassessmentid %in% DBS_EcosystemAssessment$ecosystemassessmentid)
  
        # Ecosystem
  DBS_Ecosystem %<>%
    filter(ecosystemid %in% DBS_EcosystemAtSite$ecosystemid)
  
        # BIOTICS_ECOSYSTEM
  DBS_BIOTICS_ECOSYSTEM %<>%
    filter(ecosystemid %in% DBS_EcosystemAtSite$ecosystemid)
  
  # Biodiversity element distributions
  biodivelementdistributionids <- c(DBS_SpeciesAtSite$biodivelementdistributionid, DBS_EcosystemAtSite$biodivelementdistributionid) %>% .[which(!is.na(.))]
  DBS_BiodivElementDistribution %<>%
    filter(biodivelementdistributionid %in% biodivelementdistributionids)
  
  ### If global site not yet globally accepted, set to national and add disclaimer ###
  if((DBS_KBASite$kbalevel_en %in% c("Global", "Global and National")) & (!DBS_KBASite$sitestatus == "8")){
    
    # Change level to national
    DBS_KBASite %<>%
      mutate(kbalevel_en = "National",
             kbalevel_fr = "National",
             kbalevel_es = "Nacional")
    
    # Add special disclaimer
          # Initialize disclaimer
    extraDisclaimer_EN <- "<p>"
    extraDisclaimer_FR <- "<p>"
    
          # Species
                # Get global species
    globalNotAccepted_species <- DBS_SpeciesAtSite %>%
      filter(!is.na(globalcriteria)) %>%
      left_join(., DBS_BIOTICS_ELEMENT_NATIONAL[,c("speciesid", "national_scientific_name", "national_engl_name", "national_fr_name")], by="speciesid") %>%
      mutate(national_scientific_name = replace(national_scientific_name, display_taxonname == "No", NA),
             national_engl_name = replace(national_engl_name, display_taxonname == "No", "a sensitive species"),
             national_fr_name = replace(national_fr_name, display_taxonname == "No", "une espèce sensible")) %>%
      select(national_scientific_name, national_engl_name, national_fr_name) %>%
      group_by(national_scientific_name, national_engl_name, national_fr_name) %>%
      summarize(count = n(), .groups="keep") %>%
      mutate(national_engl_name = replace(national_engl_name, (national_engl_name == "a sensitive species") & (count > 1), paste(count, "sensitive species")),
             national_fr_name = replace(national_fr_name, (national_fr_name == "une espèce sensible") & (count > 1), paste(count, "espèces sensibles"))) %>%
      select(-count) %>%
      distinct()
    
                # Compute extra disclaimer
                      # English
    if(nrow(globalNotAccepted_species) > 0){
      extraDisclaimer_EN <- globalNotAccepted_species %>%
        mutate(text = ifelse(is.na(national_scientific_name),
                             national_engl_name,
                             paste0(national_engl_name, " (<i>", national_scientific_name, "</i>)"))) %>%
        arrange(text) %>%
        pull(text) %>%
        {paste0(extraDisclaimer_EN, "The following taxa are being reviewed at the global level: ", paste0(., collapse="; "), ". ")}
    }
    
                        # French
      if(nrow(globalNotAccepted_species) > 0){
      extraDisclaimer_FR <- globalNotAccepted_species %>%
        mutate(text = ifelse(is.na(national_fr_name),
                             paste0("<i>", national_scientific_name, "</i>"),
                             ifelse(is.na(national_scientific_name),
                                    national_fr_name,
                                    paste0(national_fr_name, " (<i>", national_scientific_name, "</i>)")))) %>%
        arrange(text) %>%
        pull(text) %>%
        {paste0(extraDisclaimer_FR, "Les taxons suivants font l'objet d'un examen au niveau mondial : ", paste0(., collapse="; "), ". ")}
    }
    
          # Ecosystems
                # Get global ecosystems
    globalNotAccepted_ecosystems <- DBS_EcosystemAtSite %>%
      filter(!is.na(globalcriteria)) %>%
      left_join(., DBS_BIOTICS_ECOSYSTEM[,c("ecosystemid", "cnvc_english_name", "cnvc_french_name")], by="ecosystemid") %>%
      select(cnvc_english_name, cnvc_french_name) %>%
      distinct()
    
                # Compute extra disclaimer
                      # English
    if(nrow(globalNotAccepted_ecosystems) > 0){
      extraDisclaimer_EN <- globalNotAccepted_ecosystems %>%
        arrange(cnvc_english_name) %>%
        pull(cnvc_english_name) %>%
        {paste0(extraDisclaimer_EN, "The following ecosystems are being reviewed at the global level: ", paste0(., collapse="; "), ". ")}
    }
    
                      # French
    if(nrow(globalNotAccepted_ecosystems) > 0){
      extraDisclaimer_FR <- globalNotAccepted_ecosystems %>%
        mutate(text = ifelse(is.na(cnvc_french_name),
                             cnvc_english_name,
                             cnvc_french_name)) %>%
        arrange(text) %>%
        pull(text) %>%
        {paste0(extraDisclaimer_FR, "Les écosystèmes suivants font l'objet d'un examen au niveau mondial : ", paste0(., collapse="; "), ". ")}
    }
    
          # Finalize disclaimer
    extraDisclaimer_EN <- paste0(extraDisclaimer_EN, "Once accepted, this site will become a global KBA.</p>")
    extraDisclaimer_FR <- paste0(extraDisclaimer_FR, "Une fois cette évaluation validée, le site deviendra une KBA mondiale.</p>")
    
          # Add to site disclaimer
    DBS_KBASite %<>%
      mutate(disclaimer_en = paste0(extraDisclaimer_EN, disclaimer_en),
             disclaimer_fr = paste0(extraDisclaimer_FR, disclaimer_fr))
  }
  
  ### Handle sensitive species ###
  if(sum(DBS_SpeciesAtSite$display_taxonname == "No") > 0 | sum(DBS_SpeciesAtSite$display_assessmentinfo == "No") > 0 | sum(DBS_SpeciesAtSite$display_biodivelementdist == "No") > 0){
    
    # If Display_TaxonName = 0, create new species record with new ID
    if(sum(DBS_SpeciesAtSite$display_taxonname == "No") > 0){
      
      # Maximum species ID
      maxSensitiveSpeciesID <- REG_Species %>%
        pull(SpeciesID) %>%
        max(.) %>%
        {ifelse(. < 1000000, 1000000, .)}
      
      # Create sensitive species dataset
      sensitiveSpecies_new <- DBS_SpeciesAtSite %>%
        filter(display_taxonname == "No") %>%
        select(speciesatsiteid, speciesid, display_alternativename, display_taxonomicgroup, display_assessmentinfo, display_biodivelementdist) %>%
        mutate(newid = (maxSensitiveSpeciesID+1):(maxSensitiveSpeciesID+nrow(.))) %>%
        left_join(., DBS_BIOTICS_ELEMENT_NATIONAL[, c("speciesid", "kba_group")], by="speciesid") %>%
        mutate(display_alternativegroup = ifelse(display_taxonomicgroup == "No", "Sensitive Species", kba_group),
               display_alternativename = str_to_sentence(display_alternativename)) %>%
        mutate(display_alternativename = ifelse(display_alternativename == "Une espèce sensible",
                                                "A sensitive species",
                                                ifelse(display_alternativename == "Une espèce en péril",
                                                       "A species at risk",
                                                       display_alternativename)))
      
      # Update all relevant datasets
            # SpeciesAtSite
      DBS_SpeciesAtSite %<>%
        left_join(., sensitiveSpecies_new[, c("speciesatsiteid", "newid")], by="speciesatsiteid") %>%
        mutate(speciesid = ifelse(!is.na(newid), newid, speciesid)) %>%
        select(-newid)
      
            # REGA_Species
      REGA_Species %<>%
        add_row(SpeciesID = sensitiveSpecies_new$newid,
                CommonName_EN = str_to_sentence(sensitiveSpecies_new$display_alternativename),
                CommonName_FR = ifelse(CommonName_EN == "A sensitive species",
                                       "Une espèce sensible",
                                       ifelse(CommonName_EN == "A species at risk",
                                              "Une espèce en péril",
                                              NA)),
                InformalTaxonomicGroup = sensitiveSpecies_new$display_alternativegroup,
                Sensitive = 1) %>%
        mutate(InformalTaxonomicGroup_FR = case_when(InformalTaxonomicGroup == "Amphibians and Reptiles" ~ "Amphibiens et reptiles",
                                                     InformalTaxonomicGroup == "Birds" ~ "Oiseaux",
                                                     InformalTaxonomicGroup == "Fishes" ~ "Poissons",
                                                     InformalTaxonomicGroup == "Fungi and Lichens" ~ "Champignons et lichens",
                                                     InformalTaxonomicGroup == "Invertebrates" ~ "Invertébrés",
                                                     InformalTaxonomicGroup == "Mammals" ~ "Mammifères",
                                                     InformalTaxonomicGroup == "Nonvascular Plants" ~ "Plantes non vasculaires",
                                                     InformalTaxonomicGroup == "Vascular Plants" ~ "Plantes vasculaires",
                                                     InformalTaxonomicGroup == "Sensitive Species" ~ "Espèces sensibles",
                                                     .default = NA))
      
            # crosswalk_SpeciesID
      crosswalk_SpeciesID %<>%
        add_row(DB_SpeciesID = sensitiveSpecies_new$newid,
                REG_SpeciesID = sensitiveSpecies_new$newid,
                NSElementCode = paste0("SensSpp", sensitiveSpecies_new$newid))
      
      # Check that alternative names were correctly translated
      if(sum(is.na(REGA_Species %>% filter(Sensitive == 1) %>% pull(CommonName_FR))) > 0){
        
        stop("Some alternative name for sensitive species couldn't be translated to French.")
      }
      
      # Add records to the master sensitiveSpecies dataset
      sensitiveSpecies %<>% bind_rows(., sensitiveSpecies_new)
      rm(sensitiveSpecies_new)
    }
    
    # If Display_AssessmentInfo = No, remove assessment information
    DBS_SpeciesAssessment %<>%
      left_join(., DBS_SpeciesAtSite[,c("speciesatsiteid", "display_assessmentinfo")], by="speciesatsiteid") %>%
      mutate(across(!created_date & !created_user & !criteriamet & !dateassessed & !globalid & !kbalevel & !last_edited_date & !last_edited_user & !objectid & !speciesassessmentid & !speciesatsiteid, ~ replace(., display_assessmentinfo == "No", NA))) %>%
      select(all_of(colnames(DBS_SpeciesAssessment)))
    
    # If Display_BiodivElementDist = No, remove biodiversity element distribution
    DBS_SpeciesAtSite %<>%
      mutate(biodivelementdistributionid = replace(biodivelementdistributionid, display_biodivelementdist == "No", NA))
    
    biodivelementdistributionids <- c(DBS_SpeciesAtSite$biodivelementdistributionid, DBS_EcosystemAtSite$biodivelementdistributionid) %>% .[which(!is.na(.))]
    DBS_BiodivElementDistribution %<>%
      filter(biodivelementdistributionid %in% biodivelementdistributionids)
  }
  
  ### Compute Registry IDs ###
        # SiteID
  REG_siteID <- DBS_KBASite %>%
    left_join(., st_drop_geometry(REG_KBA_Site[,c("SiteID", "SiteCode")]), by=c("sitecode" = "SiteCode")) %>%
    mutate(SiteID = ifelse(is.na(SiteID), max(REG_KBA_Site$SiteID) + 1, SiteID)) %>%
    pull(SiteID)
  
        # SpeciesID
              # Number of new species
  nNewSpp <- crosswalk_SpeciesID %>%
    filter(DB_SpeciesID %in% DBS_SpeciesAtSite$speciesid) %>%
    filter(is.na(REG_SpeciesID)) %>%
    nrow()
  
              # Maximum SpeciesID already in the Registry
  maxSpeciesID <- max(REG_Species$SpeciesID[REG_Species$SpeciesID<1000000], na.rm=T) %>%
    {ifelse(. < 0, 0, .)} %>%
    suppressWarnings()
  
              # Add new IDs to crosswalk
  crosswalk_SpeciesID %<>%
    mutate(REG_SpeciesID = replace(REG_SpeciesID, (DB_SpeciesID %in% DBS_SpeciesAtSite$speciesid) & (is.na(REG_SpeciesID)), (maxSpeciesID+1):(maxSpeciesID+nNewSpp)))
  
              # Update in REGA_Species
  REGA_Species %<>%
    left_join(., crosswalk_SpeciesID[,c("NSElementCode", "REG_SpeciesID")], by="NSElementCode") %>%
    mutate(SpeciesID = ifelse(Sensitive == 1, SpeciesID, REG_SpeciesID)) %>%
    select(all_of(colnames(REG_Species)))
  
        # EcosystemID
              # Number of new ecosystems
  nNewEco <- crosswalk_EcosystemID %>%
    filter(DB_EcosystemID %in% DBS_EcosystemAtSite$ecosystemid) %>%
    filter(is.na(REG_EcosystemID)) %>%
    nrow()
  
              # Maximum EcosystemID already in the Registry
  maxEcosystemID <- max(REG_Ecosystem$EcosystemID, na.rm=T) %>%
    {ifelse(. < 0, 0, .)} %>%
    suppressWarnings()
  
              # Add new IDs to crosswalk
  crosswalk_EcosystemID %<>%
    mutate(REG_EcosystemID = replace(REG_EcosystemID, (DB_EcosystemID %in% DBS_EcosystemAtSite$ecosystemid) & (is.na(REG_EcosystemID)), (maxEcosystemID+1):(maxEcosystemID+nNewEco)))
  
              # Update in REGA_Ecosystem
  REGA_Ecosystem %<>%
    left_join(., crosswalk_EcosystemID[,c("NSElementCode_IVC", "REG_EcosystemID")], by="NSElementCode_IVC") %>%
    mutate(EcosystemID = REG_EcosystemID) %>%
    select(all_of(colnames(REG_Ecosystem)))
  
  ### Convert to Registry data model ###
  # KBA_Site
  REGS_KBA_Site <- DBS_KBASite %>%
    rename(Name_EN = nationalname,
           SiteCode = sitecode,
           WDKBAID = wdkbaid,
           DateAssessed = dateassessed,
           AltMin = altitudemin,
           AltMax = altitudemax,
           Area = areakm2,
           Version = version) %>%
    mutate(SiteID = REG_siteID,
           Name_FR = ifelse(is.na(nationalname_fr), Name_EN, nationalname_fr),
           Level_EN = ifelse(grepl("Global", kbalevel_en), "Global", "National"),
           BoundaryGeneralized = ifelse(boundarygeneralization == 3, 1, 0),
           Obsolete = ifelse(sitestatus == 9, "Replaced", ifelse(sitestatus == 10, "Delisted", NA)),
           Latitude = round(lat_wgs84, 2),
           Longitude = round(long_wgs84, 2),
           PercentProtected = round(percentprotected, 0)) %>%
    left_join(., REG_KBA_Province, by=c("jurisdiction_en" = "Province_EN")) %>%
    left_join(., REG_KBA_Level, by="Level_EN") %>%
    select(all_of(colnames(REG_KBA_Site))) %>% st_cast("MULTIPOLYGON")
  
  # KBA_Website
  REGS_KBA_Website <- DBS_KBASite %>%
    st_drop_geometry() %>%
    rename(PublicCredit_EN = publicauthorship_en,
           PublicCredit_FR = publicauthorship_fr,
           Disclaimer_EN = disclaimer_en,
           Disclaimer_FR = disclaimer_fr,
           SiteHistory_EN = sitehistory_en,
           SiteHistory_FR = sitehistory_fr) %>%
    mutate(SiteID = REG_siteID,
           SiteDescription_EN = ifelse(str_detect(sitedescription_en,"<p>"), sitedescription_en, sitedescription_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           SiteDescription_FR = ifelse(str_detect(sitedescription_fr,"<p>"), sitedescription_fr, sitedescription_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           Conservation_EN = ifelse(str_detect(conservation_en,"<p>"), conservation_en, conservation_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           Conservation_FR = ifelse(str_detect(conservation_fr,"<p>"), conservation_fr, conservation_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           BiodiversitySummary_EN = case_when(
           is.na(additionalbiodiversity_en) & str_detect(nominationrationale_en,"<p>") ~ nominationrationale_en,
           str_detect(nominationrationale_en,"<p>") & str_detect(additionalbiodiversity_en,"<p>") ~ paste0(nominationrationale_en,additionalbiodiversity_en),
           .default = paste0("<p>", gsub("\n\n", "</p><p>", ifelse(is.na(additionalbiodiversity_en), nominationrationale_en, paste(nominationrationale_en, additionalbiodiversity_en, sep="\n\n"))), "</p>") %>% gsub("\n", "<br>", .)),
           BiodiversitySummary_FR = case_when(
           is.na(additionalbiodiversity_fr) & str_detect(nominationrationale_fr,"<p>") ~ nominationrationale_fr,
           str_detect(nominationrationale_fr,"<p>") & str_detect(additionalbiodiversity_fr,"<p>") ~ paste0(nominationrationale_fr,additionalbiodiversity_fr),
           .default = paste0("<p>", gsub("\n\n", "</p><p>", ifelse(is.na(additionalbiodiversity_fr), nominationrationale_fr, paste(nominationrationale_fr, additionalbiodiversity_fr, sep="\n\n"))), "</p>") %>% gsub("\n", "<br>", .)),
           CustomaryJurisdiction_EN = ifelse(str_detect(customaryjurisdiction_en,"<p>"), customaryjurisdiction_en, customaryjurisdiction_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           CustomaryJurisdiction_FR = ifelse(str_detect(customaryjurisdiction_fr,"<p>"), customaryjurisdiction_fr, customaryjurisdiction_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           CustomaryJurisdictionSource_EN = ifelse(str_detect(customaryjurisdictionsrce_en,"<p>"), customaryjurisdictionsrce_en, customaryjurisdictionsrce_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           CustomaryJurisdictionSource_FR = ifelse(str_detect(customaryjurisdictionsrce_fr,"<p>"), customaryjurisdictionsrce_fr, customaryjurisdictionsrce_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .) %>% gsub("\n", "<br>", .)),
           GlobalCriteriaSummary_EN = ifelse(grepl("GLOBAL", criteriasummary_en), ifelse(grepl("NATIONAL", criteriasummary_en), substr(criteriasummary_en, start=9, stop=gregexpr("NATIONAL:", criteriasummary_en)[[1]][1]-3), substr(criteriasummary_en, start=9, stop=nchar(criteriasummary_en)-1)), NA),
           GlobalCriteriaSummary_FR = ifelse(grepl("MONDIAL", criteriasummary_fr), ifelse(grepl("NATIONAL", criteriasummary_fr), substr(criteriasummary_fr, start=11, stop=gregexpr("NATIONAL :", criteriasummary_fr)[[1]][1]-3), substr(criteriasummary_fr, start=11, stop=nchar(criteriasummary_fr)-1)), NA),
           NationalCriteriaSummary_EN = ifelse(grepl("NATIONAL", criteriasummary_en), substr(criteriasummary_en, start=gregexpr("NATIONAL:", criteriasummary_en)[[1]][1]+10, stop=nchar(criteriasummary_en)-1), NA),
           NationalCriteriaSummary_FR = ifelse(grepl("NATIONAL", criteriasummary_fr), substr(criteriasummary_fr, start=gregexpr("NATIONAL :", criteriasummary_fr)[[1]][1]+11, stop=nchar(criteriasummary_fr)-1), NA),
           Conservation_EN = ifelse(Conservation_EN == "<p>None</p>", "<p>There are no known conservation actions at this site.</p>", Conservation_EN),
           Conservation_FR = ifelse(Conservation_FR %in% c("<p>Aucun</p>", "<p>Aucune</p>"), "<p>Le site ne bénificie d'aucune action de conservation connue.</p>", Conservation_FR),
           ObsoleteReason_EN = ObsoleteReason_EN,
           ObsoleteReason_FR = ObsoleteReason_FR,
           eBirdLink = NA,
           iNatLink = NA) %>%
    select(all_of(colnames(REG_KBA_Website)))
  
  # KBA_Citation
  REGS_KBA_Citation <- DBS_KBACitation %>%
    filter(sensitive == 0) %>% # Remove sensitive citations
    rename(ShortCitation = shortcitation,
           LongCitation = longcitation,
           DOI = doi,
           URL = url) %>%
    mutate(SiteID = REG_siteID,
           KBACitationID = ifelse(nrow(.)>0, 1:nrow(.), 1)) %>%
    select(all_of(colnames(REG_KBA_Citation)))
  
  # KBA_Threats
  if(nrow(DBS_KBAThreat) > 0){
    REGS_KBA_Threats <- DBS_KBAThreat %>%
      mutate(SiteID = REG_siteID,
             ThreatsSiteID = NA,
             Threat_EN = sapply(1:nrow(.), function(x){
        threats <- .[x, c("level1", "level2")] %>%
          as.vector(.)
        level <- last(threats[which(!is.na(threats))])
      })) %>%
      mutate(ThreatCode = sapply(Threat_EN, function(x) substr(x, 1, stri_locate_all(pattern=" ", x, fixed=T)[[1]][1,1]-1))) %>%
      left_join(., REG_Threats[,c("ThreatID", "ThreatCode")], by="ThreatCode") %>%
      select(all_of(colnames(REG_KBA_Threats))) %>%
      distinct() %>%
      arrange(ThreatID) %>%
      mutate(ThreatsSiteID = 1:nrow(.))
    
  }else{
    REGS_KBA_Threats <- REG_KBA_Threats[0,]
  }
  
  # KBA_Conservation
  REGS_KBA_Conservation <- DBS_KBAAction %>%
    rename(Ongoing_Needed = ongoingorneeded) %>%
    mutate(SiteID = REG_siteID,
           ConservationAction = ifelse(conservationaction == "None", "0.0 None", conservationaction),
           ConservationCode = sapply(ConservationAction, function(x) as.numeric(substr(x, 1, stri_locate_all(pattern=" ", x, fixed=T)[[1]][1,1]-1)))) %>%
    left_join(., REG_Conservation[,c("ConservationID", "ConservationCode")], by="ConservationCode") %>%
    arrange(SiteID, ConservationID, Ongoing_Needed) %>%
    mutate(ConservationSiteID = 1:nrow(.)) %>%
    select(all_of(colnames(REG_KBA_Conservation)))
  
  # KBA_System
  REGS_KBA_System <- DBS_KBASite %>%
    st_drop_geometry() %>%
    separate_rows(., systems, sep="; ") %>%
    left_join(., REG_System[,c("SystemID", "Type_EN")], by=c("systems" = "Type_EN")) %>%
    mutate(SiteID = REG_siteID,
           SystemSiteID = 1:nrow(.),
           Rank = 1:nrow(.)) %>%
    select(all_of(colnames(REG_KBA_System)))
  
  # KBA_Habitat
  REGS_KBA_Habitat <- DBS_KBALandCover %>%
    mutate(SiteID = REG_siteID,
           HabitatSiteID = ifelse(nrow(.)>0, 1:nrow(.), 1),
           landcover_en = as.character(landcover_en)) %>%
    rename(HabitatArea = areakm2,
           PercentCover = percentcover,
           Group_EN = group_en,
           Group_FR = group_fr) %>%
    left_join(., REG_Habitat[,c("HabitatID", "Habitat_EN")], by=c("landcover_en" = "Habitat_EN")) %>%
    select(all_of(colnames(REG_KBA_Habitat)))
  
  # KBA_ProtectedArea
  REGS_KBA_ProtectedArea <- DBS_KBAProtectedArea %>%
    mutate(ProtectedAreaID = ifelse(nrow(.)>0, 1:nrow(.), 1),
           SiteID = REG_siteID,
           IUCNCat_EN = case_when(iucncat_en %in% c("Ia", "IA") ~ "Ia - Strict nature reserve",
                                  iucncat_en %in% c("Ib", "IB") ~ "Ib - Wilderness area",
                                  iucncat_en == "II" ~ "II - National park",
                                  iucncat_en == "III" ~ "III - Natural monument or feature",
                                  iucncat_en == "IV" ~ "IV - Habitat/species management area",
                                  iucncat_en == "V" ~ "V - Protected landscape or seascape",
                                  iucncat_en == "VI" ~ "VI - Protected areas with sustainable use of natural resources",
                                  .default = iucncat_en),
           IUCNCat_FR = case_when(iucncat_fr %in% c("Ia", "IA") ~ "Ia - Réserve naturelle intégrale",
                                  iucncat_fr %in% c("Ib", "IB") ~ "Ib - Zone de nature sauvage",
                                  iucncat_fr == "II" ~ "II - Parc national",
                                  iucncat_fr == "III" ~ "III - Monument ou élément naturel",
                                  iucncat_fr == "IV" ~ "IV - Aire de gestion des habitats ou des espèces",
                                  iucncat_fr == "V" ~ "V - Paysage terrestre ou marin protégé",
                                  iucncat_fr == "VI" ~ "VI - Aire protégée avec utilisation durable des ressources naturelles",
                                  .default = iucncat_fr)) %>%
    rename(PercentCover = percentcover,
           ProtectedArea_EN = protectedarea_en,
           ProtectedArea_FR = protectedarea_fr,
           Type_EN = type_en,
           Type_FR = type_fr) %>%
    select(all_of(colnames(REG_KBA_ProtectedArea)))
  
  if(DBS_KBASite$boundarygeneralization == "3"){ # If boundary generalization = 3, do not send protected area information
    REGS_KBA_ProtectedArea <- REGS_KBA_ProtectedArea[0,]
  }
  
  # Species
  REGS_Species <- REGA_Species %>%
    filter(!is.na(SpeciesID)) %>%
    left_join(., crosswalk_SpeciesID[,c("REG_SpeciesID", "DB_SpeciesID")], by=c("SpeciesID" = "REG_SpeciesID")) %>%
    filter(DB_SpeciesID %in% DBS_SpeciesAtSite$speciesid) %>%
    mutate(NationalPopulationSize = as.integer(NationalPopulationSize),
           ContinentalPopulationSize = as.integer(ContinentalPopulationSize),
           GlobalPopulationSize = as.integer(GlobalPopulationSize)) %>%
    select(all_of(colnames(REG_Species)))
  
  # Species_Citation
  REGS_Species_Citation <- relevantReferenceEstimates_spp %>%
    left_join(., crosswalk_SpeciesID[,c("REG_SpeciesID", "DB_SpeciesID")], by=c("speciesid" = "DB_SpeciesID")) %>%
    mutate(speciesid = REG_SpeciesID) %>%
    select(-REG_SpeciesID) %>%
    filter(speciesid %in% REGS_Species$SpeciesID) %>%
    pivot_longer(cols = c(CitationGlobalPopulation, CitationNationalPopulation), names_to = "Level", values_to = "shortcitation") %>%
    select(speciesid, shortcitation) %>%
    filter(!is.na(shortcitation)) %>%
    separate_rows(shortcitation, sep="; ") %>%
    distinct() %>% # Only keep each citation once per species (this line is necessary for cases where the same citation was used for both a global and a national assessment for the species)
    left_join(., DBS_KBACitation[, c("shortcitation", "longcitation", "url", "doi", "sensitive")], by="shortcitation") %>%
    filter(sensitive == 0) %>% # Remove sensitive citations
    rename(SpeciesID = speciesid,
           ShortCitation = shortcitation,
           LongCitation = longcitation,
           URL = url,
           DOI = doi) %>%
    mutate(SpeciesCitationID = ifelse(nrow(.)>0, 1:nrow(.), 1)) %>%
    select(all_of(colnames(REG_Species_Citation)))
  
  # KBA_SpeciesAssessments
  REGS_KBA_SpeciesAssessments <- DBS_SpeciesAssessment %>%
    left_join(., DBS_SpeciesAtSite[,c("speciesatsiteid", "speciesid", "biodivelementdistributionid")], by="speciesatsiteid") %>%
    left_join(., crosswalk_SpeciesID[,c("REG_SpeciesID", "DB_SpeciesID")], by=c("speciesid" = "DB_SpeciesID")) %>%
    rename(SpeciesID = REG_SpeciesID,
           DateAssessed = dateassessed,
           PercentAtSite = percentatsite,
           SeasonalDistribution = seasonaldistribution,
           MinReproductiveUnits = ru_min,
           RUType = ru_composition10rus,
           RUSources = ru_sources,
           MinSitePopulation = siteestimate_min,
           BestSitePopulation = siteestimate_best,
           MaxSitePopulation = siteestimate_max,
           SiteDerivation = siteestimate_derivation,
           SitePopulationSources = siteestimate_sources,
           MinRefPopulation = referenceestimate_min,
           BestRefPopulation = referenceestimate_best,
           MaxRefPopulation = referenceestimate_max,
           RefPopulationSources = referenceestimate_sources,
           SpeciesAssessmentsID = speciesassessmentid,
           InternalBoundaryID = biodivelementdistributionid) %>%
    mutate(SiteID = REG_siteID,
           SpeciesStatus = ifelse(is.na(status_value), NA, paste0(status_value, " (", status_assessmentagency, ")")) %>% as.character(),
           status_assessmentagency_FR = case_when(status_assessmentagency == "IUCN" ~ "UICN",
                                                  status_assessmentagency == "COSEWIC" ~ "COSEPAC",
                                                  status_assessmentagency == "NatureServe" ~ "NatureServe",
                                                  .default = NA),
           status_value_FR = case_when(status_value == "X" ~ "D",
                                       status_value == "XT" ~ "DP",
                                       status_value == "E" ~ "VD",
                                       status_value == "T" ~ "M",
                                       status_value == "SC" ~ "P",
                                       status_value == "DD" ~ "DI",
                                       status_value == "NAR" ~ "NEP",
                                       .default = status_value),
           SpeciesStatus_FR = ifelse(is.na(status_value_FR), NA, paste0(status_value_FR, " (", status_assessmentagency_FR, ")")) %>% as.character(),
           FootnoteID = NA,
           AssessmentParameter_EN = sapply(assessmentparameter, function(x) str_to_sentence(substr(x, start=gregexpr(")", x, fixed=T)[[1]][1]+2, stop=nchar(x)))),
           AssessmentParameter_EN = as.character(AssessmentParameter_EN),
           SeasonalDistribution_FR = case_when(SeasonalDistribution == "Migration site" ~ "Site de migration",
                                               SeasonalDistribution == "Breeding" ~ "Site de reproduction",
                                               SeasonalDistribution == "Non-breeding" ~ "Site non reproducteur",
                                               .default = SeasonalDistribution)) %>%
    left_join(., REG_AssessmentParameter[,c("AssessmentParameterID", "AssessmentParameter_EN")], by="AssessmentParameter_EN") %>%
    left_join(.,REG_KBA_SpeciesAssessments %>% 
                select(SpeciesID,SiteID,DateAssessed,Original_ScientificName,Original_CommonNameFR, Original_CommonNameEN) %>% distinct(),
              by=c("SiteID","SpeciesID","DateAssessed"))  %>%
    left_join(.,crosswalk_SpeciesID,by=c("SpeciesID"="REG_SpeciesID")) %>%
    left_join(.,DB_BIOTICS_ELEMENT_NATIONAL %>% select(speciesid,national_scientific_name,national_engl_name, national_fr_name), by=c("DB_SpeciesID"="speciesid")) %>% 
    mutate(Original_ScientificName=case_when(is.na(Original_ScientificName)~national_scientific_name,
                                             .default = Original_ScientificName),
           Original_CommonNameEN=case_when(is.na(Original_CommonNameEN)~national_engl_name,
                                           .default = Original_CommonNameEN),
           Original_CommonNameFR=case_when(is.na(Original_CommonNameFR)~national_fr_name,
                                           .default = Original_CommonNameFR))%>%
    select(all_of(colnames(REG_KBA_SpeciesAssessments)))
    
  # SpeciesAssessment_Subcriterion
  REGS_SpeciesAssessment_Subcriterion <- DBS_SpeciesAssessment %>%
    separate_rows(., criteriamet, sep="; ") %>%
    mutate(criteriamet = substr(criteriamet, start=2, stop=nchar(criteriamet)),) %>%
    left_join(., REG_KBA_Level[,c("LevelID", "Level_EN")], by=c("kbalevel" = "Level_EN")) %>%
    left_join(., REG_Subcriterion[,c("SubcriterionID", "Subcriterion", "LevelID")], by=c("criteriamet" = "Subcriterion", "LevelID" = "LevelID")) %>%
    rename(SpeciesAssessmentsID = speciesassessmentid) %>%
    mutate(AssessmentSubcriterionID = ifelse(nrow(.)>0, 1:nrow(.), 1)) %>%
    select(all_of(colnames(REG_SpeciesAssessment_Subcriterion)))
  
  # Ecosystem
  REGS_Ecosystem <- REGA_Ecosystem %>%
    filter(!is.na(EcosystemID)) %>%
    left_join(., crosswalk_EcosystemID[,c("REG_EcosystemID", "DB_EcosystemID")], by=c("EcosystemID" = "REG_EcosystemID")) %>%
    filter(DB_EcosystemID %in% DBS_EcosystemAtSite$ecosystemid) %>%
    select(all_of(colnames(REG_Ecosystem)))
  
  # Ecosystem_Citation
  REGS_Ecosystem_Citation <- relevantReferenceEstimates_eco %>%
    left_join(., crosswalk_EcosystemID[,c("REG_EcosystemID", "DB_EcosystemID")], by=c("ecosystemid" = "DB_EcosystemID")) %>%
    mutate(ecosystemid = REG_EcosystemID) %>%
    select(-REG_EcosystemID) %>%
    filter(ecosystemid %in% REGS_Ecosystem$EcosystemID) %>%
    mutate(CitationGlobalExtent = as.character(CitationGlobalExtent),
           CitationNationalExtent = as.character(CitationNationalExtent)) %>%
    pivot_longer(cols = c(CitationGlobalExtent, CitationNationalExtent), names_to = "Level", values_to = "shortcitation") %>%
    select(ecosystemid, shortcitation) %>%
    filter(!is.na(shortcitation)) %>%
    separate_rows(shortcitation, sep="; ") %>%
    distinct() %>% # Only keep each citation once per ecosystem (this line is necessary for cases where the same citation was used for both a global and a national assessment for the ecosystem)
    left_join(., DBS_KBACitation[, c("shortcitation", "longcitation", "url", "doi", "sensitive")], by="shortcitation") %>%
    filter(sensitive == 0) %>% # Remove sensitive citations
    rename(EcosystemID = ecosystemid,
           ShortCitation = shortcitation,
           LongCitation = longcitation,
           URL = url,
           DOI = doi) %>%
    mutate(EcosystemCitationID = ifelse(nrow(.)>0, 1:nrow(.), 1)) %>%
    select(all_of(colnames(REG_Ecosystem_Citation)))
  
  # KBA_EcosystemAssessments
  REGS_KBA_EcosystemAssessments <- DBS_EcosystemAssessment %>%
    left_join(., DBS_EcosystemAtSite[,c("ecosystematsiteid", "ecosystemid", "biodivelementdistributionid")], by="ecosystematsiteid") %>%
    left_join(., crosswalk_EcosystemID[,c("REG_EcosystemID", "DB_EcosystemID")], by=c("ecosystemid" = "DB_EcosystemID")) %>%
    rename(EcosystemID = REG_EcosystemID,
           DateAssessed = dateassessed,
           PercentAtSite = percentatsite,
           MinSiteExtent = siteestimate_min,
           BestSiteExtent = siteestimate_best,
           MaxSiteExtent = siteestimate_max,
           SiteExtentSources = siteestimate_sources,
           MinRefExtent = referenceestimate_min,
           BestRefExtent = referenceestimate_best,
           MaxRefExtent = referenceestimate_max,
           RefExtentSources = referenceestimate_sources,
           EcosystemAssessmentsID = ecosystemassessmentid,
           InternalBoundaryID = biodivelementdistributionid) %>%
    mutate(SiteID = REG_siteID,
           EcosystemStatus = ifelse(is.na(status_value), NA, paste0(status_value, " (", status_assessmentagency, ")")) %>% as.character(),
           status_assessmentagency_FR = case_when(status_assessmentagency == "IUCN" ~ "UICN",
                                                  status_assessmentagency == "COSEWIC" ~ "COSEPAC",
                                                  status_assessmentagency == "NatureServe" ~ "NatureServe",
                                                  .default = NA),
           EcosystemStatus_FR = ifelse(is.na(status_value), NA, paste0(status_value, " (", status_assessmentagency_FR, ")")) %>% as.character(),
           FootnoteID = NA) %>%
    left_join(.,REG_KBA_EcosystemAssessments %>% 
                select(SiteID,EcosystemID,DateAssessed,
                       Original_ScientificNameEN,Original_ScientificNameFR,
                       Original_TypeFR,Original_TypeEN),by=c("SiteID","EcosystemID","DateAssessed")) %>%
    left_join(.,crosswalk_EcosystemID,by=c("EcosystemID"="REG_EcosystemID")) %>%
    left_join(.,DB_BIOTICS_ECOSYSTEM %>% 
                select(ecosystemid,ivc_formatted_scientific_name, 
                       cnvc_english_name,cnvc_french_name), by=c("DB_EcosystemID"="ecosystemid"))%>% 
    left_join(.,DB_Ecosystem %>% 
                select(ecosystemid,ivc_formatted_scientific_name_fr, 
                       ivc_name_fr), by=c("DB_EcosystemID"="ecosystemid"))%>% 
    mutate(Original_ScientificNameEN=case_when(is.na(Original_ScientificNameEN)~ivc_formatted_scientific_name,
                                             .default = Original_ScientificNameEN),
           Original_ScientificNameFR=case_when(is.na(Original_ScientificNameFR)~ivc_formatted_scientific_name_fr,
                                           .default = Original_ScientificNameFR),
           Original_TypeFR=case_when(is.na(Original_TypeFR) ~ cnvc_french_name,
                                           .default = Original_TypeFR),
           Original_TypeEN=case_when(is.na(Original_TypeEN) ~ cnvc_english_name,
                                           .default = Original_TypeEN))%>%
    select(all_of(colnames(REG_KBA_EcosystemAssessments)))
  
  # EcosystemAssessment_Subcriterion
  REGS_EcosystemAssessment_Subcriterion <- DBS_EcosystemAssessment %>%
    separate_rows(., criteriamet, sep="; ") %>%
    mutate(criteriamet = substr(criteriamet, start=2, stop=nchar(criteriamet)),) %>%
    left_join(., REG_KBA_Level[,c("LevelID", "Level_EN")], by=c("kbalevel" = "Level_EN")) %>%
    left_join(., REG_Subcriterion[,c("SubcriterionID", "Subcriterion", "LevelID")], by=c("criteriamet" = "Subcriterion", "LevelID" = "LevelID")) %>%
    rename(EcosystemAssessmentsID = ecosystemassessmentid) %>%
    mutate(EcoAssessmentSubcriterionID = ifelse(nrow(.)>0, 1:nrow(.), 1)) %>%
    select(all_of(colnames(REG_EcosystemAssessment_Subcriterion)))
  
  # InternalBoundary
  REGS_InternalBoundary <- DBS_BiodivElementDistribution %>%
    rename(InternalBoundaryID = biodivelementdistributionid,
           Name_EN = name_en,
           Name_FR = name_fr,
           InternalBoundarySummary_EN = description_en,
           InternalBoundarySummary_FR = description_fr) %>%
    mutate(Area = round(as.double(st_area(.))), 3) %>%
    select(all_of(colnames(REG_InternalBoundary))) %>% 
    st_cast("MULTIPOLYGON")
  
  ### Update species name in website text
  for(spp in REGS_KBA_SpeciesAssessments$SpeciesID){
    
    if(spp < 1000000){
    
      for(nameType in c("ScientificName", "CommonNameEN", "CommonNameFR")){
        
        # Get original name
        originalName <- REGS_KBA_SpeciesAssessments %>%
          filter(SpeciesID == spp) %>%
          pull(paste0("Original_", nameType)) %>%
          unique()
        
        # Get new name
        newName <- REGS_Species %>%
          filter(SpeciesID == spp)
        
        if(nameType == "ScientificName"){
          newName %<>% pull(ScientificName)
          
        }else if(nameType == "CommonNameEN"){
          newName %<>% pull(CommonName_EN)
          
        }else{
          newName %<>% pull(CommonName_FR)
        }
          
        # If different, replace original name with new name in relevant text fields
        if(!is.na(originalName) && !originalName == newName){
          
          for(field in c("SiteDescription_EN", "SiteDescription_FR", "BiodiversitySummary_EN", "BiodiversitySummary_FR", "Conservation_EN", "Conservation_FR", "CustomaryJurisdiction_EN", "CustomaryJurisdiction_FR", "Disclaimer_EN", "Disclaimer_FR", "SiteHistory_EN", "SiteHistory_FR", "ObsoleteReason_EN", "ObsoleteReason_FR")){
            REGS_KBA_Website[1, field] <- gsub(originalName, newName, REGS_KBA_Website[1, field], fixed=T)
          }
        }
      }
    }
  }
  rm(spp, nameType, field) %>% suppressWarnings()
  
  ### Add HTML tags to superscripts in website text
  for(field in c("SiteDescription_EN", "SiteDescription_FR", "BiodiversitySummary_EN", "BiodiversitySummary_FR", "Conservation_EN", "Conservation_FR", "CustomaryJurisdiction_EN", "CustomaryJurisdiction_FR", "CustomaryJurisdictionSource_EN", "CustomaryJurisdictionSource_FR", "Disclaimer_EN", "Disclaimer_FR", "SiteHistory_EN", "SiteHistory_FR", "PublicCredit_EN", "PublicCredit_FR", "ObsoleteReason_EN", "ObsoleteReason_FR")){
    REGS_KBA_Website[1, field] <- gsub("m2", "m<sup>2</sup>", REGS_KBA_Website[1, field], fixed=T)
  }
  
  ### Add/update information for the site in the Registry Database ###
  # Start transaction
  registryDB %>% dbBegin()
  transaction <<- TRUE
  
  # KBA_Site
  registryDB %>% update.table("KBA_Site","SiteID",REGS_KBA_Site,REG_KBA_Site)
  
  # KBA_Website
  registryDB %>% update.table("KBA_Website","SiteID",REGS_KBA_Website,REG_KBA_Website)
  
  # KBA_System
        # Create new full table in case systems have been added or removed from a site
  New_KBA_System <- REG_KBA_System %>% 
    filter(SiteID %!in% REGS_KBA_System$SiteID) %>% 
    bind_rows(REGS_KBA_System) %>%
    arrange(SiteID) %>%
    mutate(SystemSiteID=if(n()>0) 1:n() else 0)
  
        # Update table (full update)
  registryDB %>% update.table("KBA_System","SystemSiteID",New_KBA_System,REG_KBA_System,full = T)
  
        # Remove data to free up memory
  rm(REG_KBA_System,REGS_KBA_System,New_KBA_System)
  
  # KBA_Threats
        # Create new full table in case threats have been added or removed from a site
  New_KBA_Threats <- REG_KBA_Threats %>% 
    filter(SiteID %!in% REGS_KBA_Threats$SiteID) %>% 
    bind_rows(REGS_KBA_Threats) %>%
    arrange(SiteID) %>%
    mutate(ThreatsSiteID=if(n()>0) 1:n() else 0)
  
        # Update table (full update)
  registryDB %>% update.table("KBA_Threats","ThreatsSiteID",New_KBA_Threats,REG_KBA_Threats,full = T)
  
        # Remove data to free up memory
  rm(REG_KBA_Threats,REGS_KBA_Threats,New_KBA_Threats)
  
  # KBA_Conservation
        # Create new full table in case conservation records have been added or removed from a site
  New_KBA_Conservation <- REG_KBA_Conservation %>% 
    filter(SiteID %!in% REGS_KBA_Conservation$SiteID) %>% 
    bind_rows(REGS_KBA_Conservation) %>%
    arrange(SiteID) %>%
    mutate(ConservationSiteID=if(n()>0) 1:n() else 0)
  
        # Update table (full update)
  registryDB %>% update.table("KBA_Conservation","ConservationSiteID",New_KBA_Conservation,REG_KBA_Conservation,full = T)
  
        # Remove data to free up memory
  rm(REG_KBA_Conservation,REGS_KBA_Conservation,New_KBA_Conservation)
  
  # KBA_Habitat
        # Create new full table in case habitat records have been added or removed from a site
  New_KBA_Habitat <- REG_KBA_Habitat %>% 
    filter(SiteID %!in% REGS_KBA_Habitat$SiteID) %>% 
    bind_rows(REGS_KBA_Habitat) %>%
    arrange(SiteID) %>%
    mutate(HabitatSiteID=if(n()>0) 1:n() else 0)
  
        # Update table (full update)
  registryDB %>% update.table("KBA_Habitat","HabitatSiteID",New_KBA_Habitat,REG_KBA_Habitat,full = T)
  
        # Remove data to free up memory
  rm(REG_KBA_Habitat,REGS_KBA_Habitat,New_KBA_Habitat)
  
  # KBA_ProtectedArea
        # Create new full table in case protected areas have been added or removed from a site
  New_KBA_ProtectedArea <- REG_KBA_ProtectedArea %>%
    filter(SiteID %!in% REGS_KBA_ProtectedArea$SiteID) %>%
    bind_rows(REGS_KBA_ProtectedArea) %>%
    arrange(SiteID) %>%
    mutate(ProtectedAreaID=if(n()>0) 1:n() else 0)

        # Update table (full update)
  registryDB %>% update.table("KBA_ProtectedArea","ProtectedAreaID",New_KBA_ProtectedArea,REG_KBA_ProtectedArea,full = T)
  
        # Remove data to free up memory
  rm(REG_KBA_ProtectedArea,REGS_KBA_ProtectedArea,New_KBA_ProtectedArea)
  
  # KBA_Citation
        # Create new full table in case citations have been added or removed from a site
  New_KBA_Citation <- REG_KBA_Citation %>% 
    filter(SiteID %!in% REGS_KBA_Citation$SiteID) %>% 
    bind_rows(REGS_KBA_Citation) %>%
    arrange(SiteID,LongCitation) %>%
    mutate(KBACitationID=if(n()>0) 1:n() else 0)
  
        # Update table (full update)
  registryDB %>% update.table("KBA_Citation","KBACitationID",New_KBA_Citation,REG_KBA_Citation,full = T)
  
        # Remove data to free up memory
  rm(REG_KBA_Citation,REGS_KBA_Citation,New_KBA_Citation)
  
  # Species
  registryDB %>% update.table("Species","SpeciesID",REGS_Species,REG_Species, full= F)
  
  # Species_Link
        # Query for any links of non sensitive species and species that dont have any links yet
  SpeciesLinks <- REGS_Species %>% filter(!SpeciesID %in% REG_Species_Link$SpeciesID,!Sensitive)
  REGS_Species_Link <- getSpeciesLinks(SpeciesLinks)

        # Create new full table in case species links have been added or removed from a site
  New_Species_Link <- REG_Species_Link %>%
    filter(SpeciesID %!in% REGS_Species_Link$SpeciesID) %>%
    bind_rows(REGS_Species_Link) %>%
    arrange(SpeciesID) %>%
    mutate(SpeciesLinkID=if(n()>0) 1:n() else 0)

        # Update table (full update)
  registryDB %>% update.table("Species_Link","SpeciesLinkID",New_Species_Link,REG_Species_Link,full = T)
  
        # Remove data to free up memory
  rm(REG_Species_Link,REGS_Species_Link,New_Species_Link,SpeciesLinks)
  
  # Species_Citation
        # Create new full table in case species citations have been added or removed from a site
  New_Species_Citation <- REG_Species_Citation %>% 
    filter(SpeciesID %!in% REGS_Species_Citation$SpeciesID) %>% 
    bind_rows(REGS_Species_Citation) %>%
    arrange(SpeciesID,LongCitation) %>%
    mutate(SpeciesCitationID=if(n()>0) 1:n() else 0)
  
        # Update table (full update)
  registryDB %>% update.table("Species_Citation","SpeciesCitationID",New_Species_Citation,REG_Species_Citation,full = T)
  
        # Remove data to free up memory
  rm(REG_Species_Citation,REGS_Species_Citation,New_Species_Citation)
  
  # InternalBoundary
  registryDB %>% update.table("InternalBoundary","InternalBoundaryID",REGS_InternalBoundary,REG_InternalBoundary)
  rm(REGS_InternalBoundary,REG_InternalBoundary)
  
  # KBA_SpeciesAssessments &  SpeciesAssessment_Subcriterion
        # Do left join on SpeciesAssessment_Subcriterion
  REG_SpeciesAssessment <- REG_SpeciesAssessment_Subcriterion %>%
    full_join(REG_KBA_SpeciesAssessments, by="SpeciesAssessmentsID")
  
  REGS_SpeciesAssessment <- REGS_SpeciesAssessment_Subcriterion %>%
    full_join(REGS_KBA_SpeciesAssessments, by="SpeciesAssessmentsID")
  
        # Rebuild tables
  New_SpeciesAssessment <- REG_SpeciesAssessment %>% 
    filter(SiteID %!in% REGS_SpeciesAssessment$SiteID) %>%
    bind_rows(REGS_SpeciesAssessment) %>% 
    arrange(SiteID,SpeciesID) %>% 
    mutate(AssessmentSubcriterionID=if(n()>0) 1:n() else 0)%>% 
    group_by(SiteID,SpeciesID,SpeciesStatus,DateAssessed,PercentAtSite,SeasonalDistribution,MinSitePopulation,BestSitePopulation,MaxSitePopulation,SiteDerivation,MinRefPopulation,BestRefPopulation,MaxRefPopulation,SitePopulationSources,RefPopulationSources,AssessmentParameterID,MinReproductiveUnits,RUType,RUSources,FootnoteID,InternalBoundaryID) %>% mutate(SpeciesAssessmentsID=if(n()>0) cur_group_id() else 0) %>% ungroup()
  
  New_SpeciesAssessment_Subcriterion <- New_SpeciesAssessment %>%
    select(all_of(names(REG_SpeciesAssessment_Subcriterion))) %>%
    filter(!is.na(SubcriterionID)) %>%
    mutate(AssessmentSubcriterionID=if(n()>0) 1:n() else 0) 
  
  New_KBA_SpeciesAssessments <- New_SpeciesAssessment %>%
    select(all_of(names(REG_KBA_SpeciesAssessments))) %>%
    mutate(FootnoteID=NA) %>%
    distinct()
  
        # Check for missing AssessmentIDs to remove those first
  missingIDs <- REG_KBA_SpeciesAssessments %>% 
    filter(SpeciesAssessmentsID %!in% New_KBA_SpeciesAssessments$SpeciesAssessmentsID) %>%
    pull(SpeciesAssessmentsID)
  
  if(length(missingIDs)>0){
    registryDB %>% delete.id("SpeciesAssessment_Subcriterion","SpeciesAssessmentsID",missingIDs)
  }
  
        # Update tables
  registryDB %>% update.table("KBA_SpeciesAssessments","SpeciesAssessmentsID",New_KBA_SpeciesAssessments,REG_KBA_SpeciesAssessments, full = T)
  registryDB %>% update.table("SpeciesAssessment_Subcriterion","AssessmentSubcriterionID",New_SpeciesAssessment_Subcriterion, REG_SpeciesAssessment_Subcriterion, full = T)
  
        # Remove data to free up memory
  rm(missingIDs,New_KBA_SpeciesAssessments,REG_KBA_SpeciesAssessments,New_SpeciesAssessment_Subcriterion, REG_SpeciesAssessment_Subcriterion,New_SpeciesAssessment,REG_SpeciesAssessment,REGS_SpeciesAssessment,REGS_SpeciesAssessment_Subcriterion,REGS_KBA_SpeciesAssessments)
  
  # Ecosystem
  registryDB %>% update.table("Ecosystem","EcosystemID",REGS_Ecosystem,REG_Ecosystem, full= F)
  
  # Ecosystem_Citation
        # Create new full table in case ecosystem citations have been added or removed from a site
  New_Ecosystem_Citation <- REG_Ecosystem_Citation %>% 
    filter(EcosystemID %!in% REGS_Ecosystem_Citation$EcosystemID) %>% 
    bind_rows(REGS_Ecosystem_Citation) %>%
    arrange(EcosystemID,LongCitation) %>%
    mutate(EcosystemCitationID=if(n()>0) 1:n() else 0)
  
        # Update table (full update)
  registryDB %>% update.table("Ecosystem_Citation","EcosystemCitationID",New_Ecosystem_Citation,REG_Ecosystem_Citation,full = T)
  
        # Remove data to free up memory
  rm(REG_Ecosystem_Citation,REGS_Ecosystem_Citation,New_Ecosystem_Citation)
  
  # KBA_EcosystemAssessments &  EcosystemAssessment_Subcriterion
        # Do left join on SpeciesAssessment_Subcriterion
  REG_EcosystemAssessment <- REG_EcosystemAssessment_Subcriterion %>%
    full_join(REG_KBA_EcosystemAssessments, by="EcosystemAssessmentsID")
  
  REGS_EcosystemAssessment <- REGS_EcosystemAssessment_Subcriterion %>%
    full_join(REGS_KBA_EcosystemAssessments, by="EcosystemAssessmentsID")
  
        # Rebuild tables
  New_EcosystemAssessment <- REG_EcosystemAssessment %>% 
    filter(SiteID %!in% REGS_EcosystemAssessment$SiteID) %>%
    bind_rows(REGS_EcosystemAssessment) %>% 
    arrange(SiteID,EcosystemID) %>% 
    mutate(EcoAssessmentSubcriterionID=if(n()>0) 1:n() else 0) %>% 
    group_by(SiteID, EcosystemID, DateAssessed, EcosystemStatus, PercentAtSite, MinSiteExtent, BestSiteExtent, MaxSiteExtent, SiteExtentSources, MinRefExtent, BestRefExtent, MaxRefExtent, RefExtentSources, FootnoteID, InternalBoundaryID) %>% mutate(EcosystemAssessmentsID=if(n()>0) cur_group_id() else 0) %>% ungroup()
  
  New_EcosystemAssessment_Subcriterion <- New_EcosystemAssessment %>%
    select(all_of(names(REG_EcosystemAssessment_Subcriterion))) %>%
  filter(!is.na(SubcriterionID)) %>%
    mutate(EcoAssessmentSubcriterionID=if(n()>0) 1:n() else 0) 
  
  New_KBA_EcosystemAssessments <- New_EcosystemAssessment %>%
    select(all_of(names(REG_KBA_EcosystemAssessments)))%>%
    mutate(FootnoteID=NA) %>%
    distinct()
  
        # Check for missing AssessmentIDs to remove those first
  missingIDs <- REG_KBA_EcosystemAssessments %>% 
    filter(EcosystemAssessmentsID %!in% New_KBA_EcosystemAssessments$EcosystemAssessmentsID) %>%
    pull(EcosystemAssessmentsID)
  
  if(length(missingIDs)>0){
    registryDB %>% delete.id("EcosystemAssessment_Subcriterion","EcosystemAssessmentsID",missingIDs)
  }
  
        # Update tables
  registryDB %>% update.table("KBA_EcosystemAssessments","EcosystemAssessmentsID",New_KBA_EcosystemAssessments,REG_KBA_EcosystemAssessments, full = T)
  registryDB %>% update.table("EcosystemAssessment_Subcriterion","EcoAssessmentSubcriterionID", New_EcosystemAssessment_Subcriterion, REG_EcosystemAssessment_Subcriterion, full = T)
  
        # Remove data to free up memory
  rm(missingIDs,New_KBA_EcosystemAssessments,REG_KBA_EcosystemAssessments,New_EcosystemAssessment_Subcriterion, REG_EcosystemAssessment_Subcriterion,New_EcosystemAssessment,REG_EcosystemAssessment,REGS_EcosystemAssessment,REGS_EcosystemAssessment_Subcriterion,REGS_KBA_EcosystemAssessments)
  
  # End transaction, if no errors
  registryDB %>% dbCommit()
  transaction <<- FALSE
  
  ### Prepare site notification
        # Get notification type
  if(!REG_siteID %in% REG_KBA_Site$SiteID){
    siteNotification <- "New site"
    
  }else if(!REGS_KBA_Site$Version == REG_KBA_Site$Version[which(REG_KBA_Site$SiteID == REG_siteID)]){
    siteNotification <- "New version"
    
  }else{
    siteNotification <- "Site edit"
  }
  
        # Add to siteNotifications
  siteNotifications %<>%
    add_row(sitecode = DBS_KBASite$sitecode,
            sitename = DBS_KBASite$nationalname,
            jurisdiction = DBS_KBASite$jurisdiction_en,
            type = siteNotification,
            leademail = DBS_KBASite$proposallead_email)
  
  ### Print site name ###
  print(paste0(DBS_KBASite$nationalname," (",DBS_KBASite$sitecode,")"))
  
  ### End of tryCatch call ###
  }, error=function(e){
    if(transaction){
    registryDB %>% dbRollback() ### rollback site on error
      transaction <- FALSE
    }
    if(exists("DBS_KBASite")){
    message(paste(DBS_KBASite$nationalname, "KBA not processed."))
    
    # Store error info
    siteErrors <<- siteErrors %>% 
      add_row(site=DBS_KBASite$nationalname,
              sitecode=DBS_KBASite$sitecode,
              error=e[["message"]])
    message(paste0(e[["message"]], "\n"))
    } else {
      siteErrors <<- siteErrors %>% 
        add_row(site="Unknown Site (Missing DBS_KBASite) with KBA-EBAR ID ",
                sitecode=paste0(id),
                error=e[["message"]])
      message(paste0(e[["message"]], "\n"))
      
    }
  })
  
  ### Remove site-specific data ###
  rm(list=setdiff(ls(), c(ls(pattern = "DB_"), ls(pattern = "REG_"), ls(pattern = "REGA_"), "id", "lastPipelineRun", "relevantReferenceEstimates_spp", "relevantReferenceEstimates_eco", "sensitiveSpecies", "maxSensitiveSpeciesID", "siteNotifications", "siteErrors", "dataTables", "registryDB", "crosswalk_SpeciesID", "crosswalk_EcosystemID", "read_KBACanadaProposalForm", "read_KBAEBARDatabase", "filter_KBAEBARDatabase", "check_KBADataValidity", "trim_KBAEBARDataset", "update_KBAEBARDataset", "primaryKey_KBAEBARDataset", "mailtrap_pass", "pipeline.email", "cleanup.internalboundary", "cleanup.footnote", "cleanup.ecosystems", "cleanup.species", "delete.sites", "getSpeciesLinks", "url_exists", "update.table", "delete.id", "updatetextSQL", "%!in%","create.shapefile", "geoserver_pass","docker_env","transaction","generate.footnotes","kbapipeline_pswd", "postgres_user", "postgres_pass", "database_name", "database_host", "mailtrap_pass", "database_port")))
}
rm(id)

#### DELETIONS - Delete sites, species, and ecosystems ####
# Sites to retain
KBASite_retain <- DB_KBASite %>%
  st_drop_geometry() %>%
  filter(sitestatus >= 6) %>%
  pull(sitecode) %>%
  unique()

cleanupError <- c()

# Delete old site, species, and ecosystem records
transaction <- F
tryCatch({
  
# Get most recent version of KBA_Site
REG_KBA_Site <- registryDB %>% read_sf("KBA_Site")
  
# Get site codes to delete
deletesitecodes <- REG_KBA_Site %>% 
  filter(SiteCode %!in% KBASite_retain) %>% 
  arrange(SiteCode) %>%
  pull(SiteCode)

# Save site deletion notifications
if(length(deletesitecodes)>0){
  for (i in 1:length(deletesitecodes)) {
    provinceid <- REG_KBA_Site %>% filter(SiteCode == deletesitecodes[i]) %>% pull(ProvinceID)
    siteNotifications %<>% 
      add_row(sitecode = deletesitecodes[i],
              sitename = REG_KBA_Site %>% filter(SiteCode == deletesitecodes[i]) %>% pull(Name_EN),
              jurisdiction = REG_KBA_Province %>% filter(ProvinceID==provinceid)%>% pull(Province_EN),
              type = "Site deletion",
              leademail = NA)
    
  }
 

}
# Start transaction
registryDB %>% dbBegin()
transaction <- T

# Delete sites
registryDB %>% delete.sites(deletesitecodes)

# Clean up other records 
registryDB %>% cleanup.internalboundary()
registryDB %>% cleanup.species()
registryDB %>% cleanup.ecosystems()
  
#### FOOTNOTES - Generate footnotes for species and ecosystems ####
registryDB %>% generate.footnotes(crosswalk_SpeciesID,
                                  DB_BIOTICS_ELEMENT_NATIONAL,
                                  DB_Species,
                                  crosswalk_EcosystemID,
                                  DB_BIOTICS_ECOSYSTEM,
                                  DB_Ecosystem)
  
# End transaction, if no errors
registryDB %>% dbCommit()
transaction <- F
  
},error=function(e){
  
  # Rollback errors
  if(transaction){
    registryDB %>% dbRollback()
  }
  
  # Store error message
  cleanupError <<- c(e[["message"]])
})

#### NOTIFICATIONS ####
# Send error email
if(nrow(siteErrors)>0 | length(cleanupError) >0){
  errornumber <- nrow(siteErrors) + length(cleanupError)
  errortext <- if(nrow(siteErrors)>0 ){paste0("<li>",siteErrors$site," (",siteErrors$sitecode,"): ",siteErrors$error,"</li>",collapse = "")} else {""}
  errortext <- paste0(errortext,if(length(cleanupError) >0){paste0("<li>Cleanup Error: ",cleanupError,"</li>",collapse = "")}else{""})
  body <- paste0("KBA Pipeline run completed with ",errornumber," errors. Errors are as follows: <ul>",errortext,"</ul>")
  pipeline.email(to=c("devans@birdscanada.org","cdebyser@wcs.org"),
                subject = "KBA Registry Error Notification",
                password = mailtrap_pass,
                message = body)
  
}else{
  lastPipelineRun <- Sys.time() - 3*3600 # minus three hours just to make sure nothing is missed
  saveRDS(lastPipelineRun,"lastPipelineRun.RDS")
}

# If on production, refresh vector tiles
if(docker_env=="Production"){
  httr::POST(url = "https://kbacanada.org/geoserver/gwc/rest/masstruncate",
             config = authenticate("admin",geoserver_pass,type = "basic"),
             body = "<truncateLayer><layerName>kba:KBASite</layerName></truncateLayer>",
             content_type("text/xml"))
}

# Send completion email
      # Separate new sites/versions from site edits
siteNotificationsNew <- siteNotifications %>%
  filter(type %!in% c("Site edit","Site deletion"))

siteNotificationsEdit <- siteNotifications %>%
  filter(type %in% c("Site edit","Site deletion"))
  
      # Notifications for changes
if(nrow(siteNotifications) > 0){
  
  # If new sites or versions were added
  if(nrow(siteNotificationsNew) > 0){
  
    # Generate text information about every site
    siteNotificationsNew %<>%
      arrange(sitename) %>%
      mutate(text = paste0("&emsp;&bull; ", sitename, " (", jurisdiction, "): https://kbacanada.org/site/?SiteCode=", sitecode))
    
    # Body of email
    notificationMessage <- ""
    
    if(siteNotificationsNew %>% filter(type == "New site") %>% nrow(.) > 0){
      notificationMessage <- paste0(notificationMessage,
                                    "<br><br>The following sites were added to the Registry:<br>",
                                    paste(siteNotificationsNew %>% filter(type == "New site") %>% pull(text), collapse="<br>"),
                                    "<br>")
    }
    
    if(siteNotificationsNew %>% filter(type == "New version") %>% nrow(.) > 0){
      notificationMessage <- paste0(notificationMessage,
                                    "<br><br>New versions of the following sites were published on the Registry:<br>",
                                    paste(siteNotificationsNew %>% filter(type == "New version") %>% pull(text), collapse="<br>"),
                                    "<br>")
    }
    
    # Vector of emails to notify
    notificationEmails <-  c("devans@birdscanada.org", "abichel@birdscanada.org","acouturier@bsc-eoc.org", "cdebyser@wcs.org", "psoroye@wcs.org", "craudsepp@wcs.org", "aleung@wcs.org", "dbrowne@birdscanada.org") %>%
      c(., trimws(unlist(strsplit(siteNotificationsNew$leademail, ";")))) %>%
      unique()
    
    # Send email if on production
    if(docker_env=="Production"){
        
      pipeline.email(to = notificationEmails,
                     password = mailtrap_pass,
                     subject = "KBA Registry Update Notification",
                     message = notificationMessage)
        
      # Prepare shapefile for Sandra
      shapefilePath <- create.shapefile(registryDB,path="Shapefile",sitecodes=siteNotifications$sitecode)
      
      # Send to Sandra
      pipeline.email(to = "smarquez@birdscanada.org",
                     password = mailtrap_pass,
                     subject = "KBA Registry Update Notification",
                     message = notificationMessage,
                     attachment = shapefilePath)
      
    }else{
      
      pipeline.email(to = c("devans@birdscanada.org","cdebyser@wcs.org"),
                     password = mailtrap_pass,
                     subject = "KBA Registry Update Notification",
                     message = notificationMessage)
      
    }
  }
  
  # If existing sites were modified
  if(nrow(siteNotificationsEdit) > 0){
    
    notificationMessage <- ""
    
    if(siteNotificationsEdit %>% filter(type == "Site edit") %>% nrow(.) > 0){
      
    # Generate text information about every site
    siteNotificationsEdit %<>%
      arrange(sitename) %>%
      mutate(text = paste0("&emsp;&bull; ", sitename, " (", jurisdiction, "): https://kbacanada.org/site/?SiteCode=", sitecode))
    
    # Body of email
    notificationMessage <- paste0(notificationMessage,
                                  "<br><br>The following sites were modified on the Registry:<br>",
                                  paste(siteNotificationsEdit %>% filter(type == "Site edit") %>% pull(text), collapse="<br>"),
                                  "<br>")
    
    }
    if(siteNotificationsEdit %>% filter(type == "Site deletion") %>% nrow(.) > 0){
      
      # Generate text information about every site
      siteNotificationsEdit %<>%
        arrange(sitename) %>%
        mutate(text = paste0("&emsp;&bull; ", sitename, " (", jurisdiction, ")"))
      
      # Body of email
      notificationMessage <- paste0(notificationMessage,
                                    "<br><br>The following sites were deleted from the Registry:<br>",
                                    paste(siteNotificationsEdit %>% filter(type == "Site deletion") %>% pull(text), collapse="<br>"),
                                    "<br>")
      
    }
    
    # Vector of emails to notify
    notificationEmails <-  c("devans@birdscanada.org", "cdebyser@wcs.org", "abichel@birdscanada.org")
    
    # Send email
    pipeline.email(to = notificationEmails,
                   password = mailtrap_pass,
                   subject = "KBA Registry Update Notification - Site edits",
                   message = notificationMessage)
  }
  
      # Notification for no changes
}else{
  
  pipeline.email(to=c("devans@birdscanada.org","cdebyser@wcs.org"),
                 subject = "KBA Registry Notification",
                 password = mailtrap_pass,
                 message = "KBA Pipeline run completed with no errors!")
 }
}

# Close database connection, if it exists
if(exists("registryDB")){
  registryDB %>% dbDisconnect()
}
