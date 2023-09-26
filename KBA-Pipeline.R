#### KBA Canada Pipeline
#### Wildlife Conservation Society Canada and Birds Canada
#### 2023

#### INSTRUCTIONS #################################################################################
# This code should never be edited directly on the server.                                        #
# Instead, please edit the code locally and push your edits to the GitHub repository.             #
###################################################################################################

#### TO DO: Add handling of sites with multiple versions (e.g. Canadian lake superior)
#### TO DO: Do not send protected area information if boundarygeneralization == "3"
#### TO DO: Add way to match new records to previously existing IDs (e.g. for citations - if a given citation was already used in a previous KBA proposal)
#### TO DO: Don't send global sites to Registry if they don't have a WDKBAID
#### TO DO: Find substitutes for everything that is hard coded
#### TO DO: Add SARA_STATUS_DATE and NSX_URL when available in BIOTICS_ELEMENT_NATIONAL
#### TO DO: Add footnotes for species and ecosystems, where applicable (e.g. change in classification of species/ecosystem, change in status, etc.)
#### TO DO: Implement FootnoteID (right now it is just set to NA)
#### TO DO: For species that lack an IUCN/COSEWIC assessment, replace NAs with ID for "Not Assessed" (see updated IUCNStatus and COSEWICStatus lookup tables)


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

# Functions
      # Key KBA functions
source_url("https://github.com/chloedebyser/KBA-Public/blob/main/KBA%20Functions.R?raw=TRUE")

      # Pipeline functions
source("functions.R")

# Date of last pipeline run
lastPipelineRun <- readRDS("lastPipelineRun.RDS")
lastPipelineRun <- as.POSIXct("2023-09-01", tz = "GMT") # TO DO: update this parameter so it contains the date of last pipeline run (start time, or perhaps even a little before to ensure no sites are missed)

# Environment variables 
env_vars <- c("kbapipeline_pswd", "postgres_user", "postgres_pass", "database_name", "database_host", "mailtrap_pass", "database_port")

for(env in env_vars){
  
  # Get variable
  var <- Sys.getenv(toupper(env))
  
  # Assign variable
  assign(env, var)
}
rm(env, env_vars, var)

# Coordinate reference system
crs <- readRDS("crs.RDS")

# KBA-EBAR database information
Sys.sleep(20)
read_KBAEBARDatabase(datasetNames=c("DatasetSource", "InputDataset", "ECCCRangeMap", "RangeMap", "EcoshapeOverviewRangeMap"),
                     type="exclude",
                     account="kbapipeline",
                     epsg=4326) %>%
  suppressWarnings()

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
lookupTables <- c("KBA_Province", "KBA_Level", "Threats", "Conservation", "System", "Habitat", "AssessmentParameter", "COSEWICStatus", "IUCNStatus", "Criterion", "Subcriterion", "Ecosystem_Class")

for(lookupTable in lookupTables){
  
  # Get data
  data <- registryDB %>% tbl(lookupTable) %>% collect()
  
  # Assign data
  assign(paste0("REG_", lookupTable), data)
  rm(data)
}
rm(lookupTable, lookupTables)

      # Data tables
dataTables <- list(c("KBA_Site", T), c("KBA_Website", F), c("KBA_Citation", F), c("KBA_Conservation", F), c("KBA_Threats", F), c("KBA_System", F), c("KBA_Habitat", F), c("KBA_ProtectedArea", F), c("Species", F), c("Species_Citation", F), c("KBA_SpeciesAssessments", F), c("Ecosystem", F), c("Ecosystem_Citation", F), c("KBA_EcosystemAssessments", F), c("SpeciesAssessment_Subcriterion", F), c("EcosystemAssessment_Subcriterion", F), c("Footnote", F), c("InternalBoundary", T), c("Species_Link", F))

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

#### Temporary site filters/data edits ####
# TEMP: REMOVE BIRD SITES BECAUSE NO FINAL PROPOSAL FORM - TO DO: Remove this once the Proposal Form Import Tool has been run for all bird sites
DB_KBASite %<>%
  filter(is.na(birdstechnicalreviewlink))

# TEMP: PRETENT MARBLE RIDGE ALVAR IS ACCEPTED - TO DO: Remove once done with testing
DB_KBASite %<>%
  mutate(sitestatus = replace(sitestatus, nationalname == "Marble Ridge Alvar", 6),
         confirmdate = replace(confirmdate, nationalname == "Marble Ridge Alvar", Sys.time() %>% with_tz(., tzone="GMT")),
         n_ecosystematsite = replace(n_ecosystematsite, nationalname == "Marble Ridge Alvar", 1),
         n_biodivelementdistribution = replace(n_biodivelementdistribution, nationalname == "Marble Ridge Alvar", 1))

DB_Ecosystem %<>%
  mutate(kba_group = replace(kba_group, ecosystemid == DB_BIOTICS_ECOSYSTEM[which(DB_BIOTICS_ECOSYSTEM$cnvc_english_name == "Manitoba Alvar"), "ecosystemid"], "Grassland & Shrubland"))

# TEMP: PRETEND 5 NEW SITES ARE CONFIRMED
      # Update confirm date
DB_KBASite %<>%
  mutate(confirmdate = replace(confirmdate, kbasiteid %in% c(83, 440, 615, 616, 618), Sys.time() %>% with_tz(., tzone="GMT")))

      # Update N_* fields
for(id in c(83, 440, 615, 616, 618)){
  
  filter_KBAEBARDatabase(KBASiteIDs = id, RMUnfilteredDatasets = F)
  
  DB_KBASite %<>% mutate(n_speciesatsite = replace(n_speciesatsite, kbasiteid == id, nrow(DBS_SpeciesAtSite)),
                         n_ecosystematsite = replace(n_ecosystematsite, kbasiteid == id, nrow(DBS_EcosystemAtSite)),
                         n_originaldelineation = replace(n_originaldelineation, kbasiteid == id, nrow(DBS_OriginalDelineation)),
                         n_biodivelementdistribution = replace(n_biodivelementdistribution, kbasiteid == id, nrow(DBS_BiodivElementDistribution)),
                         n_kbainputpolygon = replace(n_kbainputpolygon, kbasiteid == id, nrow(DBS_KBAInputPolygon)),
                         n_kbacustompolygon = replace(n_kbacustompolygon, kbasiteid == id, nrow(DBS_KBACustomPolygon)))
}

#### SPECIES - Update all species ####
# Read in Bird Specific Data
Bird_Species <- fromJSON("https://kba-maps.deanrobertevans.ca/api/species") %>%
  select(nselementcode,birdalphacode,bcspeciesid,nationaltrend,nationaltrendreference) %>% 
  rename(NSElementCode=nselementcode,
         BirdAlphaCode=birdalphacode,
         BCSpeciesID=bcspeciesid,
         NationalTrend=nationaltrend,
         CitationNationalTrend=nationaltrendreference)
# Create initial dataframe
REGA_Species <- DB_BIOTICS_ELEMENT_NATIONAL %>%
  left_join(., DB_Species, by="speciesid") %>%
  rename(Order = tax_order,
         ScientificName = national_scientific_name,
         InformalTaxonomicGroup = kba_group,
         CommonName_EN = national_engl_name,
         CommonName_FR = national_fr_name,
         TaxonomicLevel = ca_nname_level,
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
         NSGRank = g_rank,
         NSGRankReviewDate = g_rank_review_date,
         NSNRank = n_rank,
         NSNRankReviewDate = n_rank_review_date,
         NSLink = nsx_url) %>%
  left_join(., Bird_Species, by="NSElementCode") %>%
  mutate(Subspecies_EN = NA, # TO DO: populate
         Subspecies_FR = NA, # TO DO: populate
         Population_EN = NA, # TO DO: populate
         Population_FR = NA, # TO DO: populate
         IUCNLink = NA, # TO DO: populate
         SARAAssessmentDate = NA, # TO DO: populate
         COSEWICLink = NA, # TO DO: populate
         ContinentalPopulationSize = NA, # TO DO: populate
         CitationContinentalPopulation = NA, # TO DO: populate
         Sensitive = 0,
         Endemism = ifelse(endemism == "Yes (the element is endemic)",
                           "Y",
                           ifelse(endemism == "No (the element is not endemic)",
                                  "N",
                                  ifelse(endemism == "Breeding (endemic as a breeder only)",
                                         "B",
                                         ifelse(endemism == "Probable",
                                                "P",
                                                endemism))))) %>%
  left_join(., REG_IUCNStatus, by=c("iucn_cd" = "Nomenclature")) %>%
  left_join(., REG_COSEWICStatus, by=c("cosewic_status" = "Nomenclature"))

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
      # Get estimates that are relevant to the KBA Registry (i.e. most recent # of mature individuals global/national estimate, across all SpeciesAssessment records in the KBA-EBAR database)
relevantReferenceEstimates_spp <- DB_SpeciesAssessment %>%
  left_join(., DB_SpeciesAtSite[,c("speciesatsiteid", "speciesid")], by="speciesatsiteid") %>%
  filter(assessmentparameter == "(i) number of mature individuals") %>% # Only retain the record if the assessment parameter = number of mature individuals
  filter(!is.na(referenceestimate_best)) %>% # Only retain record if there is a best reference estimate
  group_by(speciesid, kbalevel) %>%
  arrange(speciesid, kbalevel, desc(dateassessed)) %>%
  filter(row_number()==1) # Only retain the most recent SpeciesAssessment per species and per KBALevel

      # Format
relevantReferenceEstimates_spp %<>%
  select(speciesid, kbalevel, referenceestimate_best, referenceestimate_sources) %>%
  rename(PopulationSize = referenceestimate_best,
         Citation = referenceestimate_sources) %>%
  pivot_wider(names_from = kbalevel, values_from = c(PopulationSize, Citation), names_glue = "{kbalevel}{.value}") %>%
  mutate(GlobalCitation = ifelse('GlobalCitation' %in% names(.), GlobalCitation, NA),
         NationalCitation = ifelse('NationalCitation' %in% names(.), NationalCitation, NA),
         GlobalPopulationSize = ifelse('GlobalPopulationSize' %in% names(.), GlobalPopulationSize, NA),
         NationalPopulationSize = ifelse('NationalPopulationSize' %in% names(.), NationalPopulationSize, NA)) %>%
  rename(CitationGlobalPopulation = GlobalCitation,
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

### Update species names
REGA_Species <- updateSpeciesNames(REGA_Species)

# Only retain species that are in the Registry database
REGU_Species <- REGA_Species %>%
  filter(NSElementCode %in% REG_Species$NSElementCode)

# TO DO: Use symdiff to find changes - need to add footnotes
test <- symdiff(REG_Species, REGU_Species)

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
         Class_EN = class_name,
         Class_FR = class_name_fr,
         Subclass_EN = subclass_name,
         Subclass_FR = subclass_name_fr,
         Formation_EN = formation_name,
         Formation_FR = formation_name_fr,
         Division_EN = division_name,
         Division_FR = division_name_fr,
         Macrogroup_EN = cnvc_mg_englishname,
         NSGRank = g_rank,
         NSGRankReviewDate = g_rank_review_date,
         NSNRank = n_rank,
         NSNRankReviewDate = n_rank_review_date,
         NSConceptSentence_EN = g_concept_sentence,
         NSConceptSentence_FR = g_concept_sentence_fr,
         WDKBAEcoRecID = wdkbaid,
         IUCNAssessmentDate = iucn_assessmentdate,
         IUCNStatusCriteria = iucn_criteria) %>%
  mutate(EcosystemType_FR = ifelse(is.na(cnvc_french_name), ivc_name_fr, cnvc_french_name),
         Macrogroup_FR = ifelse(is.na(cnvc_mg_frenchname), ivc_mg_name_fr, cnvc_mg_frenchname),
         Group = NA, # TO DO: Populate
         IUCNLink = NA) %>% # TO DO: Populate
  left_join(., REG_IUCNStatus, by=c("iucn_cd" = "Nomenclature")) %>%
  left_join(., REG_Ecosystem_Class[,c("EcosystemClassID", "ClassName_EN")], by=c("Subclass_EN" = "ClassName_EN"))
  
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

# TO DO: Use symdiff to find changes

# Update all ecosystems that are currently on the Registry
registryDB %>% update.table("Ecosystem", "EcosystemID", REGU_Ecosystem, REG_Ecosystem)

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

# Create empty dataframe to store errors
siteErrors<- data.frame(site=character(),sitecode=character(),error=character())

# Site processing
for(id in DB_KBASite %>% arrange(nationalname) %>% pull(kbasiteid)){
  
  tryCatch({
    
  ### Load Registry data tables ###
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
    
  ### Only process sites that are ready for and in need of publishing ###
  # Only process sites with status = "Publication of National Site" or beyond
  accepted <- DB_KBASite %>%
    filter(kbasiteid == id) %>%
    pull(sitestatus) %>%
    {ifelse(. >= 6, T, F)}
  
  if(!accepted){next}
  
  # If accepted, then filter KBA-EBAR data
  filter_KBAEBARDatabase(KBASiteIDs = id, RMUnfilteredDatasets = F)
  
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
  
  if(!processSite){next}
  
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
          # Get global triggers
    globalNotAccepted_triggers <- DBS_SpeciesAtSite %>%
      filter(!is.na(globalcriteria)) %>%
      left_join(., DBS_BIOTICS_ELEMENT_NATIONAL[,c("speciesid", "national_scientific_name", "national_engl_name", "national_fr_name")], by="speciesid") %>%
      select(national_scientific_name, national_engl_name, national_fr_name)
    
          # Compute extra disclaimer
                # English
    extraDisclaimer_EN <- globalNotAccepted_triggers %>%
      arrange(national_engl_name) %>%
      mutate(extradisclaimer_en = paste0("<p>The following taxa are being proposed as global KBA triggers: ",
                                         paste0(paste0(national_engl_name, " (<i>", national_scientific_name, "</i>)"), collapse="; "),
                                         ". Once accepted, this site will become a global KBA. In the meantime, it is a national KBA.</p>")) %>%
      ungroup() %>%
      pull(extradisclaimer_en)
    
                # French
    extraDisclaimer_FR <- globalNotAccepted_triggers %>%
      mutate(text = ifelse(!is.na(national_fr_name),
                           paste0(national_fr_name, " (<i>", national_scientific_name, "</i>)"),
                           paste0("<i>", national_scientific_name, "</i>"))) %>%
      arrange(text) %>%
      mutate(extradisclaimer_fr = paste0("<p>Les taxons suivants ont été proposés comme taxons se qualifiant au niveau mondial : ",
                                         paste0(text, collapse="; "),
                                         ". Une fois cette évaluation validée, le site deviendra une KBA mondiale. En attendant, il s'agit d'une KBA nationale.</p>")) %>%
      ungroup() %>%
      pull(extradisclaimer_fr)
    
          # Add to site disclaimer
    DBS_KBASite %<>%
      mutate(disclaimer_en = paste0(extraDisclaimer_EN, disclaimer_en),
             disclaimer_fr = paste0(extraDisclaimer_FR, disclaimer_fr))
  }
  
  ### Compute reason for obsolete, if applicable ###
  if(DBS_KBASite$sitestatus %in% c(9, 10)){
    
    stop("Site is obsolete, but ObsoleteReason text has not been specified.")
    
  }else{
    ObsoleteReason_EN <- NA
    ObsoleteReason_FR <- NA
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
               display_alternativename = str_to_sentence(display_alternativename))
      
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
                Sensitive = 1)
      
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
  maxSpeciesID <- max(REG_Species$SpeciesID, na.rm=T) %>%
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
           Obsolete = ifelse(sitestatus %in% c(9, 10), 1, 0),
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
           SiteDescription_EN = sitedescription_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           SiteDescription_FR = sitedescription_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           Conservation_EN = conservation_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           Conservation_FR = conservation_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           BiodiversitySummary_EN = paste0("<p>", gsub("\n\n", "</p><p>", ifelse(is.na(additionalbiodiversity_en), nominationrationale_en, paste(nominationrationale_en, additionalbiodiversity_en, sep="\n\n"))), "</p>"),
           BiodiversitySummary_FR = paste0("<p>", gsub("\n\n", "</p><p>", ifelse(is.na(additionalbiodiversity_fr), nominationrationale_fr, paste(nominationrationale_fr, additionalbiodiversity_fr, sep="\n\n"))), "</p>"),
           CustomaryJurisdiction_EN = customaryjurisdiction_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           CustomaryJurisdiction_FR = customaryjurisdiction_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           CustomaryJurisdictionSource_EN = customaryjurisdictionsrce_en %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           CustomaryJurisdictionSource_FR = customaryjurisdictionsrce_fr %>% gsub("\n\n", "</p><p>", .) %>% paste0("<p>", ., "</p>") %>% ifelse(. == "<p>NA</p>", NA, .),
           GlobalCriteriaSummary_EN = ifelse(grepl("GLOBAL", criteriasummary_en), ifelse(grepl("NATIONAL", criteriasummary_en), substr(criteriasummary_en, start=9, stop=gregexpr("NATIONAL:", criteriasummary_en)[[1]][1]-3), substr(criteriasummary_en, start=9, stop=nchar(criteriasummary_en)-1)), NA),
           GlobalCriteriaSummary_FR = ifelse(grepl("MONDIAL", criteriasummary_fr), ifelse(grepl("NATIONAL", criteriasummary_fr), substr(criteriasummary_fr, start=11, stop=gregexpr("NATIONAL :", criteriasummary_fr)[[1]][1]-3), substr(criteriasummary_fr, start=11, stop=nchar(criteriasummary_fr)-1)), NA),
           NationalCriteriaSummary_EN = ifelse(grepl("NATIONAL", criteriasummary_en), substr(criteriasummary_en, start=gregexpr("NATIONAL:", criteriasummary_en)[[1]][1]+10, stop=nchar(criteriasummary_en)-1), NA),
           NationalCriteriaSummary_FR = ifelse(grepl("NATIONAL", criteriasummary_fr), substr(criteriasummary_fr, start=gregexpr("NATIONAL :", criteriasummary_fr)[[1]][1]+11, stop=nchar(criteriasummary_fr)-1), NA),
           Conservation_EN = ifelse(Conservation_EN == "None", NA, Conservation_EN),
           Conservation_FR = ifelse(Conservation_FR %in% c("Aucun", "Aucune"), NA, Conservation_FR),
           ObsoleteReason_EN = ObsoleteReason_EN,
           ObsoleteReason_FR = ObsoleteReason_FR) %>%
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
        threats <- .[x, c("level1", "level2", "level3")] %>%
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
           landcover_en = ifelse(landcover_en == "Urban and built-up", "Urban", landcover_en)) %>%
    rename(HabitatArea = areakm2,
           PercentCover = percentcover) %>%
    left_join(., REG_Habitat[,c("HabitatID", "Habitat_EN")], by=c("landcover_en" = "Habitat_EN")) %>%
    select(all_of(colnames(REG_KBA_Habitat)))
  
  # KBA_ProtectedArea
  REGS_KBA_ProtectedArea <- DBS_KBAProtectedArea %>%
    mutate(ProtectedAreaID = ifelse(nrow(.)>0, 1:nrow(.), 1),
           SiteID = REG_siteID) %>%
    rename(PercentCover = percentcover,
           ProtectedArea_EN = protectedarea_en,
           ProtectedArea_FR = protectedarea_fr,
           Type_EN = type_en,
           Type_FR = type_fr,
           IUCNCat_EN = iucncat_en,
           IUCNCat_FR = iucncat_fr) %>%
    select(all_of(colnames(REG_KBA_ProtectedArea)))
  
  # Species
  REGS_Species <- REGA_Species %>%
    filter(!is.na(SpeciesID)) %>%
    left_join(., crosswalk_SpeciesID[,c("REG_SpeciesID", "DB_SpeciesID")], by=c("SpeciesID" = "REG_SpeciesID")) %>%
    filter(DB_SpeciesID %in% DBS_SpeciesAtSite$speciesid) %>%
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
           FootnoteID = NA,
           AssessmentParameter_EN = ifelse(nrow(.)>0, str_to_sentence(substr(assessmentparameter, start=gregexpr(")", assessmentparameter, fixed=T)[[1]][1]+2, stop=nchar(assessmentparameter))), "")) %>%
    left_join(., REG_AssessmentParameter[,c("AssessmentParameterID", "AssessmentParameter_EN")], by="AssessmentParameter_EN") %>%
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
           FootnoteID = NA) %>%
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
    select(all_of(colnames(REG_InternalBoundary)))
  
  ### TO DO: Dean to add code for adding/updating site ###
  registryDB %>% dbBegin()
  
  # update table
   
  registryDB %>% update.table("KBA_Site","SiteID",REGS_KBA_Site,REG_KBA_Site)
  
  #### KBA_Website ####
  
  # update table
  registryDB %>% update.table("KBA_Website","SiteID",REGS_KBA_Website,REG_KBA_Website)
  
  
  #### KBA_System ####
  
  # Create new full table in case systems have been added or removed from a site
  New_KBA_System <- REG_KBA_System %>% 
    filter(SiteID %!in% REGS_KBA_System$SiteID) %>% 
    bind_rows(REGS_KBA_System) %>%
    arrange(SiteID) %>%
    mutate(SystemSiteID=if(n()>0) 1:n() else 0)
  
  # update table - full update
  registryDB %>% update.table("KBA_System","SystemSiteID",New_KBA_System,REG_KBA_System,full = T)
  # Remove data to free up any memory
  rm(REG_KBA_System,REGS_KBA_System,New_KBA_System)
  
  
  #### KBA_Threats ####
  
  # Create new full table in case systems have been added or removed from a site
  New_KBA_Threats <- REG_KBA_Threats %>% 
    filter(SiteID %!in% REGS_KBA_Threats$SiteID) %>% 
    bind_rows(REGS_KBA_Threats) %>%
    arrange(SiteID) %>%
    mutate(ThreatsSiteID=if(n()>0) 1:n() else 0)
  
  # update table - full update
  registryDB %>% update.table("KBA_Threats","ThreatsSiteID",New_KBA_Threats,REG_KBA_Threats,full = T)
  # Remove data to free up any memory
  rm(REG_KBA_Threats,REGS_KBA_Threats,New_KBA_Threats)
  
  
  #### KBA_Conservation ####
  
  # Create new full table in case systems have been added or removed from a site
  New_KBA_Conservation <- REG_KBA_Conservation %>% 
    filter(SiteID %!in% REGS_KBA_Conservation$SiteID) %>% 
    bind_rows(REGS_KBA_Conservation) %>%
    arrange(SiteID) %>%
    mutate(ConservationSiteID=if(n()>0) 1:n() else 0)
  
  # update table - full update
  registryDB %>% update.table("KBA_Conservation","ConservationSiteID",New_KBA_Conservation,REG_KBA_Conservation,full = T)
  # Remove data to free up any memory
  rm(REG_KBA_Conservation,REGS_KBA_Conservation,New_KBA_Conservation)
  
  
  #### KBA_Habitat ####
  
  # Create new full table in case systems have been added or removed from a site
  New_KBA_Habitat <- REG_KBA_Habitat %>% 
    filter(SiteID %!in% REGS_KBA_Habitat$SiteID) %>% 
    bind_rows(REGS_KBA_Habitat) %>%
    arrange(SiteID) %>%
    mutate(HabitatSiteID=if(n()>0) 1:n() else 0)
  
  # update table - full update
  registryDB %>% update.table("KBA_Habitat","HabitatSiteID",New_KBA_Habitat,REG_KBA_Habitat,full = T)
  # Remove data to free up any memory
  rm(REG_KBA_Habitat,REGS_KBA_Habitat,New_KBA_Habitat)
  
  #### KBA_ProtectedArea ####
  
  #Create new full table in case systems have been added or removed from a site
  New_KBA_ProtectedArea <- REG_KBA_ProtectedArea %>%
    filter(SiteID %!in% REGS_KBA_ProtectedArea$SiteID) %>%
    bind_rows(REGS_KBA_ProtectedArea) %>%
    arrange(SiteID) %>%
    mutate(ProtectedAreaID=if(n()>0) 1:n() else 0)

  # update table - full update
  registryDB %>% update.table("KBA_ProtectedArea","ProtectedAreaID",New_KBA_ProtectedArea,REG_KBA_ProtectedArea,full = T)
  # Remove data to free up any memory
  rm(REG_KBA_ProtectedArea,REGS_KBA_ProtectedArea,New_KBA_ProtectedArea)
  
  
  #### KBA_Citation ####
  
  # Create new full table in case systems have been added or removed from a site
  New_KBA_Citation <- REG_KBA_Citation %>% 
    filter(SiteID %!in% REGS_KBA_Citation$SiteID) %>% 
    bind_rows(REGS_KBA_Citation) %>%
    arrange(SiteID,LongCitation) %>%
    mutate(KBACitationID=if(n()>0) 1:n() else 0)
  
  # update table - full update
  registryDB %>% update.table("KBA_Citation","KBACitationID",New_KBA_Citation,REG_KBA_Citation,full = T)
  # Remove data to free up any memory
  rm(REG_KBA_Citation,REGS_KBA_Citation,New_KBA_Citation)
  
  
  #### Species ####
  
  # update table 
  registryDB %>% update.table("Species","SpeciesID",REGS_Species,REG_Species, full= F)
  
  #### Species_Link ####
  # read in existing table
  ### Query for any links of non sensitive species and species that dont have any links yet
  # SpeciesLinks <- REGS_Species %>% filter(!SpeciesID %in% REG_Species_Link$SpeciesID,!Sensitive)
  # #REGS_Species_Link <- getSpeciesLinks(SpeciesLinks)
  # 
  # # Create new full table in case systems have been added or removed from a site
  # New_Species_Link <- REG_Species_Link %>% 
  #   filter(SpeciesID %!in% REGS_Species_Link$SpeciesID) %>% 
  #   bind_rows(REGS_Species_Link) %>%
  #   arrange(SpeciesID) %>%
  #   mutate(SpeciesLinkID=if(n()>0) 1:n() else 0)
  # 
  # # update table - full update
  # registryDB %>% update.table("Species_Link","SpeciesLinkID",New_Species_Link,REG_Species_Link,full = T)
  # # Remove data to free up any memory
  # rm(REG_Species_Link,REGS_Species_Link,New_Species_Link,SpeciesLinks)
  
  
  #### Species_Citation ####
  
  # Create new full table in case systems have been added or removed from a site
  New_Species_Citation <- REG_Species_Citation %>% 
    filter(SpeciesID %!in% REGS_Species_Citation$SpeciesID) %>% 
    bind_rows(REGS_Species_Citation) %>%
    arrange(SpeciesID,LongCitation) %>%
    mutate(SpeciesCitationID=if(n()>0) 1:n() else 0)
  
  # update table - full update
  registryDB %>% update.table("Species_Citation","SpeciesCitationID",New_Species_Citation,REG_Species_Citation,full = T)
  # Remove data to free up any memory
  rm(REG_Species_Citation,REGS_Species_Citation,New_Species_Citation)
  
  ### Do internal boundary and Footnote
  
  registryDB %>% update.table("InternalBoundary","InternalBoundaryID",REGS_InternalBoundary,REG_InternalBoundary)
 # registryDB %>% update.table("Footnote","FootnoteID",REGS_Footnote,REG_Footnote)
  
  rm(REGS_InternalBoundary,REG_InternalBoundary,REGS_Footnote,REG_Footnote)
  #### KBA_SpeciesAssessments &  SpeciesAssessment_Subcriterion  ####
  
  # Do left join on SpeciesAssessment_Subcriterion
  REG_SpeciesAssessment <- REG_SpeciesAssessment_Subcriterion %>% left_join(REG_KBA_SpeciesAssessments, by="SpeciesAssessmentsID")
  REGS_SpeciesAssessment <- REGS_SpeciesAssessment_Subcriterion %>% left_join(REGS_KBA_SpeciesAssessments, by="SpeciesAssessmentsID")
  # Rebuild tables
  New_SpeciesAssessment <- REG_SpeciesAssessment %>% 
    filter(SiteID %!in% REGS_SpeciesAssessment$SiteID) %>%
    bind_rows(REGS_SpeciesAssessment) %>% 
    arrange(SiteID,SpeciesID) %>% 
    mutate(AssessmentSubcriterionID=if(n()>0) 1:n() else 0)%>% 
    group_by(SiteID,SpeciesID,SpeciesStatus,DateAssessed,PercentAtSite,SeasonalDistribution,MinSitePopulation,BestSitePopulation,MaxSitePopulation,SiteDerivation,MinRefPopulation,BestRefPopulation,MaxRefPopulation,SitePopulationSources,RefPopulationSources,AssessmentParameterID,MinReproductiveUnits,RUType,RUSources,FootnoteID,InternalBoundaryID) %>% mutate(SpeciesAssessmentsID=if(n()>0) cur_group_id() else 0) %>% ungroup()
  
  
  New_SpeciesAssessment_Subcriterion <- New_SpeciesAssessment %>% select(all_of(names(REG_SpeciesAssessment_Subcriterion)))
  
  New_KBA_SpeciesAssessments <- New_SpeciesAssessment %>% select(all_of(names(REG_KBA_SpeciesAssessments))) %>% distinct()
  
  
  #### Check for missing AssessmentIDs to remove those first
  missingIDs <- REG_KBA_SpeciesAssessments %>% 
    filter(SpeciesAssessmentsID %!in% New_KBA_SpeciesAssessments$SpeciesAssessmentsID) %>%
    pull(SpeciesAssessmentsID)
  if(length(missingIDs)>0){
    registryDB %>% delete.id("SpeciesAssessment_Subcriterion","SpeciesAssessmentsID",missingIDs)
  }
  
  ### Update tables
  registryDB %>% update.table("KBA_SpeciesAssessments","SpeciesAssessmentsID",New_KBA_SpeciesAssessments,REG_KBA_SpeciesAssessments)
  
  registryDB %>% update.table("SpeciesAssessment_Subcriterion","AssessmentSubcriterionID",New_SpeciesAssessment_Subcriterion, REG_SpeciesAssessment_Subcriterion)
  
  rm(missingIDs,New_KBA_SpeciesAssessments,REG_KBA_SpeciesAssessments,New_SpeciesAssessment_Subcriterion, REG_SpeciesAssessment_Subcriterion,New_SpeciesAssessment,REG_SpeciesAssessment,REGS_SpeciesAssessment,REGS_SpeciesAssessment_Subcriterion,REGS_KBA_SpeciesAssessments)
  
  
  #### Ecosystem ####
  
  # update table 
  registryDB %>% update.table("Ecosystem","EcosystemID",REGS_Ecosystem,REG_Ecosystem, full= F)
  
  #### Ecosystem_Citation ####
  
  # Create new full table in case systems have been added or removed from a site
  New_Ecosystem_Citation <- REG_Ecosystem_Citation %>% 
    filter(EcosystemID %!in% REGS_Ecosystem_Citation$EcosystemID) %>% 
    bind_rows(REGS_Ecosystem_Citation) %>%
    arrange(EcosystemID,LongCitation) %>%
    mutate(EcosystemCitationID=if(n()>0) 1:n() else 0)
  
  # update table - full update
  registryDB %>% update.table("Ecosystem_Citation","EcosystemCitationID",New_Ecosystem_Citation,REG_Ecosystem_Citation,full = T)
  # Remove data to free up any memory
  rm(REG_Ecosystem_Citation,REGS_Ecosystem_Citation,New_Ecosystem_Citation)
  
  
  #### KBA_EcosystemAssessments &  EcosystemAssessment_Subcriterion  ####
  
  # Do left join on SpeciesAssessment_Subcriterion
  REG_EcosystemAssessment <- REG_EcosystemAssessment_Subcriterion %>% left_join(REG_KBA_EcosystemAssessments, by="EcosystemAssessmentsID")
  REGS_EcosystemAssessment <- REGS_EcosystemAssessment_Subcriterion %>% left_join(REGS_KBA_EcosystemAssessments, by="EcosystemAssessmentsID")
  # Rebuild tables
  New_EcosystemAssessment <- REG_EcosystemAssessment %>% 
    filter(SiteID %!in% REGS_EcosystemAssessment$SiteID) %>%
    bind_rows(REGS_EcosystemAssessment) %>% 
    arrange(SiteID,EcosystemID) %>% 
    mutate(EcoAssessmentSubcriterionID=if(n()>0) 1:n() else 0) %>% 
    group_by(SiteID, EcosystemID, DateAssessed, EcosystemStatus, PercentAtSite, MinSiteExtent, BestSiteExtent, MaxSiteExtent, SiteExtentSources, MinRefExtent, BestRefExtent, MaxRefExtent, RefExtentSources, FootnoteID, InternalBoundaryID) %>% mutate(EcosystemAssessmentsID=if(n()>0) cur_group_id() else 0) %>% ungroup()
  
  
  New_EcosystemAssessment_Subcriterion <- New_EcosystemAssessment %>% select(all_of(names(REG_EcosystemAssessment_Subcriterion)))
  
  New_KBA_EcosystemAssessments <- New_EcosystemAssessment %>% select(all_of(names(REG_KBA_EcosystemAssessments))) %>% distinct()
  
  
  #### Check for missing AssessmentIDs to remove those first
  missingIDs <- REG_KBA_EcosystemAssessments %>% 
    filter(EcosystemAssessmentsID %!in% New_KBA_EcosystemAssessments$EcosystemAssessmentsID) %>%
    pull(EcosystemAssessmentsID)
  if(length(missingIDs)>0){
    registryDB %>% delete.id("EcosystemAssessment_Subcriterion","EcosystemAssessmentsID",missingIDs)
  }
  
  ### Update tables
  registryDB %>% update.table("KBA_EcosystemAssessments","EcosystemAssessmentsID",New_KBA_EcosystemAssessments,REG_KBA_EcosystemAssessments)
  
  registryDB %>% update.table("EcosystemAssessment_Subcriterion","EcoAssessmentSubcriterionID", New_EcosystemAssessment_Subcriterion, REG_EcosystemAssessment_Subcriterion)
  
  rm(missingIDs,New_KBA_EcosystemAssessments,REG_KBA_EcosystemAssessments,New_EcosystemAssessment_Subcriterion, REG_EcosystemAssessment_Subcriterion,New_EcosystemAssessment,REG_EcosystemAssessment,REGS_EcosystemAssessment,REGS_EcosystemAssessment_Subcriterion,REGS_KBA_EcosystemAssessments)
  
  registryDB %>% dbCommit()
  
  # Print message
  print(DBS_KBASite$nationalname)
  
  # End of tryCatch call
  }, error=function(e){
    registryDB %>% dbRollback() ### rollback site on error
    message(paste(DBS_KBASite$nationalname, "KBA not processed."))
    # Store error info
    siteErrors <<- rbind(siteErrors,data.frame(site=DBS_KBASite$nationalname,
                                          sitecode=DBS_KBASite$sitecode,
                                          error=e[["message"]]))
    message(e)

  })
  
  # Remove variables
  rm(list=setdiff(ls(), c(ls(pattern = "DB_"), ls(pattern = "REG_"), ls(pattern = "REGA_"), "id", "lastPipelineRun", "relevantReferenceEstimates_spp", "relevantReferenceEstimates_eco", "sensitiveSpecies", "maxSensitiveSpeciesID", "read_KBACanadaProposalForm", "read_KBAEBARDatabase", "filter_KBAEBARDatabase", "check_KBADataValidity", "trim_KBAEBARDataset", "update_KBAEBARDataset", "primaryKey_KBAEBARDataset")))
}
rm(id)

#### DELETIONS - Delete sites, species, and ecosystems ####
# Sites
      # Sites to retain - TO DO: Update once we've decided how to handle version
KBASite_retain <- DB_KBASite %>%
  st_drop_geometry() %>%
  filter(sitestatus >= 6) %>%
  select(sitecode, version)

cleanuperror <- c()

tryCatch({
  # code for deleting sites
  REG_KBA_Site <- registryDB %>% read_sf("KBA_Site") ### Most recent version
  deletesitecodes <- REG_KBA_Site %>% 
    filter(SiteCode %!in% KBASite_retain$sitecode) %>% 
    pull(SiteCode)
  
  registryDB %>% dbBegin() ### Start transaction
  registryDB %>% delete.sites(deletesitecodes)
  
  # Clean up records 
  db %>% cleanup.internalboundary()
  db %>% cleanup.footnote()
  db %>% cleanup.species()
  db %>% cleanup.ecosystems()
  
  registryDB %>% dbCommit() ### Commit changes if no errors
  
},error=function(e){
  registryDB %>% dbRollback() #### rollback error
  #store error
  cleanuperror <<- c(e[["message"]])
  
})

### Send error email
if(nrow(siteErrors)>0 | length(cleanuperror) >0){
  errornumber <- nrow(siteErrors) + length(cleanuperror)
  errortext <- if(nrow(siteErrors)>0 ){paste0("<li>",siteErrors$site," (",siteErrors$sitecode,"): ",siteErrors$error,"</li>",collapse = "")} else {""}
  errortext <- paste0(errortext,if(length(cleanuperror) >0){paste0("<li>Cleanup Error: ",cleanuperror,"</li>",collapse = "")}else{""})
  body <- paste0("KBA Pipeline run completed with ",errornumber," errors. Errors are as follows: <ul>",errortext,"</ul>")
  pipline.email(to=c("devans@birdscanada.org","cdebyser@wcs.org"),
                password = mailtrap_pass,
                message = body)
  
} else {
  pipline.email(to=c("devans@birdscanada.org","cdebyser@wcs.org"),
                password = mailtrap_pass,
                message = "KBA Pipeline run completed with no errors!")
  #### Add write to RDS to save last pipeline run
}


# TO DO: Send completion email to Sandra, Amanda, etc.

