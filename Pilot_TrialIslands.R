#### Identification of Canadian KBAs
#### Wildlife Conservation Society - 2021
#### Script by Chloé Debyser

#### 11. Global KBA Form to KBA-EBAR - Trial Islands Pilot

#### Workspace ####
# Packages
library(openxlsx)
library(tidyverse)
library(magrittr)
library(sf)
library(arcgisbinding)
arc.check_product()
library(stringi)

# Portal URL
url <- "https://gis.natureserve.ca/arcgis/rest/services/"

# Input parameters
      # Site name
siteName <- "Trial Islands"

      # Site path
sitePath <- "C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/Test workspace/Outputs_PilotBatch2/KBACanadaProposal_TrialIslands_Canada.xlsm"

      # Site code
siteCode <- 1

# Data
      # WCSC-BC crosswalk
crosswalk <- read.xlsx("G:/My Drive/KBA Canada Team/1. Source Datasets/Wildlife Conservation Society Canada/BirdsCanada-WCSC_DatabaseCrosswalk.xlsx")
Layer_BC <- crosswalk[2:nrow(crosswalk), 2]
Layer_WCSC <- crosswalk[2:nrow(crosswalk), 8]
Name_BC <- crosswalk[2:nrow(crosswalk), 3]
Name_WCSC <- crosswalk[2:nrow(crosswalk), 9]
crosswalk <- data.frame(Layer_BC = Layer_BC, Name_BC = Name_BC, Layer_WCSC = Layer_WCSC, Name_WCSC = Name_WCSC) %>%
  drop_na()
  
      # Proposal form sheets
proposer2 <- read.xlsx(sitePath, sheet="1. PROPOSER")
site2 <- read.xlsx(sitePath, sheet="2. SITE")
species3 <- read.xlsx(sitePath, sheet="3. SPECIES")
ecosystems4 <- read.xlsx(sitePath, sheet="4. ECOSYSTEMS & C")
threats5 <- read.xlsx(sitePath, sheet="5. THREATS")
review6 <- read.xlsx(sitePath, sheet="6. REVIEW")
citations7 <- read.xlsx(sitePath, sheet="7. CITATIONS")
checkboxes <- read.xlsx(sitePath, sheet="checkboxes")

      # EBAR-KBA database
arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/0')) # This line is only there because it stops R from aborting, for some reason
KBASite <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/0')) %>%
  arc.select(., where_clause=paste0("nationalname = '", siteName, "'")) %>%
  arc.data2sp() %>%
  st_as_sf()

KBASiteID <- KBASite$kbasiteid

speciesAtSite <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/7')) %>%
  arc.select(., where_clause=paste0("kbasiteid = ", KBASiteID))

speciesAtSiteIDs <- speciesAtSite$speciesatsiteid

kbaInputPolygon <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/6')) %>%
  arc.select(., where_clause=paste0("speciesatsiteid IN (", paste(speciesAtSiteIDs, collapse=", "), ")"))

kbaCustomPolygon <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/1')) %>%
  arc.select(., where_clause=paste0("speciesatsiteid IN (", paste(speciesAtSiteIDs, collapse=", "), ")"))

if(nrow(kbaCustomPolygon) > 0){
  kbaCustomPolygon %<>%
    arc.data2sp() %>%
    st_as_sf()
}

      # Master species list
species <- read.xlsx("G:/My Drive/KBA Canada Team/2. Formatted Datasets - Tabular/Ref_Species.xlsx", sheet=2)

#### Format Data - For KBA-EBAR database ####
# KBASite
      # Columns needed
KBASite_colsNeeded <- c("SiteCode", "KBASiteID", "WDKBAID", "NationalName", "InternationalName", "Jurisdiction", "KBALevel", "GlobalCriteria", "NationalCriteria", "Proposer", "ProposerEmail", "SiteStatus", "StatusChangeDate", "DelineationStatus", "DelineationNotes", "DelineationNextSteps", "SiteDescription_EN", "SiteDescription_FR", "AdditionalBiodiversity_EN", "AdditionalBiodiversity_FR", "SiteManagement_EN", "SiteManagement_FR", "NominationRationale_EN", "NominationRationale_FR", "DelineationRationale_EN", "DelineationRationale_FR", "CustomaryJurisdiction_EN", "CustomaryJurisdiction_FR", "ProtectedAreas_EN", "ProtectedAreas_FR", "PercentProtected", "OECMsAtSite_EN", "OECMsAtSite_FR", "Latitude", "Longitude", "AltitudeMin", "AltitudeMax", "Systems", "Area", "geometry")

      # Columns present
KBASite_colsPresent <- tolower(KBASite_colsNeeded) %>%
  .[which(. %in% colnames(KBASite))]

      # Columns missing
KBASite_colsMissing <- KBASite_colsNeeded[which(!tolower(KBASite_colsNeeded) %in% colnames(KBASite))]

      # Organize columns
KBASite %<>% select(all_of(KBASite_colsPresent))
colnames(KBASite) <- KBASite_colsNeeded[which(tolower(KBASite_colsNeeded) %in% colnames(KBASite))]
extraColumns <- matrix(NA_real_, nrow=nrow(KBASite), ncol=length(KBASite_colsMissing), dimnames=list(NULL, KBASite_colsMissing))
KBASite <- bind_cols(KBASite, as.data.frame(extraColumns))
KBASite %<>% select(all_of(KBASite_colsNeeded))

      # Populate
KBASite %<>% mutate(SiteCode = siteCode,
                    InternationalName = siteInformation[4,2],
                    Jurisdiction = siteInformation[6,2],
                    KBALevel = "Global",
                    StatusChangeDate = "2020-07-01",
                    SiteDescription_EN = siteInformation[14,2],
                    AdditionalBiodiversity_EN = siteInformation[23,2],
                    CustomaryJurisdiction_EN = siteInformation[24,2],
                    ProtectedAreas_EN = "Victoria Harbour Migratory Bird Sanctuary and Trial Islands Ecological Reserve",
                    PercentProtected = siteInformation[19,2],
                    Latitude = siteInformation[8,2],
                    Longitude = siteInformation[9,2],
                    AltitudeMin = siteInformation[10,2],
                    AltitudeMax = siteInformation[11,2],
                    Systems = paste(trimws(siteInformation[12,2:5][!is.na(siteInformation[12,2:5])]), collapse="; "),
                    Area = siteInformation[7,2])

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/KBASite", KBASite, overwrite=T)

# KBAHabitat
# TO DO: Implement logic for when the habitat information is blank in the proposal form (then KBAHabitat will be empty too)

      # Get habitat information from the proposal form
KBAHabitat <- data.frame(Habitat = paste(siteInformation[21, 2:5]),
                         PercentCover = paste(siteInformation[22, 2:5]))

      # Format
KBAHabitat %<>% 
  mutate(KBASiteID = KBASite$KBASiteID) %>%
  select(KBASiteID, Habitat, PercentCover)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/KBAHabitat", KBAHabitat, overwrite = T)

# KBAThreat
      # Get information from the proposal form
KBAThreat <- threats[6:13,]

      # Assign column names
colnames(KBAThreat) <- c("Applicability", "Level1", "Level2", "Level3", "Timing", "Scope", "Severity", "Notes")

      # Populate missing columns
KBAThreat %<>%
  mutate(Category = ifelse(Applicability == "All species listed in Sheet 5",
                           "Entire site",
                           "Species")) %>%
  mutate(SpeciesID = sapply(1:nrow(.), function(x) ifelse(Category[x] == "Species",
                                                          species$SpeciesID[which(species$NATIONAL_SCIENTIFIC_NAME == Applicability[x])],
                                                          NA))) %>%
  mutate(EcosystemID = NA,
         KBASiteID = KBASite$KBASiteID,
         KBAThreatID = 1:nrow(.)) %>%
  select(KBAThreatID, KBASiteID, Category, SpeciesID, EcosystemID, Level1, Level2, Level3, Timing, Scope, Severity, Notes)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/KBAThreat", KBAThreat, overwrite = T)

# KBAAction
      # Get information from the proposal form
KBAAction_ongoing <- data.frame(ConservationAction = paste(siteInformation[15, 2:5])) %>%
  mutate(OngoingOrNeeded = "Ongoing") %>%
  mutate(ConservationAction = ifelse(ConservationAction == "NA", NA, ConservationAction)) %>%
  drop_na
  
KBAAction_needed <- data.frame(ConservationAction = paste(siteInformation[16, 2:5])) %>%
  mutate(OngoingOrNeeded = "Needed") %>%
  mutate(ConservationAction = ifelse(ConservationAction == "NA", NA, ConservationAction)) %>%
  drop_na

      # Bind rows
if((nrow(KBAAction_ongoing) > 0) & (nrow(KBAAction_needed))){
  KBAAction <- bind_rows(KBAAction_ongoing, KBAAction_needed)
}else if(nrow(KBAAction_ongoing) > 0){
  KBAAction <- KBAAction_ongoing
}else{
  KBAAction <- KBAAction_needed
}
rm(KBAAction_ongoing, KBAAction_needed)

      # Add missing columns
KBAAction %<>%
  mutate(KBAActionID = 1:nrow(.),
         KBASiteID = KBASite$KBASiteID) %>%
  relocate(KBAActionID, KBASiteID, before=OngoingOrNeeded)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/KBAAction", KBAAction, overwrite = T)

# SiteCitations
# TO DO: Implement

# SpeciesAtSite
      # Filter only species that should be shared
SpeciesAtSite <- speciesAtSite %>%
  filter(meetscriteria == "Y")
# TO DO: Implement filter based on PresentAtSite, species sensitivity, etc.

      # Format
SpeciesAtSite %<>% select(speciesatsiteid, kbasiteid, speciesid) %>%
  rename(SpeciesAtSiteID = speciesatsiteid,
         KBASiteID = kbasiteid,
         SpeciesID = speciesid) %>%
  left_join(., species[,c("SpeciesID", "NATIONAL_SCIENTIFIC_NAME")]) %>%
  left_join(., criteriaBySpecies) %>%
  mutate(A1a = ifelse(A1a == 1, "A1a", NA),
         A1b = ifelse(A1b == 1, "A1b", NA),
         A1c = ifelse(A1c == 1, "A1c", NA),
         A1d = ifelse(A1d == 1, "A1d", NA),
         A1e = ifelse(A1e == 1, "A1e", NA),
         B1 = ifelse(B1 == 1, "B1", NA),
         D1a = ifelse(D1a == 1, "D1a", NA),
         D1b = ifelse(D1b == 1, "D1b", NA),
         D2 = ifelse(D2 == 1, "D2", NA),
         D3 = ifelse(D3 == 1, "D3", NA)) %>%
  mutate(GlobalCriteria = sapply(1:nrow(.), function(x) paste(c(A1a[x], A1b[x], A1c[x], A1d[x], A1e[x], B1[x], D1a[x], D1b[x], D2[x], D3[x])[which(!is.na(c(A1a[x], A1b[x], A1c[x], A1d[x], A1e[x], B1[x], D1a[x], D1b[x], D2[x], D3[x])))], collapse='; ')),
         NationalCriteria = NA) %>%
  select(SpeciesAtSiteID, KBASiteID, SpeciesID, GlobalCriteria, NationalCriteria)
  
      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/SpeciesAtSite", SpeciesAtSite, overwrite=T)

# SpeciesAssessment
      # Get the data from the proposal form
SpeciesAssessment <- left_join(speciesSite, speciesGlobal, by=c("Species", "Scientific name", "Assessment parameter"))

      # Add SpeciesAtSiteID
SpeciesAssessment %<>%
  left_join(., species[,c("SpeciesID", "NATIONAL_SCIENTIFIC_NAME")], by=c("Scientific name" = "NATIONAL_SCIENTIFIC_NAME")) %>%
  mutate(SpeciesAtSiteID = sapply(SpeciesID, function(x) ifelse(length(SpeciesAtSite$SpeciesAtSiteID[which(SpeciesAtSite$SpeciesID==x)])==0, NA, SpeciesAtSite$SpeciesAtSiteID[which(SpeciesAtSite$SpeciesID==x)])))

      # Remove species that don't meet criteria
SpeciesAssessment %<>% filter(!is.na(SpeciesAtSiteID))

      # Format
SpeciesAssessment %<>%
  mutate(SpeciesAssessmentID = 1:nrow(.)) %>%
  relocate(SpeciesAssessmentID, SpeciesAtSiteID, before = Species) %>%
  select(-c(SpeciesID, Species, "Scientific name"))

#### Format Data - For Birds Canada database ####
# KBA_Status
      # Create
KBA_Status <- data.frame(Status_EN = KBASite$KBALevel) %>%
  mutate(Status_FR = NA,
         StatusID = 1:nrow(.)) %>%
  relocate(StatusID, before = Status_EN)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_Status", KBA_Status)

# KBA_Province
      # Create
KBA_Province <- data.frame(Province_EN = KBASite$Jurisdiction) %>%
  mutate(Province_FR = NA,
         Abbreviation = NA,
         ProvinceID = 1:nrow(.)) %>%
  relocate(ProvinceID, before=Province_EN)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_Province", KBA_Province)

# KBA_ProtectedArea
      # Create
KBA_ProtectedArea <- KBASite %>%
  st_drop_geometry() %>%
  select(SiteCode, ProtectedAreas_EN, ProtectedAreas_FR, PercentProtected) %>%
  mutate(ProtectedAreaID = 1:nrow(.),
         IUCNCat = NA) %>%
  rename(SiteID = SiteCode,
         PercentCover = PercentProtected,
         ProtectedArea_EN = ProtectedAreas_EN,
         ProtectedArea_FR = ProtectedAreas_FR) %>%
  select(ProtectedAreaID, SiteID, ProtectedArea_EN, ProtectedArea_FR, IUCNCat, PercentCover)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_ProtectedArea", KBA_ProtectedArea)

# KBA_Site and KBA_Website
      # Subset the data, to grab only information that can be shared
# TO DO: Implement this filtering using SiteStatus, etc.

      # Columns to share
KBA_Site_cols <- c("SiteCode", "WDKBAID", "NationalName", "KBALevel", "StatusChangeDate", "Jurisdiction", "SiteDescription_EN", "SiteDescription_FR", "AdditionalBiodiversity_EN", "AdditionalBiodiversity_FR", "CustomaryJurisdiction_EN", "CustomaryJurisdiction_FR", "ProtectedAreas_EN", "ProtectedAreas_FR", "PercentProtected", "OECMsAtSite_EN", "OECMsAtSite_FR", "Latitude", "Longitude", "AltitudeMin", "AltitudeMax", "Area", "geometry")

      # Select columns
KBA_Site <- KBASite %>%
  select(all_of(KBA_Site_cols))

      # Rename columns
KBA_Site_cols_newName <- crosswalk %>%
  filter(Name_WCSC %in% KBA_Site_cols) %>%
  arrange(factor(Name_WCSC, levels = KBA_Site_cols)) %>%
  pull(Name_BC)
colnames(KBA_Site) <- KBA_Site_cols_newName

      # Add missing columns
KBA_Site %<>%
  mutate(StatusID = KBA_Status$StatusID[which(KBA_Status$Status_EN == Status_EN)]) %>%
  select(-Status_EN) %>%
  mutate(ProvinceID = KBA_Province$ProvinceID[which(KBA_Province$Province_EN == Province_EN)]) %>%
  select(-Province_EN) %>%
  mutate(SiteCode = NA,
         Name_FR = NA)

      # Create KBA_Website, and remove corresponding columns from KBA_Site
KBA_Website <- KBA_Site %>%
  st_drop_geometry() %>%
  select(SiteID, SiteDescription_EN, SiteDescription_FR, BiodiversitySummary_EN, BiodiversitySummary_FR, CustomaryJurisdiction_EN, CustomaryJurisdiction_FR) %>%
  mutate(Conservation_EN = NA,
         Conservation_FR = NA) %>%
  relocate(c(Conservation_EN, Conservation_FR), .before=CustomaryJurisdiction_EN)
KBA_Site %<>% select(SiteID, SiteCode, WDKBAID, StatusID, Name_EN, Name_FR, ProvinceID, Latitude, Longitude, AltMin, AltMax, Area, Assessed, geometry)

      # Save both
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_Site", KBA_Site)
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_Website", KBA_Website)

# System
      # Create
System <- data.frame(Type_EN = unlist(strsplit(KBASite$Systems, "; "))) %>%
  mutate(Type_FR = NA) %>%
  mutate(SystemID = 1:nrow(.)) %>%
  relocate(SystemID, before=Type_EN)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/System", System)

# Habitat
      # Create
Habitat <- data.frame(Habitat_EN = KBAHabitat$Habitat) %>%
  mutate(Habitat_FR = NA,
         Description = NA,
         HabitatCode = NA,
         HabitatID = 1:nrow(.)) %>%
  mutate(SystemID = sapply(Habitat_EN, function(x) ifelse(x %in% c("Grassland", "Shrubland", "Savanna"),
                                                          System$SystemID[which(System$Type_EN == "Terrestrial")],
                                                          NA))) %>%
  relocate(HabitatID, SystemID, HabitatCode, before=Habitat_EN)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/Habitat", Habitat)

# KBA_Habitat
      # Create
KBA_Habitat <- KBAHabitat %>%
  mutate(HabitatSiteID = 1:nrow(.)) %>%
  mutate(SiteID = siteCode) %>%
  rename(Habitat_EN = Habitat) %>%
  mutate(HabitatID = sapply(Habitat_EN, function(x) Habitat$HabitatID[which(Habitat$Habitat_EN == x)])) %>%
  mutate(PercentCoverMin = sapply(PercentCover, function(x) substr(x, 1, stri_locate_all(pattern = '-', x, fixed = TRUE)[[1]][1,1]-1))) %>%
  mutate(PercentCoverMin = as.integer(PercentCoverMin)) %>%
  mutate(PercentCoverMax = sapply(PercentCover, function(x) substr(x, stri_locate_all(pattern = '-', x, fixed = TRUE)[[1]][1,1]+1, nchar(x)-1))) %>%
  mutate(PercentCoverMax = as.integer(PercentCoverMax)) %>%
  select(HabitatSiteID, SiteID, HabitatID, PercentCoverMin, PercentCoverMax)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_Habitat", KBA_Habitat)

# Threats
      # Create
Threats <- KBAThreat %>%
  mutate(LastLevel = sapply(1:nrow(KBAThreat), function(x) sum(!is.na(KBAThreat[x, c("Level1", "Level2", "Level3")])))) %>%
  mutate(Threat_EN = ifelse(LastLevel == 1,
                            Level1,
                            ifelse(LastLevel == 2,
                                   Level2,
                                   Level3))) %>%
  select(Threat_EN) %>%
  distinct() %>%
  mutate(Threat_FR = NA,
         ThreatCode = sapply(Threat_EN, function(x) substr(x, 1, stri_locate_all(pattern=" ", x, fixed=T)[[1]][1,1]-1)),
         Threat_EN = sapply(Threat_EN, function(x) substr(x, stri_locate_all(pattern=" ", x, fixed=T)[[1]][1,1]+1, nchar(x))),
         ThreatID = 1:nrow(.)) %>%
  select(ThreatID, ThreatCode, Threat_EN, Threat_FR)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/Threats", Threats)

# KBA_Threats
      # Create
KBA_Threats <- KBAThreat %>%
  mutate(LastLevel = sapply(1:nrow(KBAThreat), function(x) sum(!is.na(KBAThreat[x, c("Level1", "Level2", "Level3")])))) %>%
  mutate(Threat_EN = ifelse(LastLevel == 1,
                            Level1,
                            ifelse(LastLevel == 2,
                                   Level2,
                                   Level3))) %>%
  mutate(ThreatCode = sapply(Threat_EN, function(x) substr(x, 1, stri_locate_all(pattern=" ", x, fixed=T)[[1]][1,1]-1))) %>%
  select(ThreatCode, Timing, Scope, Severity) %>%
  distinct() %>%
  mutate(ThreatID = Threats$ThreatID[which(Threats$ThreatCode == ThreatCode)],
         SiteID = siteCode,
         ThreatsSiteID = 1:nrow(.)) %>%
  select(ThreatsSiteID, SiteID, ThreatID, Timing, Scope, Severity)
  
      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_Threats", KBA_Threats)

# Conservation
      # Create
Conservation <- KBAAction %>%
  select(ConservationAction) %>%
  distinct() %>%
  mutate(ConservationCode = sapply(ConservationAction, function(x) substr(x, 1, stri_locate_all(pattern = ' ', x, fixed = TRUE)[[1]][1,1]-1)),
         ConservationAction_EN = sapply(ConservationAction, function(x) substr(x, stri_locate_all(pattern = ' ', x, fixed = TRUE)[[1]][1,1]+1, nchar(x))),
         ConservationAction_FR = NA,
         ConservationID = 1:nrow(.)) %>%
  select(ConservationID, ConservationCode, ConservationAction_EN, ConservationAction_FR)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/Conservation", Conservation)

# KBA_Conservation
      # Create
KBA_Conservation <- KBAAction %>%
  select(OngoingOrNeeded, ConservationAction) %>%
  mutate(ConservationID = Conservation$ConservationID[which(paste(Conservation$ConservationCode, Conservation$ConservationAction_EN, sep=" ") == ConservationAction)],
         SiteID = siteCode,
         ConservationSiteID = 1:nrow(.)) %>%
  rename(Ongoing_Needed = OngoingOrNeeded) %>%
  select(ConservationSiteID, SiteID, ConservationID, Ongoing_Needed)

      # Save
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/BirdsCanada_TrialIslands.gdb/KBA_Conservation", KBA_Conservation)

