#### Identification of Canadian KBAs
#### Wildlife Conservation Society - 2021
#### Script by Chloé Debyser

#### Canada KBA Form to KBA-EBAR database - Trial Islands & Yukon Pilot

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
inputDir <- "G:/My Drive/KBA Canada Team/4. KBA Site Proposals - Materials/Sites converted to KBA Canada Form/"
sitePaths <- c("Batch 1/Yukon/KBACanadaProposal_BeaverCreek_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_CarcrossDunes_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_DezadeashLake_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_KluaneNationalParkandReserveDezadeash-KaskawulshConfluence_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_KoidernMountain_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_KusawaTerritorialParkTakhiniRiver_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_LittleTeslinLake_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_Squanga,SeaforthandTeenahLakes_Canada.xlsm",
               "Batch 1/Yukon/KBACanadaProposal_WellesleyPeak_Canada.xlsm",
               "Batch 2/KBACanadaProposal_TrialIslands_Canada.xlsm")

# Data
      # WCSC-BC crosswalk
crosswalk <- read.xlsx("G:/My Drive/KBA Canada Team/1. Source Datasets/Wildlife Conservation Society Canada/BirdsCanada-WCSC_DatabaseCrosswalk.xlsx")
Layer_BC <- crosswalk[2:nrow(crosswalk), 2]
Layer_WCSC <- crosswalk[2:nrow(crosswalk), 8]
Name_BC <- crosswalk[2:nrow(crosswalk), 3]
Name_WCSC <- crosswalk[2:nrow(crosswalk), 9]
crosswalk <- data.frame(Layer_BC = Layer_BC, Name_BC = Name_BC, Layer_WCSC = Layer_WCSC, Name_WCSC = Name_WCSC) %>%
  filter_all(any_vars(!is.na(.)))
  
      # Master species list
species <- read.xlsx("G:/My Drive/KBA Canada Team/2. Formatted Datasets - Tabular/Ref_Species.xlsx", sheet=2)

      # EBAR-KBA Database
arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/0')) # This line is only there because it stops R from aborting, for some reason

# Remove the output geodatabase, to start fresh
arc.delete(dirname("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBA-EBAR_Pilot.gdb/KBASite"))

#### Canada KBA Form to KBA-EBAR database ####
for(sitePath in sitePaths){
  
  # ** LOAD KEY INFORMATION **
  # Load proposal form sheets
  home <- read.xlsx(paste0(inputDir, sitePath), sheet="HOME")
  proposer1 <- read.xlsx(paste0(inputDir, sitePath), sheet="1. PROPOSER") %>% .[,2:3] %>% rename(Field = X2, Entry = X3)
  site2 <- read.xlsx(paste0(inputDir, sitePath), sheet="2. SITE") %>% .[,2:4] %>% rename(Field = X2)
  species3 <- read.xlsx(paste0(inputDir, sitePath), sheet="3. SPECIES")
  ecosystems4 <- read.xlsx(paste0(inputDir, sitePath), sheet="4. ECOSYSTEMS & C")
  threats5 <- read.xlsx(paste0(inputDir, sitePath), sheet="5. THREATS")
  review6 <- read.xlsx(paste0(inputDir, sitePath), sheet="6. REVIEW")
  citations7 <- read.xlsx(paste0(inputDir, sitePath), sheet="7. CITATIONS")
  checkboxes <- read.xlsx(paste0(inputDir, sitePath), sheet="checkboxes")
  
  # Site name
  siteName <- site2 %>% filter(Field == "National name") %>% pull(GENERAL)
  
  # Load KBA-EBAR database information
        # KBASite
  KBASite <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/0')) %>%
    arc.select(., where_clause=paste0("nationalname = '", siteName, "'")) %>%
    arc.data2sp() %>%
    st_as_sf()
  
  KBASiteID <- KBASite$kbasiteid
  
        # SpeciesAtSite
  speciesAtSite <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/7')) %>%
    arc.select(., where_clause=paste0("kbasiteid = ", KBASiteID))
  
  speciesAtSiteIDs <- speciesAtSite$speciesatsiteid
  
        # KBAInputPolygon
  kbaInputPolygon <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/6')) %>%
    arc.select(., where_clause=paste0("speciesatsiteid IN (", paste(speciesAtSiteIDs, collapse=", "), ")"))
  
        # KBACustomPolygon
  kbaCustomPolygon <- arc.open(paste0(url, 'EBAR-KBA/KBA/FeatureServer/1')) %>%
    arc.select(., where_clause=paste0("speciesatsiteid IN (", paste(speciesAtSiteIDs, collapse=", "), ")"))
  
  if(nrow(kbaCustomPolygon) > 0){
    kbaCustomPolygon %<>%
      arc.data2sp() %>%
      st_as_sf()
  }
  
        # KBA level
  KBAlevel <- ifelse(grepl("g", home[13,4], T), ifelse(grepl("n", home[13,4], T), "Global and National", "Global"), "National")
  
        # KBA criteria
  KBAcriteria <- str_split(home[13,4], "; ")[[1]]
  
        # Check that proposer email is provided twice
  if(!proposer1$Entry[which(proposer1$Field == "Email")] == proposer1$Entry[which(proposer1$Field == "Email (please re-enter)")]){
    stop("The proposer email is not correctly entered in one of the required fields.")
  }
  
  # ** CONVERT TO KBA-EBAR DATA MODEL **
  # KBASite
        # Columns needed
  KBASite_colsNeeded <- crosswalk %>% filter(Layer_WCSC == "KBASite") %>% pull(Name_WCSC)
  
        # Columns present
  KBASite_colsPresent <- tolower(KBASite_colsNeeded) %>%
    .[which(. %in% colnames(KBASite))]
  
        # Columns missing
  KBASite_colsMissing <- KBASite_colsNeeded[which(!tolower(KBASite_colsNeeded) %in% KBASite_colsPresent)]
  
        # Organize columns
  KBASite %<>% select(all_of(KBASite_colsPresent))
  colnames(KBASite) <- KBASite_colsNeeded[which(tolower(KBASite_colsNeeded) %in% colnames(KBASite))]
  extraColumns <- matrix(NA_real_, nrow=nrow(KBASite), ncol=length(KBASite_colsMissing), dimnames=list(NULL, KBASite_colsMissing))
  KBASite <- bind_cols(KBASite, as.data.frame(extraColumns))
  KBASite %<>% select(all_of(KBASite_colsNeeded))
  
        # Populate
  KBASite %<>% mutate(SiteCode = site2 %>% filter(Field == "Canadian Site Code") %>% pull(GENERAL),
                      WDKBAID = site2 %>% filter(Field == "WDKBA number") %>% pull(GENERAL),
                      InternationalName = site2 %>% filter(Field == "International name") %>% pull(GENERAL),
                      Jurisdiction = site2 %>% filter(Field == "Province or Territory") %>% pull(GENERAL),
                      KBALevel = KBAlevel,
                      SiteStatus = 6,
                      StatusChangeDate = ifelse(NationalName == "Trial Islands", "2020-07-01", "2021-09-15"),
                      GlobalCriteria = ifelse(sum(grepl("g", KBAcriteria))==0, NA, paste0(KBAcriteria[which(grepl("g", KBAcriteria))], collapse="; ")),
                      NationalCriteria = ifelse(sum(grepl("n", KBAcriteria))==0, NA, paste0(KBAcriteria[which(grepl("n", KBAcriteria))], collapse="; ")),
                      Proposer = proposer1 %>% filter(Field == "Name") %>% pull(Entry),
                      ProposerEmail = proposer1 %>% filter(Field == "Email") %>% pull(Entry),
                      ProposerAgreement = proposer1 %>% filter(Field == "I agree to the data in this form being stored in the World Database of KBAs and used for the purposes of KBA identification and conservation.") %>% pull(Entry),
                      SiteDescription_EN = site2 %>% filter(Field == "Site description") %>% pull(GENERAL),
                      SiteDescription_FR = site2 %>% filter(Field == "Site description") %>% pull(FRENCH),
                      AdditionalBiodiversity_EN = site2 %>% filter(Field == "Additional biodiversity") %>% pull(GENERAL),
                      AdditionalBiodiversity_FR = site2 %>% filter(Field == "Additional biodiversity") %>% pull(FRENCH),
                      SiteManagement_EN = site2 %>% filter(Field == "Site management") %>% pull(GENERAL),
                      SiteManagement_FR = site2 %>% filter(Field == "Site management") %>% pull(FRENCH),
                      NominationRationale_EN = site2 %>% filter(Field == "Rationale for nomination") %>% pull(GENERAL),
                      NominationRationale_FR = site2 %>% filter(Field == "Rationale for nomination") %>% pull(FRENCH),
                      DelineationRationale_EN = site2 %>% filter(Field == "Delineation rationale") %>% pull(GENERAL),
                      DelineationRationale_FR = site2 %>% filter(Field == "Delineation rationale") %>% pull(FRENCH),
                      CustomaryJurisdiction_EN = site2 %>% filter(Field == "Customary jurisdiction") %>% pull(GENERAL),
                      CustomaryJurisdiction_FR = site2 %>% filter(Field == "Customary jurisdiction") %>% pull(FRENCH),
                      ProtectedAreas_EN = site2 %>% filter(Field == "Protected area (PA) names") %>% pull(GENERAL),
                      ProtectedAreas_FR = site2 %>% filter(Field == "Protected area (PA) names") %>% pull(FRENCH),
                      PercentProtected = site2 %>% filter(Field == "Percent protected") %>% pull(GENERAL),
                      OECMsAtSite_EN = site2 %>% filter(Field == "OECM(s) at site") %>% pull(GENERAL),
                      OECMsAtSite_FR = site2 %>% filter(Field == "OECM(s) at site") %>% pull(FRENCH),
                      LanduseRegimes_EN = site2 %>% filter(Field == "Land-use regimes") %>% pull(GENERAL),
                      LanduseRegimes_FR = site2 %>% filter(Field == "Land-use regimes") %>% pull(FRENCH),
                      Latitude = site2 %>% filter(Field == "Latitude (dd.dddd)") %>% pull(GENERAL),
                      Longitude = site2 %>% filter(Field == "Longitude (dd.dddd)") %>% pull(GENERAL),
                      AltitudeMin = site2 %>% filter(Field == "Minimum elevation (m)") %>% pull(GENERAL),
                      AltitudeMax = site2 %>% filter(Field == "Maximum elevation (m)") %>% pull(GENERAL),
                      Systems = site2 %>% filter(Field %in% c("Largest", "2nd largest", "3rd largest", "Smallest")) %>% pull(GENERAL) %>% .[which(!is.na(.))] %>% trimws() %>% paste(., collapse="; "),
                      Area = site2 %>% filter(Field == "Site area (km2)") %>% pull(GENERAL))
  
        # Add to previous sites
  if(exists("KBASite_final")){
    KBASite_final <- bind_rows(KBASite_final, KBASite)
      
  }else{
    KBASite_final <- KBASite
  }
  
  # # PICK UP HERE
  # # KBAHabitat
  # # TO DO: Implement logic for when the habitat information is blank in the proposal form (then KBAHabitat will be empty too)
  # 
  # # Get habitat information from the proposal form
  # KBAHabitat <- data.frame(Habitat = paste(siteInformation[21, 2:5]),
  #                          PercentCover = paste(siteInformation[22, 2:5]))
  # 
  # # Format
  # KBAHabitat %<>% 
  #   mutate(KBASiteID = KBASite$KBASiteID) %>%
  #   select(KBASiteID, Habitat, PercentCover)
  # 
  # # Save
  # arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/KBAHabitat", KBAHabitat, overwrite = T)
  # 
  # # KBAThreat
  # # Get information from the proposal form
  # KBAThreat <- threats[6:13,]
  # 
  # # Assign column names
  # colnames(KBAThreat) <- c("Applicability", "Level1", "Level2", "Level3", "Timing", "Scope", "Severity", "Notes")
  # 
  # # Populate missing columns
  # KBAThreat %<>%
  #   mutate(Category = ifelse(Applicability == "All species listed in Sheet 5",
  #                            "Entire site",
  #                            "Species")) %>%
  #   mutate(SpeciesID = sapply(1:nrow(.), function(x) ifelse(Category[x] == "Species",
  #                                                           species$SpeciesID[which(species$NATIONAL_SCIENTIFIC_NAME == Applicability[x])],
  #                                                           NA))) %>%
  #   mutate(EcosystemID = NA,
  #          KBASiteID = KBASite$KBASiteID,
  #          KBAThreatID = 1:nrow(.)) %>%
  #   select(KBAThreatID, KBASiteID, Category, SpeciesID, EcosystemID, Level1, Level2, Level3, Timing, Scope, Severity, Notes)
  # 
  # # Save
  # arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/KBAThreat", KBAThreat, overwrite = T)
  # 
  # # KBAAction
  # # Get information from the proposal form
  # KBAAction_ongoing <- data.frame(ConservationAction = paste(siteInformation[15, 2:5])) %>%
  #   mutate(OngoingOrNeeded = "Ongoing") %>%
  #   mutate(ConservationAction = ifelse(ConservationAction == "NA", NA, ConservationAction)) %>%
  #   drop_na
  # 
  # KBAAction_needed <- data.frame(ConservationAction = paste(siteInformation[16, 2:5])) %>%
  #   mutate(OngoingOrNeeded = "Needed") %>%
  #   mutate(ConservationAction = ifelse(ConservationAction == "NA", NA, ConservationAction)) %>%
  #   drop_na
  # 
  # # Bind rows
  # if((nrow(KBAAction_ongoing) > 0) & (nrow(KBAAction_needed))){
  #   KBAAction <- bind_rows(KBAAction_ongoing, KBAAction_needed)
  # }else if(nrow(KBAAction_ongoing) > 0){
  #   KBAAction <- KBAAction_ongoing
  # }else{
  #   KBAAction <- KBAAction_needed
  # }
  # rm(KBAAction_ongoing, KBAAction_needed)
  # 
  # # Add missing columns
  # KBAAction %<>%
  #   mutate(KBAActionID = 1:nrow(.),
  #          KBASiteID = KBASite$KBASiteID) %>%
  #   relocate(KBAActionID, KBASiteID, before=OngoingOrNeeded)
  # 
  # # Save
  # arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/KBAAction", KBAAction, overwrite = T)
  # 
  # # SiteCitations
  # # TO DO: Implement
  # 
  # # SpeciesAtSite
  # # Filter only species that should be shared
  # SpeciesAtSite <- speciesAtSite %>%
  #   filter(meetscriteria == "Y")
  # # TO DO: Implement filter based on PresentAtSite, species sensitivity, etc.
  # 
  # # Format
  # SpeciesAtSite %<>% select(speciesatsiteid, kbasiteid, speciesid) %>%
  #   rename(SpeciesAtSiteID = speciesatsiteid,
  #          KBASiteID = kbasiteid,
  #          SpeciesID = speciesid) %>%
  #   left_join(., species[,c("SpeciesID", "NATIONAL_SCIENTIFIC_NAME")]) %>%
  #   left_join(., criteriaBySpecies) %>%
  #   mutate(A1a = ifelse(A1a == 1, "A1a", NA),
  #          A1b = ifelse(A1b == 1, "A1b", NA),
  #          A1c = ifelse(A1c == 1, "A1c", NA),
  #          A1d = ifelse(A1d == 1, "A1d", NA),
  #          A1e = ifelse(A1e == 1, "A1e", NA),
  #          B1 = ifelse(B1 == 1, "B1", NA),
  #          D1a = ifelse(D1a == 1, "D1a", NA),
  #          D1b = ifelse(D1b == 1, "D1b", NA),
  #          D2 = ifelse(D2 == 1, "D2", NA),
  #          D3 = ifelse(D3 == 1, "D3", NA)) %>%
  #   mutate(GlobalCriteria = sapply(1:nrow(.), function(x) paste(c(A1a[x], A1b[x], A1c[x], A1d[x], A1e[x], B1[x], D1a[x], D1b[x], D2[x], D3[x])[which(!is.na(c(A1a[x], A1b[x], A1c[x], A1d[x], A1e[x], B1[x], D1a[x], D1b[x], D2[x], D3[x])))], collapse='; ')),
  #          NationalCriteria = NA) %>%
  #   select(SpeciesAtSiteID, KBASiteID, SpeciesID, GlobalCriteria, NationalCriteria)
  # 
  # # Save
  # arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline with Birds Canada/KBA-EBAR_TrialIslands.gdb/SpeciesAtSite", SpeciesAtSite, overwrite=T)
  # 
  # # SpeciesAssessment
  # # Get the data from the proposal form
  # SpeciesAssessment <- left_join(speciesSite, speciesGlobal, by=c("Species", "Scientific name", "Assessment parameter"))
  # 
  # # Add SpeciesAtSiteID
  # SpeciesAssessment %<>%
  #   left_join(., species[,c("SpeciesID", "NATIONAL_SCIENTIFIC_NAME")], by=c("Scientific name" = "NATIONAL_SCIENTIFIC_NAME")) %>%
  #   mutate(SpeciesAtSiteID = sapply(SpeciesID, function(x) ifelse(length(SpeciesAtSite$SpeciesAtSiteID[which(SpeciesAtSite$SpeciesID==x)])==0, NA, SpeciesAtSite$SpeciesAtSiteID[which(SpeciesAtSite$SpeciesID==x)])))
  # 
  # # Remove species that don't meet criteria
  # SpeciesAssessment %<>% filter(!is.na(SpeciesAtSiteID))
  # 
  # # Format
  # SpeciesAssessment %<>%
  #   mutate(SpeciesAssessmentID = 1:nrow(.)) %>%
  #   relocate(SpeciesAssessmentID, SpeciesAtSiteID, before = Species) %>%
  #   select(-c(SpeciesID, Species, "Scientific name"))
}

#### Save ####
# KBASite
arc.write("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBA-EBAR_Pilot.gdb/KBASite", KBASite_final, overwrite=T)
