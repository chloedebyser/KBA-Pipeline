#### Identification of Canadian KBAs
#### Wildlife Conservation Society - 2021-2023
#### Script by Chloé Debyser

#### KBA-EBAR to KBA Registry - Trial Islands & Yukon Pilot
#### URGENT: Ensure that, when Additional Biodiversity is empty, it doesn't add "NA" to the Biodiversity text (this was an issue with Fox Island - see email sent to Dean on May 2nd)
#### Needed: Way to assign unique IDs (e.g. ThreatsSiteID, ConservationSiteID) in incremental way
#### Needed: Way to match new records to previously existing IDs (e.g. for citations - if a given citation was already used in a previous KBA proposal)
#### Needed: Don't send global sites to Registry if they don't have a WDKBAID
#### Needed: Add considerations of species sensitivity
#### Needed: Find substitutes for everything that is hard coded
#### Needed: Add SARA_STATUS_DATE and NSX_URL when available in BIOTICS_ELEMENT_NATIONAL
#### Needed: Implement FootnoteID (right now it is just set to NA)
#### Needed: For species that lack an IUCN/COSEWIC assessment, replace NAs with ID for "Not Assessed" (see updated IUCNStatus and COSEWICStatus lookup tables)
#### Needed: Parse population and subspecies names out of NATIONAL_ENGL_NAME and NATIONAL_FR_NAME (Dean has draft code)

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
inputDB <- "C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBA-EBAR_Pilot_2023.04.gdb"
outputDB <- "C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBARegistry_Pilot_2023.04.gdb"

# Remove the output geodatabase, to start fresh
arc.delete(dirname(paste0(outputDB, "/KBA_Level")))

# Data
      # WCSC-BC crosswalk
crosswalk <- read.xlsx("G:/My Drive/KBA Canada Team/1. Source Datasets/Wildlife Conservation Society Canada/BirdsCanada-WCSC_DatabaseCrosswalk.xlsx")
Layer_BC <- crosswalk[2:nrow(crosswalk), 12]
Layer_WCSC <- crosswalk[2:nrow(crosswalk), 1]
Name_BC <- crosswalk[2:nrow(crosswalk), 13]
Name_WCSC <- crosswalk[2:nrow(crosswalk), 2]
Type_BC <- crosswalk[2:nrow(crosswalk), 14]
Type_WCSC <- crosswalk[2:nrow(crosswalk), 3]
NextSteps_WCSC <- crosswalk[2:nrow(crosswalk), 9]
crosswalk <- data.frame(Layer_BC = Layer_BC, Name_BC = Name_BC, Type_BC = Type_BC, Layer_WCSC = Layer_WCSC, Name_WCSC = Name_WCSC, Type_WCSC = Type_WCSC, NextSteps_WCSC = NextSteps_WCSC) %>%
  filter(is.na(NextSteps_WCSC) | !NextSteps_WCSC == "Remove")

      # List of layers in Input Database
layers <- st_layers(inputDB)[[1]]

      # Input Database
KBASite <- arc.open(paste0(inputDB, "/KBASite")) %>%
  arc.select() %>%
  arc.data2sf()

KBAThreat <- arc.open(paste0(inputDB, "/KBAThreat")) %>%
  arc.select()

KBAAction <- arc.open(paste0(inputDB, "/KBAAction")) %>%
  arc.select()

if("KBALandCover" %in% layers){
  KBALandCover <- arc.open(paste0(inputDB, "/KBALandCover")) %>%
    arc.select()
}

KBACitation <- arc.open(paste0(inputDB, "/KBACitation")) %>%
  arc.select()

Species <- arc.open(paste0(inputDB, "/Species")) %>%
  arc.select()

SpeciesAtSite <- arc.open(paste0(inputDB, "/SpeciesAtSite")) %>%
  arc.select()

SpeciesAssessment <- arc.open(paste0(inputDB, "/SpeciesAssessment")) %>%
  arc.select()

PopSizeCitation <- arc.open(paste0(inputDB, "/PopSizeCitation")) %>%
  arc.select()

KBAInputPolygon <- arc.open(paste0(inputDB, "/KBAInputPolygon")) %>%
  arc.select()

if("KBACustomPolygon" %in% layers){
  KBACustomPolygon <- arc.open(paste0(inputDB, "/KBACustomPolygon")) %>%
    arc.select() %>%
    arc.data2sf() %>%
    st_set_crs(., st_crs(KBASite))
}

      # Lookup tables
KBA_Level <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2022.02.11/KBA_Level.csv", sep=",", fileEncoding = "UTF-8-BOM")
KBA_Province <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2022.02.11/KBA_Province.csv", sep=",")
Threats <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/Threats.csv", sep="\t", fileEncoding = "UTF-8-BOM")
Conservation <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/Conservation.csv", sep=",", fileEncoding = "UTF-8-BOM")
System <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/System.csv", sep=",", fileEncoding = "UTF-8-BOM")
Habitat <- read.xlsx("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/Habitat.xlsx")
AssessmentParameter <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2022.02.11/AssessmentParameter.csv")
COSEWICStatus <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2022.02.11/COSEWICStatus.csv")
Criterion <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2022.02.11/Criterion.csv")
IUCNStatus <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2022.02.11/IUCNStatus.csv")
Subcriterion <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2022.02.11/Subcriterion.csv")

      # KBA-EBAR database
biotics <- arc.open(paste0(url, 'EBAR-KBA/Restricted/FeatureServer/4')) %>%
  arc.select()

KBAInputPolygon %<>% filter(!is.na(InputPolygonID))
inputPolygonSQL <- ifelse(length(KBAInputPolygon$InputPolygonID)>0, paste0("inputpolygonid IN (", paste(KBAInputPolygon$InputPolygonID, collapse = ", "), ")"), "inputpolygonid IN (1000000000000000000000000000000000000000000000000000)")
inputPolygon <- arc.open(paste0(url, 'EBAR-KBA/Restricted/FeatureServer/2')) %>%
  arc.select(., where_clause = inputPolygonSQL)
if(nrow(inputPolygon)>0){
  inputPolygon %<>%
    arc.data2sp() %>%
    st_as_sf() %>%
    st_set_crs(., st_crs(KBASite))
}

#### Only retain information relevant to the Registry ####
# KBASite - TO DO: Only retain sites with Public = 1 (i.e. ready for public sharing)

# KBAThreat
KBAThreat %<>%
  filter(KBASiteID %in% KBASite$KBASiteID)

# KBAAction
KBAAction %<>%
  filter(KBASiteID %in% KBASite$KBASiteID)

# KBALandCover
if("KBALandCover" %in% layers){
  KBALandCover %<>%
    filter(KBASiteID %in% KBASite$KBASiteID)
}

# KBACitation
KBACitation %<>%
  filter(KBASiteID %in% KBASite$KBASiteID)

# SpeciesAtSite
SpeciesAtSite %<>%
  filter(KBASiteID %in% KBASite$KBASiteID) %>%
  filter(MeetsCriteria == "Y") # Only retain species that meet KBA criteria

# Species
Species %<>%
  filter(SpeciesID %in% SpeciesAtSite$SpeciesID)

# SpeciesAssessment
SpeciesAssessment %<>%
  filter(SpeciesAtSiteID %in% SpeciesAtSite$SpeciesAtSiteID)

# PopSizeCitation
cols <- colnames(PopSizeCitation)
PopSizeCitation %<>%
  filter(SpeciesAssessmentID %in% SpeciesAssessment$SpeciesAssessmentID)

# KBAInputPolygon
KBAInputPolygon %<>%
  filter(SpeciesAtSiteID %in% SpeciesAtSite$SpeciesAtSiteID) %>%
  filter(SpatialScope == "S") # Only retain internal boundaries

# KBACustomPolygon
if("KBACustomPolygon" %in% layers){
  KBACustomPolygon %<>%
    filter(SpeciesAtSiteID %in% SpeciesAtSite$SpeciesAtSiteID) %>%
    filter(SpatialScope == "S") # Only retain internal boundaries
}

# InputPolygon
inputPolygon %<>%
  filter(inputpolygonid %in% KBAInputPolygon$InputPolygonID)

#### Get information about species sensitivity ####
sppSensitivity <- SpeciesAtSite %>%
  select(SpeciesAtSiteID, KBASiteID, SpeciesID, Display_TaxonomicGroup, Display_TaxonName, Display_AlternativeName, Display_AssessmentInfo, Display_InternalBoundary) %>%
  mutate(NewID = ifelse(Display_TaxonName == "No",
                        10^(floor(log10(max(biotics$speciesid))) + 2):(10^(floor(log10(max(biotics$speciesid))) + 2) + nrow(.) - 1),
                        NA)) %>%
  left_join(., biotics[,c("speciesid", "kba_group")], by=c("SpeciesID" = "speciesid"))

sensitiveIDs <- sppSensitivity %>%
  filter(!is.na(NewID)) %>%
  pull(SpeciesID)

#### KBA-EBAR database to KBA Registry ####
# KBA_Level
arc.write(paste0(outputDB, "/KBA_Level"), KBA_Level, overwrite = T)

# KBA_Province
arc.write(paste0(outputDB, "/KBA_Province"), KBA_Province, overwrite = T)

# KBA_Site
      # Create
KBA_Site <- KBASite %>%
  rename(Name_EN = NationalName,
         Assessed = DateAssessed,
         AltMin = AltitudeMin,
         AltMax = AltitudeMax,
         Latitude = lat_wgs84,
         Longitude = long_wgs84,
         Area = AreaKM2,
         geometry = geom) %>%
  mutate(SiteID = 1:nrow(.),
         Name_FR = ifelse(is.na(NationalName_FR), Name_EN, NationalName_FR),
         Level_EN = ifelse(grepl("Global", KBALevel_EN), "Global", "National"),
         PercentProtected = NA,
         BoundaryGeneralized = ifelse(BoundaryGeneralization == 3, 1, 0))

      # If the site is global and not yet globally accepted, set it to national
            # Get IDs of concerned sites
globalNotAccepted <- KBA_Site %>%
  filter(Level_EN == "Global") %>%
  filter(!SiteStatus == "8") %>%
  pull(SiteCode)

            # Change level to national
KBA_Site %<>%
  mutate(Level_EN = ifelse(SiteCode %in% globalNotAccepted, "National", Level_EN))

      # Finalize
KBA_Site %<>%
  left_join(., KBA_Province, by=c("Jurisdiction_EN" = "Province_EN")) %>%
  left_join(., KBA_Level, by="Level_EN") %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Site") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Site"), KBA_Site, overwrite = T)

# KBA_Website
      # Create
KBA_Website <- KBASite %>%
  st_drop_geometry() %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
  rename(CustomaryJurisdictionSource_EN = CustomaryJurisdictionSrce_EN,
         CustomaryJurisdictionSource_FR = CustomaryJurisdictionSrce_FR,
         PublicCredit_EN = PublicAuthorship_EN,
         PublicCredit_FR = PublicAuthorship_FR) %>%
  mutate(BiodiversitySummary_EN = paste(NominationRationale_EN, AdditionalBiodiversity_EN, sep="\n\n"),
         BiodiversitySummary_FR = paste(NominationRationale_FR, AdditionalBiodiversity_FR, sep="\n\n"),
         GlobalCriteriaSummary_EN = NA,
         GlobalCriteriaSummary_FR = NA,
         NationalCriteriaSummary_EN = NA,
         NationalCriteriaSummary_FR = NA)

      # If the site is global and not yet globally accepted, add special disclaimer
            # Get global triggers at global KBAs not yet globally accepted
globalNotAccepted_triggers <- SpeciesAtSite %>%
  left_join(., KBASite[,c("KBASiteID", "SiteCode")], by="KBASiteID") %>%
  filter(SiteCode %in% globalNotAccepted) %>%
  filter(!is.na(GlobalCriteria)) %>%
  left_join(., biotics[,c("speciesid", "national_scientific_name", "national_engl_name", "national_fr_name")], by=c("SpeciesID" = "speciesid")) %>%
  select(SiteCode, national_scientific_name, national_engl_name, national_fr_name)

            # Compute extra disclaimer
                  # English
extraDisclaimer_EN <- globalNotAccepted_triggers %>%
  group_by(SiteCode) %>%
  arrange(national_engl_name) %>%
  mutate(ExtraDisclaimer_EN = paste0("<p>The following taxa are being proposed as global KBA triggers: ",
                                     paste0(paste0(national_engl_name, " (<i>", national_scientific_name, "</i>)"), collapse="; "),
                                     ". Once accepted, this site will become a global KBA. In the meantine, it is a national KBA.</p>")) %>%
  ungroup() %>%
  select(SiteCode, ExtraDisclaimer_EN)

                  # French
extraDisclaimer_FR <- globalNotAccepted_triggers %>%
  mutate(text = ifelse(!is.na(national_fr_name),
                       paste0(national_fr_name, " (<i>", national_scientific_name, "</i>)"),
                       paste0("<i>", national_scientific_name, "</i>"))) %>%
  group_by(SiteCode) %>%
  arrange(text) %>%
  mutate(ExtraDisclaimer_FR = paste0("<p>Les taxons suivants ont été proposés comme taxons se qualifiant au niveau mondial : ",
                                     paste0(text, collapse="; "),
                                     ". Une fois cette évaluation validée, le site deviendra une KBA mondiale. En attendant, il s'agit d'une KBA nationale.</p>")) %>%
  ungroup() %>%
  select(SiteCode, ExtraDisclaimer_FR)

            # Add disclaimer
KBA_Website %<>%
  left_join(., extraDisclaimer_EN, by = "SiteCode") %>%
  left_join(., extraDisclaimer_FR, by = "SiteCode") %>%
  mutate(Disclaimer_EN = ifelse(SiteCode %in% globalNotAccepted,
                                paste0(ExtraDisclaimer_EN, Disclaimer_EN),
                                Disclaimer_EN),
         Disclaimer_FR = ifelse(SiteCode %in% globalNotAccepted,
                                paste0(ExtraDisclaimer_FR, Disclaimer_FR),
                                Disclaimer_FR))

      # Finalize
KBA_Website %<>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Website") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Website"), KBA_Website, overwrite = T)

# KBA_Citation
      # Remove sensitive citations
KBACitation %<>%
  filter(Sensitive == 0) %>%
  select(-Sensitive)

      # Create
KBA_Citation <- KBACitation %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
  mutate(KBACitationID = 1:nrow(.),
         ShortReference = ShortCitation,
         LongReference = LongCitation) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Citation") %>% pull(Name_BC)))
  
      # Save
arc.write(paste0(outputDB, "/KBA_Citation"), KBA_Citation, overwrite = T)

# Threats
arc.write(paste0(outputDB, "/Threats"), Threats, overwrite = T)

# KBA_Threats
      # Create
KBA_Threats <- KBAThreat %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
  mutate(ThreatsSiteID = NA,
         LastLevel = sapply(1:nrow(.), function(x) sum(!is.na(.[x, c("Level1", "Level2", "Level3")]))),
         Threat_EN = ifelse(LastLevel == 1,
                            Level1,
                            ifelse(LastLevel == 2,
                                   Level2,
                                   Level3)),
         ThreatCode = sapply(Threat_EN, function(x) substr(x, 1, stri_locate_all(pattern=" ", x, fixed=T)[[1]][1,1]-1))) %>%
  left_join(., Threats[,c("ThreatID", "ThreatCode")], by="ThreatCode") %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Threats") %>% pull(Name_BC))) %>%
  distinct() %>%
  mutate(ThreatsSiteID = 1:nrow(.))

      # Save
arc.write(paste0(outputDB, "/KBA_Threats"), KBA_Threats, overwrite = T)

# Conservation
      # Create
Conservation %<>% mutate(ConservationCode = as.character(format(ConservationCode, digits=2)))

      # Save
arc.write(paste0(outputDB, "/Conservation"), Conservation, overwrite = T)

# KBA_Conservation
      # Create
KBA_Conservation <- KBAAction %>%
  rename(Ongoing_Needed = OngoingOrNeeded) %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
  mutate(ConservationSiteID = 1:nrow(.),
         ConservationAction = ifelse(ConservationAction == "None", "0.0 None", ConservationAction),
         ConservationCode = sapply(ConservationAction, function(x) substr(x, 1, stri_locate_all(pattern=" ", x, fixed=T)[[1]][1,1]-1))) %>%
  left_join(., Conservation[,c("ConservationID", "ConservationCode")], by="ConservationCode") %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Conservation") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Conservation"), KBA_Conservation, overwrite = T)

# System
arc.write(paste0(outputDB, "/System"), System, overwrite = T)

# KBA_System
      # Create
KBA_System <- KBASite %>%
  st_drop_geometry() %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
  separate_rows(., Systems, sep="; ") %>%
  left_join(., System[,c("SystemID", "Type_EN")], by=c("Systems" = "Type_EN")) %>%
  mutate(SystemSiteID = 1:nrow(.),
         Rank = NA) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_System") %>% pull(Name_BC))) %>%
  group_by(SiteID) %>%
  mutate(Rank = 1:n())
  
      # Save
arc.write(paste0(outputDB, "/KBA_System"), KBA_System, overwrite = T)

# Habitat
      # Create
Habitat %<>% mutate(HabitatCode = as.character(format(HabitatCode, digits=3)))

      # Save
arc.write(paste0(outputDB, "/Habitat"), Habitat, overwrite = T)

# KBA_Habitat
if("KBALandCover" %in% layers){
      # Create
  KBA_Habitat <- KBALandCover %>%
    left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
    left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
    mutate(HabitatSiteID = 1:nrow(.),
           LandCover = ifelse(LandCover == "Cave and subterranean", "Cave and Subterranean Habitat", LandCover)) %>%
    left_join(., Habitat[,c("HabitatID", "Habitat_EN")], by=c("LandCover" = "Habitat_EN")) %>%
    select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Habitat") %>% pull(Name_BC)))
  
      # Save
  arc.write(paste0(outputDB, "/KBA_Habitat"), KBA_Habitat, overwrite = T)
}

# IUCNStatus
      # Format
IUCNStatus %<>% mutate(Nomenclature = trimws(Nomenclature))

      # Save
arc.write(paste0(outputDB, "/IUCNStatus"), IUCNStatus, overwrite = T)

# COSEWICStatus
arc.write(paste0(outputDB, "/COSEWICStatus"), COSEWICStatus, overwrite = T)

# Species - Start
      # Add BIOTICS_ELEMENT_NATIONAL information
Species %<>%
  left_join(., biotics, by=c("SpeciesID" = "speciesid"))

      # Change field names to Title Case
colnames(Species) <- tools::toTitleCase(colnames(Species))

      # General formatting
Species %<>%  
  rename(Order = Tax_order,
         ScientificName = National_scientific_name,
         InformalTaxonomicGroup = Kba_group,
         CommonName_EN = National_engl_name,
         CommonName_FR = National_fr_name,
         TaxonomicLevel = Ca_nname_level,
         WDKBASpecRecID = WDKBAID,
         IUCNTaxonID = IUCN_InternalTaxonID,
         IUCNAssessmentID = IUCN_AssessmentID,
         IUCNAssessmentDate = IUCN_AssessmentDate,
         IUCNStatusCriteria = IUCN_Criteria,
         IUCNPopulationTrend = IUCN_PopulationTrend,
         NSElementCode = Element_code,
         Systems = Major_habitat,
         NSGlobalUniqueID = Global_unique_identifier,
         NSElementGlobalID = Element_global_id,
         Endemism = N_endemism_desc,
         COSEWICTaxonID = Cosewic_id,
         COSEWICAssessmentDate = Cosewic_date,
         COSEWICStatusCriteria = Cosewic_assess_criteria,
         SARAStatus = Sara_status,
         NSGRank = G_rank,
         NSGRankReviewDate = G_rank_review_date,
         NSNRank = N_rank,
         NSNRankReviewDate = N_rank_review_date,
         NSLink = Nsx_url) %>%
  mutate(Sensitive = 0,
         Subspecies_EN = NA,
         Subspecies_FR = NA,
         Population_EN = NA,
         Population_FR = NA,
         BirdAlphaCode = NA,
         BCSpeciesID = NA,
         IUCNLink = NA,
         SARAAssessmentDate = NA,
         COSEWICLink = NA,
         ContinentalPopulationSize = NA,
         CitationContinentalPopulation = NA,
         NationalTrend = NA,
         CitationNationalTrend = NA,
         Endemism = ifelse(Endemism == "Yes (the element is endemic)",
                           "Y",
                           ifelse(Endemism == "No (the element is not endemic)",
                                  "N",
                                  ifelse(Endemism == "Breeding (endemic as a breeder only)",
                                         "B",
                                         ifelse(Endemism == "Probable",
                                                "P",
                                                Endemism))))) %>%
  left_join(., IUCNStatus, by=c("IUCN_CD" = "Nomenclature")) %>%
  left_join(., COSEWICStatus, by=c("Cosewic_status" = "Nomenclature"))

      # Range information
            # Unnest current_distribution
range_info <- Species %>%
  select(Current_distribution, SpeciesID) %>%
  mutate(Current_distribution = strsplit(Current_distribution, ", ")) %>%
  unnest(Current_distribution) %>%
  mutate(Current_distribution = ifelse(Current_distribution %in% c("LB", "NF"), "NL", Current_distribution)) %>%
  left_join(., KBA_Province, by=c("Current_distribution" = "Abbreviation"))

            # Range
range <- range_info %>%
  select(SpeciesID, Current_distribution) %>%
  group_by(SpeciesID) %>%
  arrange(SpeciesID, Current_distribution) %>%
  distinct() %>%
  summarise(Range = paste(Current_distribution, collapse = ", "))

            # Range_EN
range_en <- range_info %>%
  select(SpeciesID, Province_EN) %>%
  group_by(SpeciesID) %>%
  arrange(SpeciesID, Province_EN) %>%
  summarise(Range_EN = paste(Province_EN, collapse = ", "))

            # Range_FR
range_fr <- range_info %>%
  select(SpeciesID, Province_FR) %>%
  group_by(SpeciesID) %>%
  arrange(SpeciesID, Province_FR) %>%
  summarise(Range_FR = paste(Province_FR, collapse = ", "))

            # Join
Species %<>%
  left_join(., range, by="SpeciesID") %>%
  left_join(., range_en, by="SpeciesID") %>%
  left_join(., range_fr, by="SpeciesID")

      # Reference population size
            # Get estimates that are relevant to the KBA Registry (i.e. most recent # of mature individuals global/national estimate)
relevantReferenceEstimates <- SpeciesAssessment %>%
  left_join(., SpeciesAtSite[,c("SpeciesAtSiteID", "SpeciesID")], by="SpeciesAtSiteID") %>%
  filter(AssessmentParameter == "(i) number of mature individuals") %>% # Only retain the record if the assessment parameter = number of mature individuals
  group_by(SpeciesID, KBALevel) %>%
  arrange(SpeciesID, KBALevel, desc(DateAssessed)) %>%
  filter(row_number()==1) # Only retain the most recent SpeciesAssessment per species and per KBALevel

            # Add the estimates to the Species table
Species <- relevantReferenceEstimates %>%
  select(SpeciesID, KBALevel, ReferenceEstimate_Best, ReferenceEstimate_Sources) %>%
  rename(PopulationSize = ReferenceEstimate_Best,
         Citation = ReferenceEstimate_Sources) %>%
  pivot_wider(names_from = KBALevel, values_from = c(PopulationSize, Citation), names_glue = "{KBALevel}{.value}") %>%
  mutate(GlobalCitation = ifelse('GlobalCitation' %in% names(.), GlobalCitation, NA),
         NationalCitation = ifelse('NationalCitation' %in% names(.), NationalCitation, NA),
         GlobalPopulationSize = ifelse('GlobalPopulationSize' %in% names(.), GlobalPopulationSize, NA),
         NationalPopulationSize = ifelse('NationalPopulationSize' %in% names(.), NationalPopulationSize, NA)) %>%
  rename(CitationGlobalPopulation = GlobalCitation,
         CitationNationalPopulation = NationalCitation) %>%
  left_join(Species, ., by="SpeciesID")

      # Select final columns
Species %<>%
  select(all_of(crosswalk %>% filter(Layer_BC == "Species") %>% pull(Name_BC)))

      # Handle sensitive species
            # Add a record for every sensitive SpeciesAtSite record
Species %<>%
  full_join(., sppSensitivity %>% filter(Display_TaxonName == "No") %>% select(-c(SpeciesAtSiteID, KBASiteID, Display_AssessmentInfo, Display_InternalBoundary)), by=c("SpeciesID"="NewID"))

            # Populate this record based on Display_* specs
Species %<>%
  # Add Sensitive = 1
  mutate(Display_TaxonName = ifelse(is.na(Display_TaxonName), "Yes", Display_TaxonName),
         Sensitive = ifelse(Display_TaxonName == "No", 1, Sensitive)) %>%
  # Change taxon name to alternative name
  mutate(CommonName_EN = ifelse(Display_TaxonName == "No", str_to_sentence(Display_AlternativeName), CommonName_EN),
         CommonName_FR = ifelse(Display_TaxonName == "No", ifelse(str_to_sentence(Display_AlternativeName) == "A sensitive species", "Une espèce sensible", "PB"), CommonName_FR)) %>%
  select(-c(Display_TaxonName, Display_AlternativeName)) %>%
  # Remove taxonomic group, where applicable
  mutate(Display_TaxonomicGroup = ifelse(is.na(Display_TaxonomicGroup), "Yes", Display_TaxonomicGroup),
         InformalTaxonomicGroup = ifelse(is.na(InformalTaxonomicGroup), ifelse(Display_TaxonomicGroup == "Yes", kba_group, "Sensitive Species"), InformalTaxonomicGroup)) %>%
  select(-c(Display_TaxonomicGroup, kba_group, SpeciesID.y))

          # Check any alternative names were correctly translated
if("PB" %in% Species$CommonName_FR){
  stop("Some alternative name for sensitive species couldn't be translated to French.")
}

# Species_Citation
      # Create
Species_Citation <- PopSizeCitation %>%
  filter(!Scope == "Site") %>%
  filter(SpeciesAssessmentID %in% relevantReferenceEstimates$SpeciesAssessmentID) %>%
  left_join(., KBACitation, by="KBACitationID") %>%
  left_join(., SpeciesAssessment, by="SpeciesAssessmentID") %>%
  left_join(., SpeciesAtSite, by="SpeciesAtSiteID") %>%
  distinct(SpeciesID, KBACitationID, .keep_all = T) %>% # Only keep each citation once per species (this line is necessary for cases where the same citation was used for both a global and a national assessment for the species)
  mutate(SpeciesCitationID = 1:nrow(.)) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "Species_Citation") %>% pull(Name_BC))) %>%
  filter(!is.na(ShortCitation))

      # Check that there are no citations tied to sensitive species
if(sum(sensitiveIDs %in% Species_Citation$SpeciesID) > 0){
  Stop("Some citations tied to sensitive species have not been removed")
}

      # Save
arc.write(paste0(outputDB, "/Species_Citation"), Species_Citation, overwrite = T)

# Criterion
arc.write(paste0(outputDB, "/Criterion"), Criterion, overwrite = T)

# Subcriterion
      # Create intermediate tibble for use in KBA_SpeciesAssessments computation
Subcriterion_inter <- Subcriterion %>%
  left_join(., KBA_Level, by="LevelID") %>%
  mutate(Level_EN = sapply(Level_EN, function(x) tolower(substr(x, start=1, stop=1))),
         Subcriterion = paste0(Level_EN, Subcriterion))

      # Save
arc.write(paste0(outputDB, "/Subcriterion"), Subcriterion, overwrite = T)

# AssessmentParameter
      # Create intermediate tibble for use in KBA_SpeciesAssessments computation
AssessmentParameter_inter <- AssessmentParameter %>%
  mutate(AssessmentParameter = paste0("(", AssessmentParameterCode, ") ", tolower(AssessmentParameter_EN)))

      # Save
arc.write(paste0(outputDB, "/AssessmentParameter"), AssessmentParameter, overwrite = T)

# InternalBoundary - Start
# TO DO: Make sure internal boundaries are excluded if Display_InternalBoundary = No
      # KBACustomPolygon
if("KBACustomPolygon" %in% layers){
  if(nrow(KBACustomPolygon) > 0){
    
    internalBoundary_customPolygon <- KBACustomPolygon %>%
      select(SpeciesAtSiteID) %>%
      left_join(., SpeciesAtSite[,c("SpeciesAtSiteID", "KBASiteID", "SpeciesID")], by="SpeciesAtSiteID") %>%
      left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
      left_join(., st_drop_geometry(KBA_Site[,c("SiteCode", "SiteID")]), by="SiteCode") %>%
      select(SiteID, SpeciesID) %>%
      arrange(SiteID, SpeciesID) %>%
      mutate(InternalBoundaryID = 1:nrow(.),
             InternalBoundarySummary_EN = NA,
             InternalBoundarySummary_FR = NA)
    
    if("geom" %in% colnames(internalBoundary_customPolygon)){
      internalBoundary_customPolygon %<>% rename(geometry = geom)
    }
    
    maxID <- max(internalBoundary_customPolygon$InternalBoundaryID)
    
  }else{
    maxID <- 0
  }
}

      # KBAInputPolygon
# TO DO: Make sure internal boundaries are excluded if Display_InternalBoundary = No
if(nrow(inputPolygon) > 0){
  
  internalBoundary_inputPolygon <- inputPolygon %>%
    left_join(., KBAInputPolygon[,c("InputPolygonID", "SpeciesAtSiteID")], by=c("inputpolygonid" = "InputPolygonID")) %>%
    select(SpeciesAtSiteID) %>%
    left_join(., SpeciesAtSite[,c("SpeciesAtSiteID", "KBASiteID", "SpeciesID")], by="SpeciesAtSiteID") %>%
    left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
    left_join(., st_drop_geometry(KBA_Site[,c("SiteCode", "SiteID")]), by="SiteCode") %>%
    select(SiteID, SpeciesID, geometry) %>%
    arrange(SiteID, SpeciesID) %>%
    mutate(InternalBoundaryID = (maxID+1):(maxID+nrow(.)),
           InternalBoundarySummary_EN = NA,
           InternalBoundarySummary_FR = NA)
}

      # Bind rows
if(exists("internalBoundary_customPolygon") && exists("internalBoundary_inputPolygon")){
  InternalBoundary <- bind_rows(internalBoundary_customPolygon, internalBoundary_inputPolygon)
  
}else if(exists("internalBoundary_customPolygon")){
  InternalBoundary <- internalBoundary_customPolygon

}else if(exists("internalBoundary_inputPolygon")){
  InternalBoundary <- internalBoundary_inputPolygon
}

# KBA_SpeciesAssessments
      # Create
KBA_SpeciesAssessments <- SpeciesAssessment %>%
  left_join(., SpeciesAtSite[,c("SpeciesAtSiteID", "KBASiteID", "SpeciesID")], by="SpeciesAtSiteID") %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteCode", "SiteID")]), by="SiteCode") %>%
  rename(MinReproductiveUnits = RU_Min,
         RUType = RU_Composition10RUs,
         RUSources = RU_Sources,
         MinSitePopulation = SiteEstimate_Min,
         BestSitePopulation = SiteEstimate_Best,
         MaxSitePopulation = SiteEstimate_Max,
         SiteDerivation = SiteEstimate_Derivation,
         SitePopulationSources = SiteEstimate_Sources,
         MinRefPopulation = ReferenceEstimate_Min,
         BestRefPopulation = ReferenceEstimate_Best,
         MaxRefPopulation = ReferenceEstimate_Max,
         RefPopulationSources = ReferenceEstimate_Sources,
         SpeciesAssessmentsID = SpeciesAssessmentID) %>%
  mutate(SpeciesStatus = paste0(Status_Value, " (", Status_AssessmentAgency, ")"),
         FootnoteID = NA) %>%
  left_join(., AssessmentParameter_inter[,c("AssessmentParameter", "AssessmentParameterID")], by="AssessmentParameter")

if(exists("InternalBoundary")){
  KBA_SpeciesAssessments %<>%
    left_join(., InternalBoundary[,c("SiteID", "SpeciesID", "InternalBoundaryID")], by=c("SiteID", "SpeciesID"))
}else{
  KBA_SpeciesAssessments %<>%
    mutate(InternalBoundaryID = NA)
}

      # Select final columns
KBA_SpeciesAssessments %<>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_SpeciesAssessments") %>% pull(Name_BC)))

      # Handle sensitive information
            # For sensitive species, use NewID
KBA_SpeciesAssessments %<>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteID") %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="SiteCode") %>%
  left_join(., sppSensitivity[,c("SpeciesID", "KBASiteID", "NewID", "Display_AssessmentInfo")], by=c("SpeciesID", "KBASiteID")) %>%
  mutate(SpeciesID = ifelse(!is.na(NewID), NewID, SpeciesID),
         Display_AssessmentInfo = ifelse(is.na(Display_AssessmentInfo), "Yes", Display_AssessmentInfo))

            # If Display_AssessmentInfo = No, remove assessment information
KBA_SpeciesAssessments %<>%
  mutate(SpeciesStatus = ifelse(Display_AssessmentInfo == "No", NA, SpeciesStatus),
         PercentAtSite = ifelse(Display_AssessmentInfo == "No", NA, PercentAtSite),
         AssessmentParameterID = ifelse(Display_AssessmentInfo == "No", NA, AssessmentParameterID),
         MinReproductiveUnits = ifelse(Display_AssessmentInfo == "No", NA, MinReproductiveUnits),
         RUType = ifelse(Display_AssessmentInfo == "No", NA, RUType),
         RUSources = ifelse(Display_AssessmentInfo == "No", NA, RUSources),
         SeasonalDistribution = ifelse(Display_AssessmentInfo == "No", NA, SeasonalDistribution),
         MinSitePopulation = ifelse(Display_AssessmentInfo == "No", NA, MinSitePopulation),
         BestSitePopulation = ifelse(Display_AssessmentInfo == "No", NA, BestSitePopulation),
         MaxSitePopulation = ifelse(Display_AssessmentInfo == "No", NA, MaxSitePopulation),
         SiteDerivation = ifelse(Display_AssessmentInfo == "No", NA, SiteDerivation),
         SitePopulationSources = ifelse(Display_AssessmentInfo == "No", NA, SitePopulationSources),
         MinRefPopulation = ifelse(Display_AssessmentInfo == "No", NA, MinRefPopulation),
         BestRefPopulation = ifelse(Display_AssessmentInfo == "No", NA, BestRefPopulation),
         MaxRefPopulation = ifelse(Display_AssessmentInfo == "No", NA, MaxRefPopulation),
         RefPopulationSources = ifelse(Display_AssessmentInfo == "No", NA, RefPopulationSources))

      # Select final columns (again)
KBA_SpeciesAssessments %<>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_SpeciesAssessments") %>% pull(Name_BC)))
  
      # Save
arc.write(paste0(outputDB, "/KBA_SpeciesAssessments"), KBA_SpeciesAssessments, overwrite = T)

# SpeciesAssessment_Subcriterion
      # Create
SpeciesAssessment_Subcriterion <- SpeciesAssessment %>%
  left_join(., SpeciesAtSite[,c("SpeciesAtSiteID", "KBASiteID", "SpeciesID")], by="SpeciesAtSiteID") %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteCode", "SiteID")]), by="SiteCode") %>%
  separate_rows(., CriteriaMet, sep="; ") %>%
  left_join(., Subcriterion_inter[,c("Subcriterion", "SubcriterionID")], by=c("CriteriaMet" = "Subcriterion")) %>%
  left_join(., sppSensitivity[,c("SpeciesID", "KBASiteID", "NewID", "Display_AssessmentInfo")], by=c("SpeciesID", "KBASiteID")) %>%
  mutate(SpeciesID = ifelse(!is.na(NewID), NewID, SpeciesID)) %>%
  left_join(., KBA_SpeciesAssessments[,c("SiteID", "SpeciesID", "SpeciesAssessmentsID")], by=c("SiteID", "SpeciesID")) %>%
  mutate(AssessmentSubcriterionID = 1:nrow(.)) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "SpeciesAssessment_Subcriterion") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/SpeciesAssessment_Subcriterion"), SpeciesAssessment_Subcriterion, overwrite = T)

# InternalBoundary - Finish
if(exists("InternalBoundary")){
        # Select fields
  InternalBoundary %<>% select(all_of(crosswalk %>% filter(Layer_BC == "InternalBoundary") %>% pull(Name_BC)))
  
        # Clip to KBA boundary
  for(i in InternalBoundary$InternalBoundaryID){
    
    # Get internal boundary
    internalboundary <- InternalBoundary %>%
      filter(InternalBoundaryID == i) %>%
      st_union(.)
    
    # Get KBA boundary
    kbasite <- KBA_Site %>%
      left_join(., KBA_SpeciesAssessments[, c("SiteID", "InternalBoundaryID")], by="SiteID") %>%
      filter(InternalBoundaryID == i)
    
    # Clip
    internalboundary %<>% st_intersection(., kbasite)
    
    # Convert to sf
    internalboundary %<>% st_as_sf()
    st_geometry(internalboundary) <- "geometry"
    
    # Write
    st_geometry(InternalBoundary)[which(InternalBoundary$InternalBoundaryID == i)] <- internalboundary$geometry
  }
  
        # Save
  arc.write(paste0(outputDB, "/InternalBoundary"), InternalBoundary, overwrite = T, validate = T)
}

# Species - Finish
      # Only keep records with IDs in KBA_SpeciesAssessments
Species %<>%
  filter(SpeciesID %in% KBA_SpeciesAssessments$SpeciesID)

      # Select final columns (again)
Species %<>%
  select(all_of(crosswalk %>% filter(Layer_BC == "Species") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/Species"), Species, overwrite = T)
