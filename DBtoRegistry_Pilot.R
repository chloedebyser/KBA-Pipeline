#### Identification of Canadian KBAs
#### Wildlife Conservation Society - 2021
#### Script by Chloé Debyser

#### KBA-EBAR to KBA Registry - Trial Islands & Yukon Pilot
#### Needed: Way to assign unique IDs (e.g. ThreatsSiteID, ConservationSiteID)
#### Needed: Way to match new records to previously existing IDs (e.g. for citations - if a given citation was already used in a previous KBA proposal)
#### Needed: Don't send global sites to Registry if they don't have a WDKBAID
#### Needed: Add considerations of species sensitivity
#### Needed: Filter internal boundaries
#### Needed: Find substitutes for everything that is hard coded
#### Needed: Add SARA_STATUS_DATE when available in BIOTICS_ELEMENT_NATIONAL
#### Needed: Implement FootnoteID (right now it is just set to NA)

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
inputDB <- "C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBA-EBAR_Pilot.gdb"
outputDB <- "C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBARegistry_Pilot.gdb"

# Remove the output geodatabase, to start fresh
arc.delete(dirname("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBARegistry_Pilot.gdb/KBA_Level"))

# Data
      # WCSC-BC crosswalk
crosswalk <- read.xlsx("G:/My Drive/KBA Canada Team/1. Source Datasets/Wildlife Conservation Society Canada/BirdsCanada-WCSC_DatabaseCrosswalk.xlsx")
Layer_BC <- crosswalk[2:nrow(crosswalk), 10]
Layer_WCSC <- crosswalk[2:nrow(crosswalk), 1]
Name_BC <- crosswalk[2:nrow(crosswalk), 11]
Name_WCSC <- crosswalk[2:nrow(crosswalk), 2]
crosswalk <- data.frame(Layer_WCSC = Layer_WCSC, Name_WCSC = Name_WCSC, Layer_BC = Layer_BC, Name_BC = Name_BC) %>%
  filter_all(any_vars(!is.na(.)))

      # Input Database
KBASite <- arc.open(paste0(inputDB, "/KBASite")) %>%
  arc.select() %>%
  arc.data2sf()

KBAThreat <- arc.open(paste0(inputDB, "/KBAThreat")) %>%
  arc.select()

KBAAction <- arc.open(paste0(inputDB, "/KBAAction")) %>%
  arc.select()

KBAHabitat <- arc.open(paste0(inputDB, "/KBAHabitat")) %>%
  arc.select()

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

KBACustomPolygon <- arc.open(paste0(inputDB, "/KBACustomPolygon")) %>%
  arc.select() %>%
  arc.data2sf()

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

inputPolygon <- arc.open(paste0(url, 'EBAR-KBA/Restricted/FeatureServer/2')) %>%
  arc.select(., where_clause = paste0("inputpolygonid IN (", paste(KBAInputPolygon$InputPolygonID, collapse = ", "), ")")) %>%
  arc.data2sp() %>%
  st_as_sf()

#### Only retain information relevant to the Registry ####
# KBASite
KBASite %<>%
  filter(Public == 1) # Only retain sites with Public = 1 (i.e. ready for public sharing)

# KBAThreat
KBAThreat %<>%
  filter(KBASiteID %in% KBASite$KBASiteID)

# KBAAction
KBAAction %<>%
  filter(KBASiteID %in% KBASite$KBASiteID)

# KBAHabitat
KBAHabitat %<>%
  filter(KBASiteID %in% KBASite$KBASiteID)

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
KBACustomPolygon %<>%
  filter(SpeciesAtSiteID %in% SpeciesAtSite$SpeciesAtSiteID) %>%
  filter(SpatialScope == "S") # Only retain internal boundaries

# InputPolygon
inputPolygon %<>%
  filter(inputpolygonid %in% KBAInputPolygon$InputPolygonID)

#### KBA-EBAR database to KBA Registry ####
# KBA_Level
arc.write(paste0(outputDB, "/KBA_Level"), KBA_Level, overwrite = T)

# KBA_Province
arc.write(paste0(outputDB, "/KBA_Province"), KBA_Province, overwrite = T)

# KBA_Site
      # Create
KBA_Site <- KBASite %>%
  rename(Name_EN = NationalName,
         Assessed = StatusChangeDate,
         AltMin = AltitudeMin,
         AltMax = AltitudeMax,
         Latitude = lat_wgs84,
         Longitude = long_wgs84,
         Area = AreaKM2,
         geometry = geom) %>%
  mutate(SiteID = 1:nrow(.),
         Name_FR = ifelse(is.na(NationalName_FR), Name_EN, NationalName_FR),
         Level_EN = ifelse(grepl("Global", KBALevel_EN), "Global", "National"),
         PercentProtected = NA) %>%
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
  rename(BiodiversitySummary_EN = AdditionalBiodiversity_EN,
         BiodiversitySummary_FR = AdditionalBiodiversity_FR) %>%
  mutate(Conservation_EN = NA,
         Conservation_FR = NA,
         GlobalCriteriaSummary_EN = NA,
         GlobalCriteriaSummary_FR = NA,
         NationalCriteriaSummary_EN = NA,
         NationalCriteriaSummary_FR = NA) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Website") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Website"), KBA_Website, overwrite = T)

# KBA_Citation
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
      # Create
KBA_Habitat <- KBAHabitat %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
  mutate(HabitatSiteID = 1:nrow(.)) %>%
  left_join(., Habitat[,c("HabitatID", "Habitat_EN")], by=c("Habitat" = "Habitat_EN")) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Habitat") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Habitat"), KBA_Habitat, overwrite = T)

# IUCNStatus
arc.write(paste0(outputDB, "/IUCNStatus"), IUCNStatus, overwrite = T)

# COSEWICStatus
arc.write(paste0(outputDB, "/COSEWICStatus"), COSEWICStatus, overwrite = T)

# Species
      # Add BIOTICS_ELEMENT_NATIONAL information
Species %<>%
  left_join(., biotics, by=c("SpeciesID" = "speciesid"))

      # Change field names to Title Case
colnames(Species) <- tools::toTitleCase(colnames(Species))

      # General formatting
Species %<>%  
  rename(Order = Tax_order,
         ScientificName = National_scientific_name,
         InformalTaxonomicGroup = RegistryTaxGroup,
         CommonName_EN = National_engl_name,
         CommonName_FR = National_fr_name,
         TaxonomicLevel = Ca_nname_level,
         WDKBASpecRecID = WDKBAID,
         IUCNTaxonID = IUCN_InternalTaxonID,
         IUCNAssessmentID = IUCN_AssessmentID,
         IUCNAssessmentDate = IUCN_AssessmentDate,
         IUCNPopulationTrend = IUCN_PopulationTrend,
         NSElementCode = Element_code,
         NSGlobalUniqueID = Global_unique_identifier,
         NSElementGlobalID = Element_global_id,
         COSEWICAssessmentDate = Cosewic_date,
         COSEWICStatusCriteria = Cosewic_assess_criteria,
         SARStatus = Sara_status,
         NSGRank = G_rank,
         NSGRankReviewDate = G_rank_review_date,
         NSNRank = N_rank,
         NSNRankReviewDate = N_rank_review_date) %>%
  mutate(Sensitive = 0,
         Subspecies_EN = NA,
         Subspecies_FR = NA,
         Population_EN = NA,
         Population_FR = NA,
         BirdAlphaCode = NA,
         BCSpeciesID = NA,
         IUCNLink = NA,
         SARAssessmentDate = NA,
         COSEWICLink = NA,
         NSLink = NA,
         ContinentalPopulationSize = NA,
         CitationContinentalPopulation = NA,
         NationalTrend = NA,
         CitationNationalTrend = NA) %>%
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
  rename(CitationGlobalPopulation = GlobalCitation,
         CitationNationalPopulation = NationalCitation) %>%
  left_join(Species, ., by="SpeciesID")

      # Select final columns
Species %<>%
  select(all_of(crosswalk %>% filter(Layer_BC == "Species") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/Species"), Species, overwrite = T)

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
  select(all_of(crosswalk %>% filter(Layer_BC == "Species_Citation") %>% pull(Name_BC)))

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

# KBA_SpeciesAssessments
      # Create
KBA_SpeciesAssessments <- SpeciesAssessment %>%
  left_join(., SpeciesAtSite[,c("SpeciesAtSiteID", "KBASiteID", "SpeciesID")], by="SpeciesAtSiteID") %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteCode", "SiteID")]), by="SiteCode") %>%
  separate_rows(., CriteriaMet, sep="; ") %>%
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
         RefPopulationSources = ReferenceEstimate_Sources) %>%
  mutate(SpeciesAssessmentsID = 1:nrow(.),
         SpeciesStatus = paste0(Status_Value, " (", Status_AssessmentAgency, ")"),
         FootnoteID = NA) %>%
  left_join(., Subcriterion_inter[,c("Subcriterion", "SubcriterionID")], by=c("CriteriaMet" = "Subcriterion")) %>%
  left_join(., AssessmentParameter_inter[,c("AssessmentParameter", "AssessmentParameterID")], by="AssessmentParameter") %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_SpeciesAssessments") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_SpeciesAssessments"), KBA_SpeciesAssessments, overwrite = T)

# InternalBoundary
      # Check that KBACustomPolygon is empty (because the code that deals with that feature class is not yet written)
if(nrow(KBACustomPolygon) > 0){
  stop("There are records in KBACustomPolygon, and the code isn't yet written for this dataset.")
}

      # Create
InternalBoundary <- inputPolygon %>%
  left_join(., KBAInputPolygon[,c("InputPolygonID", "SpeciesAtSiteID")], by=c("inputpolygonid" = "InputPolygonID")) %>%
  select(SpeciesAtSiteID) %>%
  left_join(., SpeciesAtSite[,c("SpeciesAtSiteID", "KBASiteID", "SpeciesID")], by="SpeciesAtSiteID") %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteCode", "SiteID")]), by="SiteCode") %>%
  select(SiteID, SpeciesID, geometry) %>%
  arrange(SiteID, SpeciesID) %>%
  mutate(InternalBoundaryID = 1:nrow(.), .before=SiteID)
      
      # Save
arc.write(paste0(outputDB, "/InternalBoundary"), InternalBoundary, overwrite = T, validate = T)
