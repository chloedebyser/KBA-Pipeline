#### Identification of Canadian KBAs
#### Wildlife Conservation Society - 2021
#### Script by Chloé Debyser

#### KBA-EBAR to KBA Registry - Trial Islands & Yukon Pilot

#### Workspace ####
# Packages
library(openxlsx)
library(tidyverse)
library(magrittr)
library(sf)
library(arcgisbinding)
arc.check_product()
library(stringi)

# Input parameters
inputDB <- "C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBA-EBAR_Pilot.gdb"
outputDB <- "C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBARegistry_Pilot.gdb"

# Remove the output geodatabase, to start fresh
arc.delete(dirname("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBARegistry_Pilot.gdb/KBA_Status"))

# Data
      # WCSC-BC crosswalk
crosswalk <- read.xlsx("G:/My Drive/KBA Canada Team/1. Source Datasets/Wildlife Conservation Society Canada/BirdsCanada-WCSC_DatabaseCrosswalk.xlsx")
Layer_BC <- crosswalk[2:nrow(crosswalk), 2]
Layer_WCSC <- crosswalk[2:nrow(crosswalk), 8]
Name_BC <- crosswalk[2:nrow(crosswalk), 3]
Name_WCSC <- crosswalk[2:nrow(crosswalk), 9]
crosswalk <- data.frame(Layer_BC = Layer_BC, Name_BC = Name_BC, Layer_WCSC = Layer_WCSC, Name_WCSC = Name_WCSC) %>%
  filter_all(any_vars(!is.na(.)))

      # Input Database
KBASite <- arc.open(paste0(inputDB, "/KBASite")) %>%
  arc.select() %>%
  arc.data2sp() %>%
  st_as_sf() %>%
  arrange(StatusChangeDate) %>%
  mutate(SiteCode = 1:nrow(.)) # Add temporary site codes

      # Lookup tables
KBA_Status <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Bird Studies Canada/LookUps_2021.09.21/KBA_Status.csv", sep="\t", fileEncoding = "UTF-8-BOM")
KBA_Province <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Bird Studies Canada/LookUps_2021.09.21/KBA_Province.csv", sep=",", fileEncoding = "UTF-8-BOM")

#### KBA-EBAR database to KBA Registry ####
# KBA_Status
arc.write(paste0(outputDB, "/KBA_Status"), KBA_Status, overwrite = T)

# KBA_Province
arc.write(paste0(outputDB, "/KBA_Province"), KBA_Province, overwrite = T)

# KBA_ProtectedArea
      # Create
KBA_ProtectedArea <- KBASite %>%
  filter(!is.na(ProtectedAreas_EN) | !is.na(ProtectedAreas_FR)) %>% # Only retain sites that have protected area information
  st_drop_geometry() %>%
  select(SiteCode, ProtectedAreas_EN, ProtectedAreas_FR, PercentProtected) %>%
  mutate(ProtectedAreaID = ifelse(nrow(.)>0, 1:nrow(.), NA),
         IUCNCat = NA) %>%
  rename(SiteID = SiteCode,
         PercentCover = PercentProtected,
         ProtectedArea_EN = ProtectedAreas_EN,
         ProtectedArea_FR = ProtectedAreas_FR) %>%
  mutate(PercentCover = gsub(" - completely unprotected", "", PercentCover)) %>%
  mutate(PercentCover = gsub(" - completely protected", "", PercentCover)) %>%
  select(ProtectedAreaID, SiteID, ProtectedArea_EN, ProtectedArea_FR, IUCNCat, PercentCover)

      # Save
arc.write(paste0(outputDB, "/KBA_ProtectedArea"), KBA_ProtectedArea, overwrite = T)

# KBA_Site
      # Create
KBA_Site <- KBASite %>%
  filter(SiteStatus == 6) %>% # Only retain sites with SiteStatus = 6 (Accepted)
  rename(SiteID = SiteCode,
         Name_EN = NationalName,
         Assessed = StatusChangeDate,
         AltMin = AltitudeMin,
         AltMax = AltitudeMax) %>%
  mutate(SiteCode = NA,
         Name_FR = Name_EN,
         Status_EN = ifelse(grepl("Global", KBALevel), "Global", "National")) %>%
  left_join(., KBA_Province, by=c("Jurisdiction" = "Province_EN")) %>%
  left_join(., KBA_Status, by=c("Status_EN" = "Status_EN")) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Site") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Site"), KBA_Site, overwrite = T)

# KBA_Website
      # Create
KBA_Website <- KBASite %>%
  filter(SiteStatus == 6) %>% # Only retain sites with SiteStatus = 6 (Accepted)
  st_drop_geometry() %>%
  rename(SiteID = SiteCode,
         BiodiversitySummary_EN = AdditionalBiodiversity_EN,
         BiodiversitySummary_FR = AdditionalBiodiversity_FR) %>%
  mutate(Conservation_EN = NA,
         Conservation_FR = NA) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Website") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Website"), KBA_Website, overwrite = T)




# PICK UP HERE
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

