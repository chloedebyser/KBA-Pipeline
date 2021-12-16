#### Identification of Canadian KBAs
#### Wildlife Conservation Society - 2021
#### Script by Chloé Debyser

#### KBA-EBAR to KBA Registry - Trial Islands & Yukon Pilot
#### Needed: Way to assign unique IDs (e.g. ThreatsSiteID, ConservationSiteID)

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
arc.delete(dirname("C:/Users/CDebyser/OneDrive - Wildlife Conservation Society/4. Analyses/5. Pipeline - KBA-EBAR to Registry/KBARegistry_Pilot.gdb/KBA_Level"))

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
  arc.data2sf()

KBAThreat <- arc.open(paste0(inputDB, "/KBAThreat")) %>%
  arc.select()

KBAAction <- arc.open(paste0(inputDB, "/KBAAction")) %>%
  arc.select()

KBAHabitat <- arc.open(paste0(inputDB, "/KBAHabitat")) %>%
  arc.select()

KBACitation <- arc.open(paste0(inputDB, "/KBACitation")) %>%
  arc.select()

      # Lookup tables
KBA_Level <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/KBA_Level.csv", sep=",", fileEncoding = "UTF-8-BOM")
KBA_Province <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/KBA_Province.csv", sep=",", fileEncoding = "UTF-8-BOM")
Threats <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/Threats.csv", sep="\t", fileEncoding = "UTF-8-BOM")
Conservation <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/Conservation.csv", sep=",", fileEncoding = "UTF-8-BOM")
System <- read.csv("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/System.csv", sep=",", fileEncoding = "UTF-8-BOM")
Habitat <- read.xlsx("G:/My Drive/KBA Canada Team/1. Source Datasets/Birds Canada/LookUps_2021.09.21/Habitat.xlsx")

#### Only retain information pertaining to ACCEPTED sites ####
# KBASite
KBASite %<>% filter(SiteStatus == 6) # Only retain sites with SiteStatus = 6 (Accepted)

# KBAThreat
KBAThreat %<>% filter(KBASiteID %in% KBASite$KBASiteID)

# KBAAction
KBAAction %<>% filter(KBASiteID %in% KBASite$KBASiteID)

# KBAHabitat
KBAHabitat %<>% filter(KBASiteID %in% KBASite$KBASiteID)

# KBACitation
KBACitation %<>% filter(KBASiteID %in% KBASite$KBASiteID)
  
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
         geometry = geom) %>%
  mutate(SiteID = 1:nrow(.),
         Name_FR = ifelse(is.na(NationalName_FR), Name_EN, NationalName_FR),
         Level_EN = ifelse(grepl("Global", KBALevel), "Global", "National"),
         PercentProtected = NA) %>%
  left_join(., KBA_Province, by=c("Jurisdiction" = "Province_EN")) %>%
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
         Conservation_FR = NA) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Website") %>% pull(Name_BC)))

      # Save
arc.write(paste0(outputDB, "/KBA_Website"), KBA_Website, overwrite = T)

# KBA_Citations
      # Create
KBA_Citations <- KBACitation %>%
  left_join(., st_drop_geometry(KBASite[,c("KBASiteID", "SiteCode")]), by="KBASiteID") %>%
  left_join(., st_drop_geometry(KBA_Site[,c("SiteID", "SiteCode")]), by="SiteCode") %>%
  mutate(KBACitationID = 1:nrow(.),
         ShortReference = ShortCitation,
         LongReference = LongCitation) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_Citations") %>% pull(Name_BC)))
  
      # Save
arc.write(paste0(outputDB, "/KBA_Citations"), KBA_Citations, overwrite = T)

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
  mutate(SystemSiteID = 1:nrow(.)) %>%
  select(all_of(crosswalk %>% filter(Layer_BC == "KBA_System") %>% pull(Name_BC)))
  
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
