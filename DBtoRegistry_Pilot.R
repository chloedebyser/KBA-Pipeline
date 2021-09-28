#### Identification of Canadian KBAs
#### Wildlife Conservation Society - 2021
#### Script by Chloé Debyser

#### KBA-EBAR to KBA Registry - Trial Islands Pilot

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

