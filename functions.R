#######################################################################################
################## KBACanada Pipeline - Custom functions for Pipeline #################
################## Written by Dean Evans                              #################
#######################################################################################

### Opposite of %in% used for filtering -> Not in essentally
`%!in%` <- Negate(`%in%`)


#### Function to get table of current SiteIDs for KBAs and assign them a new one based on if they exist or are new sites


### Function for custom SQL escaping for different column types
updatetextSQL <- function(x,column){
  return(case_when(
    column=="geometry"~paste0('"',column,'" = \'',x,'\''),
    is.na(x) ~ paste0('"',column,'" = Null'),
    all(class(x) %in% c("integer","numeric")) ~ paste0('"',column,'" = ',x),
    all(class(x) %in% c("logical")) ~ paste0('"',column,'" = ',x),
    .default = paste0('"',column,'" = \'',str_replace_all(x, "'", "''"),'\'')
  ))
  
}

### Function to delete any columns with specific numerical IDs
delete.id <- function(.db,tablename="",idname="",ids,min=F){
  if(class(ids) %!in% c("numeric","integer")){stop("ID provided is not numeric or interger.")}
  if(min){
    deleteSQL <- paste0('DELETE FROM "',tablename,'" WHERE "',idname,'" > ',ids)
  } else {
    deleteSQL <- paste0('DELETE FROM "',tablename,'" WHERE "',idname,'" = ',ids)
  }
  #message(deleteSQL)
  for (i in 1:length(deleteSQL)) {
    .db %>% dbExecute(deleteSQL[i])
  }
  
}



### Function that does updates or appends on any table. Full means that the entire table is to be checked.
update.table <-function(.db,tablename="",primarykey="",newdata,existingdata,full=F){
  if(nrow(newdata)>0){
    #### Do data checks
    ###PrimaryKey
    if(!primarykey %in% names(existingdata)){
      stop("Table primary key is not in the original table provided!")}
    if(!primarykey %in% names(newdata)){
      stop("Table primary key is not in the new table provided!")}
    ###Matching Column Names
    if(any(!names(newdata) %in% names(existingdata))){
      stop(paste0("The column(s): '",paste0(names(newdata)[which(!names(newdata) %in% names(existingdata))],collapse = "'; '"),"' are not currently in the database please add this column or check for incorrect naming."))}
    if(any(!names(existingdata) %in% names(newdata))){
      stop(paste0("The column(s): '",paste0(names(existingdata)[which(!names(existingdata) %in% names(newdata))],collapse = "'; '"),"' are missing from your new data. Add this column or check for incorrect naming."))}
    ### Delete extra records if table length is smaller than what is was previously
    if(full){
      if(nrow(newdata)<nrow(existingdata)){
        delete.id(.db,tablename,primarykey,max(newdata[,primarykey]),min=T)
        
      } 
    } else {
      existingdata %<>% filter(!!as.symbol(primarykey) %in% newdata[,primarykey])
    }
    ### Sperate data into add or updates
    existingpks <- existingdata %>% pull(!!as.symbol(primarykey))
    update <- newdata %>% filter(!!as.symbol(primarykey) %in% existingpks)
    add <- newdata %>% filter(!!as.symbol(primarykey) %!in% existingpks)
    ### Do append 
    if(nrow(add)>0){
      .db  %>% dbWriteTable(name = tablename,add,append = TRUE)
      message(nrow(newdata) - nrow(existingdata), " record(s) added to the ",tablename," table.")
    } else {message("No new records to added to the ",tablename," table.")}
    ### Do updates
    if(nrow(update)>0){
      if("sf" %in% class(update)){
        update %<>% rename(shape=geometry) %>% mutate(geometry=st_as_binary(shape,EWKB=T,hex=T)) %>% st_drop_geometry()
        existingdata %<>% rename(shape=geometry) %>% mutate(geometry=st_as_binary(shape,EWKB=T,hex=T)) %>% st_drop_geometry()
        newdata %<>% rename(shape=geometry) %>% mutate(geometry=st_as_binary(shape,EWKB=T,hex=T)) %>% st_drop_geometry()
      }
      diffs <- setdiff(update,existingdata)
      truediff <- existingdata %>% select(-!!as.symbol(primarykey)) %>% setdiff(.,newdata %>% select(-!!as.symbol(primarykey)))
      if(nrow(diffs)>0){
        pks <- diffs %>% pull(primarykey)
        updateSQL <- c()
        for (i in 1:length(pks)) {
          diff <- diffs %>% filter(!!as.symbol(primarykey)==pks[i]) %>% bind_rows("new"=.,"existing"=existingdata %>% filter(!!as.symbol(primarykey) == pks[i]),.id = "datatype")
          diff %<>% select(!!as.symbol(primarykey),where(~ n_distinct(.) > 1)) %>% filter(datatype=="new") %>% select(-datatype)
          diff %<>% mutate(across(!any_of(primarykey),~updatetextSQL(.x,cur_column()))) %>% rowwise() %>%
            mutate(SQL=paste0('Update "',tablename,'" Set ',paste0(across(!any_of(primarykey)),collapse = ", "),' Where "',primarykey,'" = ',!!as.symbol(primarykey)))
          updateSQL <- c(updateSQL,diff$SQL)
          
        }
        SQL <- paste0(updateSQL, collapse = "; ")
        #message(SQL)
        for (i in 1:length(updateSQL)) {
          .db %>% dbExecute(updateSQL[i])
        }
        if(tablename %!in% c("SpeciesAssessment_Subcriterion","EcosystemAssessment_Subcriterion")){
          message(nrow(truediff), " records updated in the ",tablename," table.")
        } else {
          message("Unable to calculate the number records updated in the ",tablename," table.")
        }
        
        
      } else{message("No records to update in the ",tablename," table.")}
      
    } else {message("No records to update in the ",tablename," table.")}
    
  } else {
    message("No records added to the ",tablename," table.")
    message("No records to update in the ",tablename," table.")
  }
}






url_exists <- function(x, non_2xx_return_value = FALSE, quiet = FALSE,...) {
  
  suppressPackageStartupMessages({
    require("httr", quietly = FALSE, warn.conflicts = FALSE)
  })
  
  # you don't need thse two functions if you're alread using `purrr`
  # but `purrr` is a heavyweight compiled pacakge that introduces
  # many other "tidyverse" dependencies and this doesnt.
  
  capture_error <- function(code, otherwise = NULL, quiet = TRUE) {
    tryCatch(
      list(result = code, error = NULL),
      error = function(e) {
        if (!quiet)
          message("Error: ", e$message)
        
        list(result = otherwise, error = e)
      },
      interrupt = function(e) {
        stop("Terminated by user", call. = FALSE)
      }
    )
  }
  
  safely <- function(.f, otherwise = NULL, quiet = TRUE) {
    function(...) capture_error(.f(...), otherwise, quiet)
  }
  
  sHEAD <- safely(httr::HEAD)
  sGET <- safely(httr::GET)
  
  # Try HEAD first since it's lightweight
  res <- sHEAD(x, ...)
  
  if (is.null(res$result) || 
      ((httr::status_code(res$result) %/% 200) != 1)) {
    
    res <- sGET(x, ...)
    
    if (is.null(res$result)) return(NA) # or whatever you want to return on "hard" errors
    
    if (((httr::status_code(res$result) %/% 200) != 1)) {
      if (!quiet) warning(sprintf("Requests for [%s] responded but without an HTTP status code in the 200-299 range", x))
      return(non_2xx_return_value)
    }
    
    return(TRUE)
    
  } else {
    return(TRUE)
  }
  
}

getSpeciesLinks <- function(Species){
  all <- naturecounts::search_species(show = "all")
  Links <- data.frame(SpeciesID=integer(),LinkName_EN=character(),LinkName_FR=character(),URL=character())
  if(nrow(Species)>0){
  for (i in 1:nrow(Species)) {
    if(!Species$Sensitive[i]){
      if(Species$TaxonomicLevel[i]=="Species"){
        if(!is.na(Species$BCSpeciesID[i])){
          conceptid<- all$concept_id[all$species_id==Species$BCSpeciesID[i]]
          Links <- rbind(Links,data.frame(SpeciesID=Species$SpeciesID[i],
                                          LinkName_EN="Avibase - The World Bird Database",
                                          LinkName_FR="Avibase - La base ornithologique mondiale",
                                          URL=paste0("https://avibase.bsc-eoc.org/species.jsp?lang=EN&avibaseid=",conceptid)))}
        if(url_exists(paste0("https://en.wikipedia.org/wiki/",gsub(" ","_",Species$ScientificName[i])))){
          Links <- rbind(Links,data.frame(SpeciesID=Species$SpeciesID[i],
                                          LinkName_EN="Wikipedia",
                                          LinkName_FR="Wikipedia",
                                          URL=paste0("https://en.wikipedia.org/wiki/",gsub(" ","_",Species$ScientificName[i]))))
        }
        ebird <- auk::ebird_species(Species$ScientificName[i],type = c("all"))
        if(!is.na(ebird$species_code[1])){
          if(url_exists(paste0("https://ebird.org/species/",ebird$species_code[1]))){
            Links <- rbind(Links,data.frame(SpeciesID=Species$SpeciesID[i],
                                            LinkName_EN="eBird",
                                            LinkName_FR="eBird",
                                            URL=paste0("https://ebird.org/species/",ebird$species_code[1])))
            Links <- rbind(Links,data.frame(SpeciesID=Species$SpeciesID[i],
                                            LinkName_EN="All About Birds",
                                            LinkName_FR="All About Birds",
                                            URL=paste0("https://www.allaboutbirds.org/guide/",ebird$species_code[1])))
          } 
        }
      }
      
      
      
      if((!is.na(Species$IUCNTaxonID[i])) & Species$InformalTaxonomicGroup[i]=="Birds"){
        
        Links <- rbind(Links,data.frame(SpeciesID=Species$SpeciesID[i],
                                        LinkName_EN="Birdlife International Data Zone",
                                        LinkName_FR="Birdlife International Data Zone",
                                        URL=paste0("http://datazone.birdlife.org/species/factsheet/",Species$IUCNTaxonID[i])))
      }
      inat <- content(GET(paste0("https://api.inaturalist.org/v1/taxa?q=",gsub(" ","%20",Species$ScientificName[i]))))
      if(inat$total_results>0){
        if(inat$results[[1]]$name==Species$ScientificName[i]){
          Links <- rbind(Links,data.frame(SpeciesID=Species$SpeciesID[i],
                                          LinkName_EN="iNaturalist",
                                          LinkName_FR="iNaturalist",
                                          URL=paste0("https://www.inaturalist.org/taxa/",inat$results[[1]]$id)))
          
          
          
        }
        
      }
      
      
    }
  }
  Links <- Links %>% group_by(SpeciesID) %>% arrange(LinkName_EN,.by_group = TRUE)
  Links <- Links %>% ungroup() %>% mutate(SpeciesLinkID=NA) %>% relocate(SpeciesLinkID)
  }
  return(Links)
  
}


delete.sites <- function(.db,sitecodes){
  siteid <- .db %>% tbl("KBA_Site") %>% collect() %>% filter(SiteCode %in% sitecodes) %>% pull(SiteID)
  if(length(siteid)>0){
  .db %>% delete.id("KBA_Website","SiteID",siteid)
  .db %>% delete.id("KBA_Photo","SiteID",siteid)
  .db %>% delete.id("KBA_System","SiteID",siteid)
  .db %>% delete.id("KBA_Habitat","SiteID",siteid)
  .db %>% delete.id("KBA_Threats","SiteID",siteid)
  .db %>% delete.id("KBA_Conservation","SiteID",siteid)
  .db %>% delete.id("KBA_ProtectedArea","SiteID",siteid)
  .db %>% delete.id("KBA_BiodiversityPlot","SiteID",siteid)
  .db %>% delete.id("KBA_Citation","SiteID",siteid)
  assessmentids <- .db %>% tbl("KBA_SpeciesAssessments") %>% collect() %>% 
    filter(SiteID %in% siteid) %>% pull(SpeciesAssessmentsID)
  if(length(assessmentids)>0){
    .db %>% delete.id("SpeciesAssessment_Subcriterion","SpeciesAssessmentsID",assessmentids)
    .db %>% delete.id("KBA_SpeciesAssessments","SpeciesAssessmentsID",assessmentids)}
  ecoassessmentids <- .db %>% tbl("KBA_EcosystemAssessments") %>% collect() %>% 
    filter(SiteID %in% siteid) %>% pull(EcosystemAssessmentsID)
  if(length(ecoassessmentids)>0){
    .db %>% delete.id("EcosystemAssessment_Subcriterion","EcosystemAssessmentsID",ecoassessmentids)
    .db %>% delete.id("KBA_EcosystemAssessments","EcosystemAssessmentsID",ecoassessmentids)}
  .db %>% delete.id("KBA_Site","SiteID",siteid)
  message(length(siteid)," sites deleted.")
  } else {
    message("No sites deleted.")
  }
}

cleanup.species <- function(.db){
  specieswithassessments <- .db %>% tbl("KBA_SpeciesAssessments") %>% collect() %>% 
    pull(SpeciesID)
  speciesids <- .db %>% tbl("Species") %>% collect() %>% 
    filter(SpeciesID %!in% specieswithassessments) %>% pull(SpeciesID)
  if(length(speciesids)>0){
    .db %>% delete.id("Species_Citation","SpeciesID",speciesids)
    .db %>% delete.id("Species_Link","SpeciesID",speciesids)
    .db %>% delete.id("Species_Photo","SpeciesID",speciesids)
    .db %>% delete.id("Species","SpeciesID",speciesids)
    message(length(speciesids)," species cleaned up.")
  } else {
    message("No species needed to be cleaned up.")
  }
}

cleanup.ecosystems <- function(.db){
  ecosystemswithassessments <- .db %>% tbl("KBA_EcosystemAssessments") %>% collect() %>% 
    pull(EcosystemID)
  ecosystemids <- .db %>% tbl("Ecosystem") %>% collect() %>% 
    filter(EcosystemID %!in% ecosystemswithassessments) %>% pull(EcosystemID)
  if(length(ecosystemids)>0){
    .db %>% delete.id("Ecosystem_Citation","EcosystemID",ecosystemids)
    .db %>% delete.id("Ecosystem_Link","EcosystemID",ecosystemids)
    .db %>% delete.id("Ecosystem_Photo","EcosystemID",ecosystemids)
    .db %>% delete.id("Ecosystem","EcosystemID",ecosystemids)
    message(length(ecosystemids)," ecosystems cleaned up.")
  } else {
    message("No ecosystems needed to be cleaned up.")
  }
}

cleanup.footnote <- function(.db){
  ecosystemsfootnotes <- .db %>% tbl("KBA_EcosystemAssessments") %>% collect() %>% 
    pull(FootnoteID)
  speciesfootnotes <- .db %>% tbl("KBA_SpeciesAssessments") %>% collect() %>% 
    pull(FootnoteID)
  footnoteids <- .db %>% tbl("Footnote") %>% collect() %>%
    filter(FootnoteID %!in% c(ecosystemsfootnotes,speciesfootnotes)) %>%
  pull(FootnoteID)
  if(length(footnoteids)>0){
    .db %>% delete.id("Footnote","FootnoteID",footnoteids)
    message(length(footnoteids)," footnotes cleaned up.")
  } else {
    message("No footnotes needed to be cleaned up.")
  }
}

cleanup.internalboundary <- function(.db){
  ecosystemsboundary <- .db %>% tbl("KBA_EcosystemAssessments") %>% collect() %>% 
    pull(InternalBoundaryID)
  speciesboundary <- .db %>% tbl("KBA_SpeciesAssessments") %>% collect() %>% 
    pull(InternalBoundaryID)
  boundaryids <- .db %>% tbl("InternalBoundary") %>% collect() %>%
    filter(InternalBoundaryID %!in% c(ecosystemsboundary,speciesboundary)) %>%
  pull(InternalBoundaryID)
  if(length(boundaryids)>0){
    .db %>% delete.id("InternalBoundary","InternalBoundaryID",boundaryids)
    message(length(boundaryids)," Internal Boundaries cleaned up.")
  } else {
    message("No Internal Boundaries needed to be cleaned up.")
  }
}

pipeline.email <- function(to=c(),password="",message="",subject="KBA Canada Pipeline"){
  mailR::send.mail(from = "pipeline@kbacanada.com",
                   to = to,
                   subject = subject,
                   body = message,
                   html = T,
                   smtp = list(host.name = "live.smtp.mailtrap.io", port = 587,
                               user.name = "api",
                               passwd = password, ssl = TRUE),
                   authenticate = TRUE,
                   send = TRUE)
  
}


updateSpeciesNames <- function(species){
  if(!all(c("TaxonomicLevel","CommonName_EN","CommonName_FR","Population_EN","Population_FR","Subspecies_EN","Subspecies_FR") %in% names(species))){stop("Missing columns in the input species table.")}
  New_Species <- species %>% arrange(CommonName_EN)
  for (i in 1:nrow(New_Species)) {
    CommonName_EN <- New_Species$CommonName_EN[i]
    CommonName_FR <- New_Species$CommonName_FR[i]
    TaxonomicLevel <- New_Species$TaxonomicLevel[i]
    if(TaxonomicLevel %in% c("Population","Subspecies")){
      if((!is.na(CommonName_EN)) & TaxonomicLevel=="Population" & str_detect(CommonName_EN," - ")){
        New_Species$Population_EN[i] <- str_split(CommonName_EN, " - ", 2,simplify = T)[,2]
        New_Species$CommonName_EN[i] <- str_split(CommonName_EN, " - ", 2,simplify = T)[,1]
        CommonName_EN <- str_split(CommonName_EN, " - ", 2,simplify = T)[,1]
      }
      if((!is.na(CommonName_FR)) & TaxonomicLevel=="Population" & str_detect(CommonName_FR," - ")){
        New_Species$Population_FR[i] <- str_split(CommonName_FR, " - ", 2,simplify = T)[,2]
        New_Species$CommonName_FR[i] <- str_split(CommonName_FR, " - ", 2,simplify = T)[,1]
        CommonName_FR <- str_split(CommonName_FR, " - ", 2,simplify = T)[,1]
      }
      if((!is.na(CommonName_EN)) & str_detect(CommonName_EN,"subspecies")){
        strings <- str_split_1(CommonName_EN," ")
        index <- which(str_detect(strings,"subspecies"))
        New_Species$Subspecies_EN[i] <- paste0(strings[c(index-1,index)],collapse = " ")
        New_Species$CommonName_EN[i] <- paste0(strings[-c(index-1,index)],collapse = " ")
        CommonName_EN <- paste0(strings[-c(index-1,index)],collapse = " ")
      }
      if((!is.na(CommonName_FR)) & str_detect(CommonName_FR," de la sous-espèce")){
        New_Species$Subspecies_FR[i] <- paste0("de la sous-espèce",str_split(CommonName_FR, " de la sous-espèce", 2,simplify = T)[,2])
        New_Species$CommonName_FR[i] <- str_split(CommonName_FR, " de la sous-espèce", 2,simplify = T)[,1]
        
      }
      if((!is.na(CommonName_EN)) & TaxonomicLevel=="Subspecies" & str_detect(CommonName_EN," - ")){
        New_Species$Population_EN[i] <- str_split(CommonName_EN, " - ", 2,simplify = T)[,2]
        New_Species$CommonName_EN[i] <- str_split(CommonName_EN, " - ", 2,simplify = T)[,1]
        CommonName_EN <- str_split(CommonName_EN, " - ", 2,simplify = T)[,1]
      }
      if((!is.na(CommonName_FR)) & TaxonomicLevel=="Subspecies" & str_detect(CommonName_FR," - ")){
        New_Species$Population_FR[i] <- str_split(CommonName_FR, " - ", 2,simplify = T)[,2]
        New_Species$CommonName_FR[i] <- str_split(CommonName_FR, " - ", 2,simplify = T)[,1]
        CommonName_FR <- str_split(CommonName_FR, " - ", 2,simplify = T)[,1]
      }
      ### Clean up
      CommonName_EN <- str_replace(CommonName_EN," -$","")
      CommonName_EN <- str_replace(CommonName_EN,",$","")
      New_Species$CommonName_EN[i] <- CommonName_EN
      
      
    }
  }
  return(New_Species)
}



