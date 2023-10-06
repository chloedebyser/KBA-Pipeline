# 1. Clean Environment
rm(list = ls())
# 2. Set working directory
setwd("/home/rstudio/KBA-Pipeline")
# 3. Git Pull most recent version
system("git pull")
# 4. Run Pipeline
source("KBA-Pipeline.R")
# 5. Clean Environment
rm(list = ls())
# 6. Garbage collection for ram
gc()