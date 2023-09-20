# 1. Clean Environment
rm(list = ls())
# 2. Git Pull most recent version
system("git pull")
# 3. Run Pipeline
source("KBA-Pipeline.R")
# 4. Clean Environment
rm(list = ls())
# 5. Garbage collection for ram
gc()