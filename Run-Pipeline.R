# Start message that pipeline has started with datetime for log file
message(paste0("\n\n\n\n\nStarting Pipeline at ", Sys.time(),"\n\n\n\n\n"))

# 1. Clean Environment
rm(list = ls())
# 2. Set working directory
setwd("/home/thekbapipeline/KBA-Pipeline")
# 3. Git Pull most recent version
system("sudo git pull")
# 4. Run Pipeline
source("KBA-Pipeline.R")
# 5. Clean Environment
rm(list = ls())
# 6. Garbage collection for ram
gc()

# End message that pipeline has finished with datetime for log file
message(paste0("\n\n\n\n\nFinishing Pipeline at ", Sys.time(),"\n\n\n\n\n"))