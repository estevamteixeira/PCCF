#------------------------------------------
# PCCF+ version 8A
# Input files for geography reference files
#------------------------------------------

# Load required packages

require(dplyr)
require(readr)

# Set the Datasets folder path
georef_in <- paste0(getwd(),"/Compressed_Data/georef_in")

# Set the installation folder path
installDir <- paste0(getwd(),"/Data")

# Specify year and month of PCCF annual release
# to identify appropriate input files (YYMM);
yymm <- 2212

# Remove the comment of this piece of code if you 
# are using the original '.txt' datasets in the 
# Data folder.
# There is the option of using the derived compressed
# version of these files in the Compressed_Data folder

temp <- list.files(installDir,
                   pattern = "GEO")

##-------------------------------------
## Read a fixed width '.txt' files ----
##-------------------------------------

# Pointer file for dissemination area ----
# and dissemination block ----------------
geo_dablkpnt <- readr::read_fwf(
  file.path(installDir,temp[1]),
  fwf_positions(c(1,1,3,5,9,17,25,33,36),
                c(8,2,4,8,16,24,32,35,38),
                c("DAuid",# Dissemination Area unique identifier
                  "PR",# Province
                  "CD",# Census Division
                  "DA",# Dissemination Area
                  "NBLK",# Number of Dissemination Blocks in DA
                  "FirstObs",# Observation number of first occurrence
                  "DA21pop",# Sum of DB population within DA
                  "SACcode",# SAC = CMA+MIZ
                  "CSD")),# Census Subdivision
  locale = locale(encoding = 'LATIN1')
)

# Subset of geographic attribute file ----
# with Agricultural Regions and historic -
# best-fit correspondence ----------------
geo_gaf21 <- readr::read_delim(
  file.path(installDir,temp[2]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
geo_gaf21 <- geo_gaf21 %>%
  dplyr::mutate(
    DAlat = as.numeric(DAlat),
    DAlong = as.numeric(DAlong),
    DBpop2021 = as.numeric(DBpop2021),
    DBtdwell2021 = as.numeric(DBtdwell2021),
    FED = substr(FEDuid, 3, 3),
    CSD = substr(CSDuid, 5, 3),
    DA11uid = substr(DB11uid, 1, 8)
  )

# Health Region definitions, plus additional variables for PCCF+ ----
geo_hrdef <- readr::read_fwf(
  file.path(installDir,temp[3]),
  fwf_positions(c(1,11,1,3,5,9,20,27,31,91,151,155,215),
                c(10,18,2,4,8,11,22,30,90,150,154,214,274),
                c("DBuid",# Dissemination Block unique identifier -- PR(2)+CD(2)+DA(4)+DB(2)
                  "DAuid",# Dissemination Area unique identifier
                  "PR",# Province
                  "CD",# Census Division
                  "DA",# Dissemination Area
                  "DB",# Dissemination Block
                  "CSD",# Census Subdivision
                  "HRuid",# Health Region unique identifier
                  "HRename",# Health Region name (English)
                  "HRfname",# Health Region name (French)
                  "AHRuid",# Alternate Health Region unique identifier - Ontario PHU only
                  "AHRename",# Alternate Health Region name (English)
                  "AHRfname")),# Alternate Health Region name (French)
  locale = locale(encoding = 'LATIN1')
)

# Neighbourhood income quantiles ----
geo_ses21 <- readr::read_delim(
  file.path(installDir,temp[4]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
geo_ses21 <- geo_ses21 %>%
  dplyr::mutate(
    ATIPPE = ifelse(is.na(ATIPPE), " "),
    BTIPPE = ifelse(is.na(BTIPPE), " ")
  )
