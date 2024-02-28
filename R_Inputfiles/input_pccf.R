#-----------------
# PCCF+ version 8A
#-----------------

# Load required packages

require(dplyr)
require(readr)

# Input files for Postal Code and related address files

# Set the Datasets folder path
pccf_in <- paste0(getwd(),"/Compressed_Data/pccf_in")

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
                   pattern = "CPC|PCCF")

##-------------------------------------
## Read a fixed width '.txt' files ----
##-------------------------------------

# Air stage delivery list ----
# (from Canada Post website) -
airstage <- readr::read_fwf(
  file.path(installDir,temp[1]),
  fwf_positions(c(1,22,24,27,24),
                c(21,23,26,29,29),
             c("Comm_Name",# Canada Post Community Name
               "PR",# Province
               "FSA",# Forward Sortation Areas
               "LDU",# Local Delivery Units
               "PCODE")),# Postal Code
  locale = locale(encoding = 'LATIN1')
)

# Building name and address file ----
bldgnam <- readr::read_fwf(
  file.path(installDir,temp[2]),
  fwf_positions(c(2,6,8,15,17,116,143),
                c(5,7,14,16,115,142,145),
                c("NumAdr",# Number of address ranges at this postal code
                  "Hosp",# 1=Hospital, health centre, etc...
                  "PCODE",# Postal Code
                  "DMT",# Delivery Mode Type
                  "NameAdr",# Building name and address
                  "Comm_Name",# Canada Post Community Name
                  "PR")),# Province
  locale = locale(encoding = 'LATIN1')
)

# Residential flag ----
emgres <- readr::read_fwf(
  file.path(installDir,temp[3]),
  fwf_positions(c(1,8,10,11,12,14),
                c(7,9,11,12,13,111),
                c("PCODE",# Postal Code
                  "ResFlag",# Flag for primarily residential
                  "DMT",# Delivery Mode Type
                  "DMTDIFF",# Historical Delivery Mode Type if different
                  "H_DMT",# Historical Delivery Mode Type
                  "BldgName")),# Building name
  locale = locale(encoding = 'LATIN1')
)

# Institutional areas -----------------------------
# (determined by census collective dwelling flag) -
instflg <- readr::read_fwf(
  file.path(installDir,temp[4]),
  fwf_positions(c(1,1,1,3,5,9,1,12),
                c(9,10,3,5,9,11,7,13),
                c("DAuid",# Dissemination Area ID
                  "DBuid",# Dissemination Block ID
                  "PR",# Province
                  "CD",# Census Division
                  "DA",# Dissemination Area
                  "DB",# Dissemination Block
                  "PCODE",# Postal Code
                  "InstFlag")),# Flag for primarily residential
  locale = locale(encoding = 'LATIN1')
)

# Number of addresses per postal code ----
# with delivery mode type ----------------
nadr <- readr::read_fwf(
  file.path(installDir,temp[5]),
  fwf_positions(c(1,8,9),
                c(7,9,12),
                c("PCODE",# Postal Code
                  "DMT",# Delivery Mode Type
                  "NumAdr")),# Number of address ranges per postal code
  locale = locale(encoding = 'LATIN1')
)

# Duplicate postal codes from PCCF ----
pccfdups <- readr::read_delim(
  file.path(installDir,temp[6]),
  locale = locale(encoding = 'LATIN1')
)

# Create a 'names' data frame
# names <- as.data.frame(names(pccfdups))

# Modify selected variables
pccfdups <- pccfdups %>%
  dplyr::mutate(
    LAT = as.numeric(LAT),
    LONG = as.numeric(LONG),
    nBLK = as.numeric(nBLK),
    nDA = as.numeric(nDA),
    nCSD = as.numeric(nCSD),
    nCD = as.numeric(nCD),
    nPCODE = as.numeric(nPCODE),
    FED = substr(FEDuid, 3, 3),
    CSD = substr(CSDuid, 5, 3),
    CD = substr(DAuid, 3, 2)
  ) %>%
  dplyr::select(-DAuid, -CDuid, -nPCODE)

# Pointer file for duplicate postal codes ----

pointdup <- readr::read_fwf(
  file.path(installDir,temp[7]),
  fwf_positions(c(1,7,11),
                c(7,11,19),
                c("PCODE",# Postal Code
                  "nPCODE",# Number of records for this Postal Code
                  "ObsDup")),# Observation number for first occurence on duplicates
  locale = locale(encoding = 'LATIN1')
)

# Rural post office codes ----

rpo <- readr::read_delim(
  file.path(installDir,temp[8]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
rpo <- rpo %>%
  dplyr::mutate(
    LAT = as.numeric(LAT),
    LONG = as.numeric(LONG),
    FED = substr(FEDuid, 3, 3),
    CSD = substr(CSDuid, 5, 3)
    ) %>%
  dplyr::select(-DAuid, -CDuid, -nDA, -nCSD, -nCD,
                -nBLK, -nPCODE, -nDAnew,
                -`_keeprec`, -`_abegm4p`,
                -Count, -FSA2)

# Unique postal codes from PCCF ----

pccfuniq <- readr::read_delim(
  file.path(installDir,temp[9]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
pccfuniq <- pccfuniq %>%
  dplyr::mutate(
    LAT = as.numeric(LAT),
    LONG = as.numeric(LONG),
    nBLK = as.numeric(nBLK),
    FED = substr(FEDuid, 3, 3),
    CSD = substr(CSDuid, 5, 3),
    CD = substr(DAuid, 3, 2)
  ) %>%
  dplyr::select(-DAuid, -CDuid, #-nBLK,
                -nPCODE, -Count)

# Weighting for first 2 characters of postal code ----

wc2dups <- readr::read_delim(
  file.path(installDir,temp[10]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
wc2dups <- wc2dups %>%
  dplyr::mutate(
    LAT = as.numeric(LAT),
    LONG = as.numeric(LONG),
    PC2DAWT = as.numeric(PC2DAWT),
    nCD = as.numeric(nCD),
    nDA = as.numeric(nDA),
    nCSD = as.numeric(nCSD),
    PCODE2 = substr(PCODE2, 1, 2)
  ) %>%
  dplyr::select(-DAuid, -CDuid,
                -nBLK, -nPCODE, -Count)

# Weighting for first 3 characters of Postal Code (FSA) ----

wc3dups <- readr::read_delim(
  file.path(installDir,temp[12]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
wc3dups <- wc3dups %>%
  dplyr::mutate(
    LAT = as.numeric(LAT),
    LONG = as.numeric(LONG),
    PC3DAWT = as.numeric(PC3DAWT),
    nCD = as.numeric(nCD),
    nDA = as.numeric(nDA),
    nCSD = as.numeric(nCSD),
    PCODE3 = substr(PCODE3, 1, 3)
    )

# Weighting for first 4 characters of Postal Code ----

wc4dups <- readr::read_delim(
  file.path(installDir,temp[14]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
wc4dups <- wc4dups %>%
  dplyr::mutate(
    LAT = as.numeric(LAT),
    LONG = as.numeric(LONG),
    PC4DAWT = as.numeric(PC4DAWT),
    nCD = as.numeric(nCD),
    nDA = as.numeric(nDA),
    nCSD = as.numeric(nCSD),
    PCODE4 = substr(PCODE4, 1, 4)
  )

# Weighting for first 5 characters of Postal Code ----

wc5dups <- readr::read_delim(
  file.path(installDir,temp[16]),
  locale = locale(encoding = 'LATIN1')
)

# Modify selected variables
wc5dups <- wc5dups %>%
  dplyr::mutate(
    LAT = as.numeric(LAT),
    LONG = as.numeric(LONG),
    PC5DAWT = as.numeric(PC5DAWT),
    nCD = as.numeric(nCD),
    nDA = as.numeric(nDA),
    nCSD = as.numeric(nCSD),
    PCODE5 = substr(PCODE5, 1, 5)
  )

# Pointer for 2-character weighting file ----

wc2point <- readr::read_fwf(
  file.path(installDir,temp[11]),
  fwf_positions(c(1,7,14,19),
                c(2,13,18,23),
                c("PCODE2",# Forward Sortation Area
                  "FirstObs",# Delivery Mode Type
                  "nOBS",
                  "TWT")),# Dissemination Area-Level
  locale = locale(encoding = 'LATIN1')
)

# Pointer for 3-character weighting file (FSA) ----

wc3point <- readr::read_fwf(
  file.path(installDir,temp[13]),
  fwf_positions(c(1,7,14,19),
                c(3,13,18,23),
                c("PCODE3",# Forward Sortation Area
                  "FirstObs",
                  "nOBS",
                  "TWT")),
  locale = locale(encoding = 'LATIN1')
)

# Pointer for 4-character weighting file ----

wc4point <- readr::read_fwf(
  file.path(installDir,temp[15]),
  fwf_positions(c(1,7,14,19),
                c(4,13,18,23),
                c("PCODE4",# Forward Sortation Area
                  "FirstObs",
                  "nOBS",
                  "TWT")),
  locale = locale(encoding = 'LATIN1')
)

# Pointer for 5-character weighting file ----

wc5point <- readr::read_fwf(
  file.path(installDir,temp[17]),
  fwf_positions(c(1,7,14,19),
                c(5,13,18,23),
                c("PCODE5",# Forward Sortation Area
                  "FirstObs",
                  "nOBS",
                  "TWT")),
  locale = locale(encoding = 'LATIN1')
)

# Weighting for postal code ----

wc6dups <- readr::read_fwf(
  file.path(installDir,temp[18]),
  fwf_positions(c(1, 7,7, 9,11,19,22,25,32,33,44,57,58,59,60,61,62,63,64,65),
                c(6,13,8,10,14,21,24,31,32,43,56,57,58,59,60,61,62,63,64,69),
                c("PCODE",# Postal Code
                  "DAuid",# Dissemination Area unique identifier
                  "PR",# Province
                  "CD",
                  "DA",
                  "CSD",# Census Sub-division unique identifier
                  "SAC",# Statistical Area Classification code
                  "CTname",# Census Tract Name (0000.00)
                  "Tracted",# Whether the majority of CT within FSA2 are tracted
                  "LAT",# Latitude
                  "LONG",# Longitude
                  "DMT",# Delivery Mode Type
                  "H_DMT",# Historic Delivery Mode Type
                  "DMTDIFF",# Previous DMT if different from current DMT
                  "Rep_Pt_Type",# Representative Point Type
                  "PCtype",# Postal Code type
                  "nDA",# Number of Dissemination Areas for PCODE
                  "nCD",# Number of Census Divisions for PCODE
                  "nCSD",# Number of Census Sub-divisions for PCODE
                  "PC6DAWT")),# Dissemination Area-Level weight for 6-digit postal codes
  locale = locale(encoding = 'LATIN1')
)

# Pointer for postal code weighting file ----

wc6point <- readr::read_fwf(
  file.path(installDir,temp[19]),
  fwf_positions(c(1,7,14,19),
                c(5,13,18,23),
                c("PCODE",# Postal Code
                  "FirstObs",# Pointer for first observation on wc6dups
                  "nOBS",# Total number of observations for PCODE
                  "TWT")),# Total weight
  locale = locale(encoding = 'LATIN1')
)

