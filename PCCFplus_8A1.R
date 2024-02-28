pccf_r <- function(infile){
  
#------------------
# PCCF+ version 8A1
#------------------

# R routine (based on the original SAS reoutine) 
# for automated geographic coding from postal codes
# using the Postal Code Conversion File (PCCF)
# and Weighted Conversion File (WCF)

# Load required packages

require(arrow)
require(data.table)
require(dplyr)
require(readr)

path <- "H:/RCP/RCP_Data/TeixeiEC/PCCF/2023/PCCF8A1"
# 1. Set the installation folder path

installDir <- paste0(path,"/Data/")

# 2. Specify the input data library and input file ----
# Default for the input file is a .parquet file
# The file MUST contain the following fields:
#  ID - Unique identifier, character field, 15 characters default
#  PCODE - Postal Code, character field, 6 characters, no spaces

# inData <- paste0(path,"/Sample/")

# Code to read a parquet file into a RData
# inFile <- arrow::read_parquet(file.path(
#   paste0(inData,"sample.parquet"))) %>% 
#   mutate(
#     ID = as.character(BIRTHID),
#     PCODE = as.character(trimws(DLPSTCOD))
#     ) %>% 
#    select(#-BIRTHID,
#   -DLPSTCOD, -CONTCTID) %>% 
#   arrow_table()

inFile <- infile %>%
    mutate(
      ID = as.character(BIRTHID),
      PCODE = as.character(trimws(DLPSTCOD))
      ) %>%
     select(-BIRTHID,-DLPSTCOD, -CONTCTID) %>%
    arrow_table()

# 3. Output data library and file name 
# (the problem file is indicated with a "_problem" suffix)
  
# Output data path

outData <- paste0(path,"/sample/")

# This output replaces the former HLTHOUT and GEOPROB files

outName <- "sampledat_out"

# 4. Specify the output path and filename for the
# output summary (.pdf)
# This output includes both the former HLTHOUT and
# GEOPROB files

pdfOutput = paste0(path,"/sample/sample_01.pdf")

# 5. Version to run either Residential or Institutional
# coding (residential is default)
#   - Indicate 0 if residential
#   - Indicate 1 if institutional

codeVersion <- 0

# EDITS NOT NORMALLY REQUIRED BELOW THIS POINT
# Some options are specified below that can be edited

# Read in program SOURCE files from the installation ----
# directory

pccf_in <- paste0(path,"/Compressed_Data/pccf_in/")
georef_in <- paste0(path,"/Compressed_Data/georef_in/")

# Load ".parquet" files

temp_pccf <- list.files(pccf_in,
                        pattern = ".parquet")

myfiles1 <- lapply(file.path(paste0(pccf_in,
                                    temp_pccf)),
                   arrow::read_parquet,
                   as_data_frame = FALSE)

temp_georef <- list.files(georef_in,
                          pattern = ".parquet")

myfiles2 <- lapply(file.path(paste0(georef_in,
                                    temp_georef)),
                   arrow::read_parquet,
                   as_data_frame = FALSE)

# Random Seed Value ----
# If the seed value is 0 (default) then computer time
# is used. Change this value as desired to use the
# same seed between PCCF+ trials
# %let seedVal=0;
seedVal <- NULL

# PCCF+ Release Version ----
version <- "8A1"

# Specify seeds
set.seed(seedVal)

# Read in data file with postal codes to be geocoded ----
input_data <- inFile %>%
  mutate(
    PCODE = trimws(PCODE),# Postal Code
    FSA = toupper(substr(PCODE, 1, 3)),# Forward Sortation Areas
    LDU = toupper(substr(PCODE, 4, 6)), # Local Delivery Units
# Generate random number for probabilistic assignment
    RAND2 = runif(n = max(nrow(inFile)))
    ) %>%
  filter(!PCODE %in% c("", " ")) %>%
# To keep the first occurrence of each ID
  distinct(ID, .keep_all = TRUE) %>% 
# Sort the data frame (by PCODE or ID, choose one)
  arrange(BrthYear, PCODE)

tmp_pccf_wcf <- merge(
  myfiles1[[which(grepl("wc6dups", temp_pccf))]]%>%
    mutate(inDup = row_number()),
  myfiles1[[which(grepl("wc6point", temp_pccf))]] %>%
    mutate(inPnt = row_number()) %>% 
    rename(SumWTS = TWT),
  by = "PCODE",
  all = TRUE
) %>%
  filter(!is.na(inDup) & !is.na(inPnt)) %>% 
  mutate(
    CMA = SAC,
    # Change the SAC MIZ codes to CMA
    CMA = ifelse(
      CMA %in% c("996", "997", "998", "999"),
      "000",
      CMA),
    # Rural postal codes
    DMTDIFF = ifelse(
      tolower(DMT) %in% "z" &
        substr(PCODE,2,2) %in% "0",
      "W",
      DMTDIFF
    ),
    RPF = "5",
    DB = "",
    # Calculate weights
    WT = ifelse(
      PC6DAWT > 0,
      PC6DAWT/SumWTS,
      0)) %>%
  group_by(PCODE) %>% 
  mutate(#TWT = 0, # Initialize TWT
         TWT = ifelse(
           row_number() %in% 1,
           WT,
           cumsum(WT)
         ),
         TWT = ifelse(
           row_number() %in% n(),
           1,
           TWT)
  ) %>%
  ungroup() 

tmp_pccf_wcfhit <- merge(
  input_data %>% 
    mutate(inHlth = row_number()),
  myfiles1[[which(grepl("wc6point", temp_pccf))]] %>%
    mutate(inPnt = row_number()),
  by = "PCODE",
  all = TRUE
) 

# Not found on duplicates file

tmp_pccf_wcfmiss <- tmp_pccf_wcfhit %>% 
  filter(!is.na(inHlth) & is.na(inPnt))

# Found on duplicates file
tmp_pccf_wcfhit <- tmp_pccf_wcfhit %>% 
  filter(!is.na(inHlth) & !is.na(inPnt))

# Temporary file with PCODEs not found on duplicates

tmp_pccf_hlthdat <- tmp_pccf_wcfmiss %>% 
  select(ID, PCODE, BrthYear) %>% 
  arrange(BrthYear, PCODE) %>% 
  distinct()

# Do indexed search in records found on duplicates
# using the FirstObs variable
#   - Pointer for first observation on 'wcfhit'

i <- tmp_pccf_wcfhit[,c("ID","FirstObs","RAND2")]

# Get the i-th observation from tmp_pccf_wcf

point <- tmp_pccf_wcf %>%
  # Filter obs by FirstObs variable
  filter(tmp_pccf_wcf[["FirstObs"]] %in% i[["FirstObs"]]) %>% 
  # Add RAND2 field from "tmp_pccf_wcfhit" 
  # stored in i
  merge(i,
        by = "FirstObs") %>%
  # Compare RAND2 and TWT. Create Link_Source
  mutate(Link_Source = ifelse(
    RAND2 <= TWT,
    "C",
    NA
  )) %>%
  # Remove not created Link_Source
  filter(!is.na(Link_Source)) %>%
  group_by(ID) %>%
  # Keep the ones with greatest weight
  filter(WT %in% max(WT)) %>%
  ungroup() %>% 
  # If the weights are equal, keep first appearance
  distinct(ID, .keep_all = TRUE) %>%
  arrange(FirstObs)

## Add Link_Source to "tmp_pccf_wcfhit"
# Keep only selected fields

tmp_pccf_wcfhit <- tmp_pccf_wcfhit %>% 
  # Sort by FirstObs
  arrange(FirstObs) %>% 
  select(setdiff(names(tmp_pccf_wcfhit),
                 names(point))) %>% 
  # Combine Link_Source by column dropping the
  # FirstObs field to avoid duplication
  bind_cols(
    point
    ) %>% 
  # Sort the data frame (by PCODE or ID, choose one)
  arrange(ID, PCODE) %>% 
  select("ID", "PCODE", "BrthYear", "PR", "CD", "DA", "CSD",
         "SAC", "CTname", "Tracted", "LAT", "LONG",
         "DMT", "H_DMT", "RPF", "PCtype", "nDA",
         "nCD", "nCSD", "DMTDIFF", "Link_Source",
         "Rep_Pt_Type"
         )

# Match postal codes that are unique on the PCCF ----
# Since all PCODEs on 'pccfuniq' occur only once on
# the PCCF these are matched to unique geography
# at all levels

### Residential geocoding ----
if(codeVersion == 0){
  tmp_pccf_pccfuniq <- myfiles1[[which(grepl("pccf_pccfuniq", temp_pccf))]] %>%
    mutate(
      CMA = SAC,
      # Change the SAC MIZ codes to CMA
      CMA = ifelse(
        CMA %in% c("996", "997", "998", "999"),
        "000",
        CMA),
      # Missing the Block code (RPF=3)
      DB = ifelse(
        DB %in% "000",
        "",
        DB
      ),
      SLI = 1,
      nCSD=1,
      nCD=1,
      nDA=1
    ) %>% 
    arrange(PCODE, PR, CSD, DA, DB, LAT, LONG)
  ### Institutional geocoding ----
} else if(codeVersion == 1){ 
tmp_pccf_pccfuniq <- bind_rows(
  myfiles1[[which(grepl("pccf_pccfuniq", temp_pccf))]],
  myfiles1[[which(grepl("pccf_rpo", temp_pccf))]]
) %>% 
  mutate(
    CMA = SAC,
    # Change the SAC MIZ codes to CMA
    CMA = ifelse(
      CMA %in% c("996", "997", "998", "999"),
      "000",
      CMA),
    # Missing the Block code (RPF=3)
    DB = ifelse(
      DB %in% "000",
      "",
      DB
    ),
    SLI = 1,
    nCSD=1,
    nCD=1,
    nDA=1
  ) %>%
  arrange(PCODE, PR, CSD, DA, DB, LAT, LONG)
}

### Residential geocoding ----
if(codeVersion == 0){ 
  tmp_pccf_hit1 <- merge(
    # Records not found on duplicate file
    tmp_pccf_hlthdat %>% 
      mutate(inHlth = row_number()),
    tmp_pccf_pccfuniq %>% 
      mutate(inPCCF = row_number()),
    by = "PCODE",
    all = TRUE
  )
  ### Institutional geocoding ----
} else if(codeVersion == 1){
  tmp_pccf_hit1 <- merge(
    input_data %>% # All records: i.e. those not found on duplicate file + rural postal codes which will be assigned to the coords of the RPO
      mutate(inHlth = row_number()),
    tmp_pccf_pccfuniq %>% 
      mutate(inPCCF = row_number()),
    by = "PCODE",
    all = TRUE
  )
}

### Not matched to unique PCODE ----
tmp_pccf_miss1 <- tmp_pccf_hit1 %>% 
  filter(!is.na(inHlth) & 
           is.na(inPCCF)
         )

### Matched to unique PCODE ----
tmp_pccf_hit1 <- tmp_pccf_hit1 %>% 
  filter(!is.na(inHlth) &
           !is.na(inPCCF) &
           !DA %in% "0000")

# Records found on unique postal code file,----
# no further processing ----

tmp_pccf_hit1 <- tmp_pccf_hit1 %>% 
  mutate(
    # Source of geocoding is from PCCF unique records
    Link_Source = "F",
    RPF = "",
    RPF = case_when(
      # Block face
      Rep_Pt_Type %in% "1" ~ "1",
      # DB
      Rep_Pt_Type %in% "2" ~ "2",
      # DA now - later, DB will be imputed within DA
      Rep_Pt_Type %in% "3" ~ "3",
      TRUE ~ RPF
    )
  )

# Specify seeds
set.seed(seedVal)

tmp_pccf_pccfdups <- myfiles1[[which(grepl("pccf_pccfdups", temp_pccf))]] %>%
  mutate(
    CMA = SAC,
    # Change the SAC MIZ codes to CMA
    CMA = ifelse(
      CMA %in% c("996", "997", "998", "999"),
      "000",
      CMA),
    # Missing the Block code (RPF=3)
    DB = ifelse(
      DB %in% "000",
      "",
      DB
    ),
# Randomize records zxy (=LAT LONG) within each
# PCODE so that successive coding sessions can match
# to every possibility rather than favouring the 
# first occurrences on file and generate random num
# for sort w/in PCODE
      RANDOM = runif(n = max(nrow(myfiles1[[which(grepl("pccf_pccfdups", temp_pccf))]])))
  ) %>% 
  arrange(PCODE, RANDOM) %>% 
  mutate(POINTER = row_number())

# Assign sequence number to successive occurrences
# of same postal code. Sequence number is used if
# the PCODE appears more than once on PCCF

tmp_pccf_miss1 <- tmp_pccf_miss1 %>% 
  arrange(PCODE) %>% 
  group_by(PCODE) %>% 
  mutate(
    SEQH = row_number()
    ) %>% 
  ungroup()

tmp_pccf_hit2 <- merge(
  tmp_pccf_miss1 %>% # Records not found on duplicate file
    mutate(inMiss = row_number()),
  myfiles1[[which(grepl("pccf_pointdup", temp_pccf))]] %>% 
    arrange(PCODE) %>% 
    mutate(inPoint = row_number()),
  by = "PCODE",
  all = TRUE
)

tmp_pccf_miss2 <- tmp_pccf_hit2 %>% 
  filter(!is.na(inMiss) &
           is.na(inPoint))

# Records matched to dups file will be uniformly
# distributed among applicable DA/BLK for that PCODE
tmp_pccf_hit2 <- tmp_pccf_hit2 %>% 
  filter(!is.na(inMiss) &
           !is.na(inPoint)) %>% 
  group_by(PCODE) %>%
  # If matched to duplicate PCODE record select the
  # all are used
  mutate(
    # next record which matches starting over when
    OFFSET = SEQH %% nPCODE,
    POINTER = ObsDup + OFFSET
  ) %>% 
  ungroup()

tmp_pccf_miss2 <- tmp_pccf_miss2 %>% 
  # DMT missing since not matched to pccf(6)
  # no geography assigned yet to "pccf_miss2"
  mutate(
    DMT = ifelse(
      is.na(DMT) | DMT %in% c(""," "),
      "9",
    DMT
  )) %>% 
  select(ID, BrthYear, PCODE, DMT)

# For postal codes which match on "pccf_pointdup"
# file get full geography from "pccf_pccfdups" file
# Do indexed search in records found on duplicates

i2 <- tmp_pccf_hit2 %>% 
  select(ID,PCODE,POINTER) %>% 
  mutate(POINTER2 = POINTER)

# Get the i-th observation from "pccf_pccfdups"

point2 <- tmp_pccf_pccfdups %>%
  # Merge with specified record on "pccf_pccfdups"
  filter(tmp_pccf_pccfdups[["POINTER"]] %in% i2[["POINTER2"]]) %>% 
  # Create Link_Source
  # Source of geocoding is from PCCF duplicate records
  mutate(Link_Source = "D",
         RPF = "",
         RPF = case_when(
           # Block face
           Rep_Pt_Type %in% "1" ~ "1",
           # DB
           Rep_Pt_Type %in% "2" ~ "2",
           # DA now - later, DB will be imputed within DA
           Rep_Pt_Type %in% "3" ~ "3",
           TRUE ~ RPF
           )
         )

## Add Geocoding info to "pccf_hit2"

tmp_pccf_hit2 <- merge(
  tmp_pccf_hit2 %>% 
    select(POINTER, setdiff(names(tmp_pccf_hit2),
                   names(point2))),
  point2,
  by = c("POINTER")
) %>% 
  arrange(PCODE)

# Imputes full geography from first 5 digits ----
# of postal code for "pccf_miss2" not matched previously

tmp_pccf_wc5 <- myfiles1[[which(grepl("pccf_wc5dups", temp_pccf))]] %>% 
  mutate(
    CMA = SAC,
    # Change the SAC MIZ codes to CMA
    CMA = ifelse(
      CMA %in% c("996", "997", "998", "999"),
      "000",
      CMA),
    RPF = "6",
    DB = "")

tmp_pccf_wc5 <- merge(
  tmp_pccf_wc5,
  myfiles1[[which(grepl("wc5point", temp_pccf))]] %>% 
    rename(SumTWT = TWT),
  by = "PCODE5",
  all = TRUE
) %>%
  mutate(
    # Calculate weights
    WT = ifelse(
      PC5DAWT > 0,
      PC5DAWT/SumTWT,
      0)) %>%
  group_by(PCODE5) %>%
  mutate(#TWT = 0, # Initialize TWT
         TWT = ifelse(
           row_number() %in% 1,
           WT,
           cumsum(WT)
         ),
         TWT = ifelse(
           row_number() %in% n(),
           1,
           TWT)
  ) %>% 
  ungroup()

# Specify seeds
set.seed(seedVal)

tmp_pccf_miss2 <- tmp_pccf_miss2 %>% 
  mutate(
    # First 5 characters of postal code
    PCODE5 = substr(PCODE, 1, 5),
    # Generate random number for probabilistic assignment
    RAND2 = runif(n = max(nrow(tmp_pccf_miss2)))
  ) %>% 
  # Order rows by PCODE5 and RAND2
  arrange(PCODE5, RAND2) 

tmp_pccf_wc5hit <- merge(
  tmp_pccf_miss2 %>% 
    mutate(inHlth = row_number()),
  myfiles1[[which(grepl("wc5point", temp_pccf))]] %>% 
    mutate(inPnt = row_number()),
  by = "PCODE5",
  all = TRUE
) 

### Not matched to unique PCODE5 ----
tmp_pccf_wc5miss <- tmp_pccf_wc5hit %>% 
  filter(!is.na(inHlth) & is.na(inPnt))

### Matched to unique PCODE5 ----
tmp_pccf_wc5hit <- tmp_pccf_wc5hit %>% 
  filter(!is.na(inHlth) & !is.na(inPnt))

# Do indexed search in records found on duplicates
# using the FirstObs variable
#   - Pointer for first observation on wc5hit

i3 <- tmp_pccf_wc5hit[,c("ID","FirstObs","RAND2")]

# Get the i-th observation from tmp_pccf_wc5

point3 <- as.data.frame(tmp_pccf_wc5) %>% 
  filter(tmp_pccf_wc5[["FirstObs"]] %in% i3[["FirstObs"]]) %>% 
  # Add RAND2 field from "tmp_pccf_wc5hit" 
  # stored in i
  merge(i3,
        by = "FirstObs") %>% 
  # Compare RAND2 and TWT. Create Link_Source
  # Geocoding source is 5-character imputation
  mutate(Link_Source = ifelse(
    RAND2 <= TWT,
    "5",
    NA
  )) %>% 
  # Remove not created Link_Source
  filter(!is.na(Link_Source)) %>%
  group_by(ID) %>%
  # Keep the ones with greatest weight
  filter(WT %in% max(WT)) %>%
  ungroup() %>% 
  # If the weights are equal, keep first appearance
  distinct(ID, .keep_all = TRUE) %>%
  arrange(FirstObs) 

## Add Link_Source to "pccf_wc5hit"
# Keep only selected fields

tmp_pccf_wc5hit <- tmp_pccf_wc5hit %>% 
  # Sort by FirstObs
  arrange(FirstObs) %>% 
  select(setdiff(names(tmp_pccf_wc5hit),
                 names(point3))) %>% 
  # Combine Link_Source by column dropping the
  # FirstObs field to avoid duplication
  cbind(
    point3
  ) %>% 
  # Sort the data frame (by PCODE or ID, choose one)
  arrange(ID, PCODE)

tmp_pccf_wc5miss <- tmp_pccf_wc5miss %>% 
  mutate(
    FSA = substr(PCODE, 1, 3),
    DMT = "9"
  ) %>% 
  select(ID, BrthYear, PCODE, FSA, DMT)

# Imputes full geography from first 4 digits ----
# of postal code for 'wc5miss' not matched previously

tmp_pccf_wc4 <- myfiles1[[which(grepl("pccf_wc4dups", temp_pccf))]] %>% 
  mutate(
    CMA = SAC,
    # Change the SAC MIZ codes to CMA
    CMA = ifelse(
      CMA %in% c("996", "997", "998", "999"),
      "000",
      CMA),
    RPF = "6",
    DB = "")

tmp_pccf_wc4 <- merge(
  tmp_pccf_wc4,
  myfiles1[[which(grepl("wc4point", temp_pccf))]] %>% 
    rename(SumTWT = TWT),
  by = "PCODE4",
  all = TRUE
) %>%
  mutate(
    # Calculate weights
    WT = ifelse(
      PC4DAWT > 0,
      PC4DAWT/SumTWT,
      0)) %>%
  group_by(PCODE4) %>%
  mutate(#TWT = 0, # Initialize TWT
         TWT = ifelse(
           row_number() %in% 1,
           WT,
           cumsum(WT)
         ),
         TWT = ifelse(
           row_number() %in% n(),
           1,
           TWT)
  ) %>% 
  ungroup() 

# Specify seeds
set.seed(seedVal)

tmp_pccf_wc5miss <- tmp_pccf_wc5miss %>% 
  mutate(
    # First 4 characters of postal code
    PCODE4 = substr(PCODE, 1, 4),
    # Generate random number for probabilistic assignment
    RAND2 = runif(n = max(nrow(tmp_pccf_wc5miss)))
  ) %>% 
  # Order rows by PCODE4 and RAND2
  arrange(PCODE4, RAND2) 

tmp_pccf_wc4hit <- merge(
  tmp_pccf_wc5miss %>% 
    mutate(inHlth = row_number()),
  myfiles1[[which(grepl("wc4point", temp_pccf))]] %>% 
    mutate(inPnt = row_number()),
  by = "PCODE4",
  all = TRUE
)

### Not matched to unique PCODE4 ----
tmp_pccf_wc4miss <- tmp_pccf_wc4hit %>% 
  filter(!is.na(inHlth) & is.na(inPnt))

### Matched to unique PCODE4 ----
tmp_pccf_wc4hit <- tmp_pccf_wc4hit %>% 
  filter(!is.na(inHlth) & !is.na(inPnt))

# Do indexed search in records found on duplicates
# using the FirstObs variable
#   - Pointer for first observation on wc4hit

i4 <- tmp_pccf_wc4hit[,c("ID","FirstObs","RAND2")]

# Get the i-th observation from tmp_pccf_wc4

point4 <- tmp_pccf_wc4 %>% 
  filter(tmp_pccf_wc4[["FirstObs"]] %in% i4[["FirstObs"]]) %>% 
  # Add RAND2 field from "tmp_pccf_wc4hit" 
  # stored in i4
  merge(i4,
        by = "FirstObs") %>% 
  # Compare RAND2 and TWT. Create Link_Source
  # Geocoding source is 4-character imputation
  mutate(Link_Source = ifelse(
    RAND2 <= TWT,
    "4",
    NA
  )) %>% 
  # Remove not created Link_Source
  filter(!is.na(Link_Source)) %>%
  group_by(ID) %>%
  # Keep the ones with greatest weight
  filter(WT %in% max(WT)) %>%
  ungroup() %>% 
  # If the weights are equal, keep first appearance
  distinct(ID, .keep_all = TRUE) %>%
  arrange(FirstObs) 

## Add Link_Source to "pccf_wc4hit"
# Keep only selected fields

tmp_pccf_wc4hit <- tmp_pccf_wc4hit %>% 
  # Sort by FirstObs
  arrange(FirstObs) %>% 
  select(setdiff(names(tmp_pccf_wc4hit),
                 names(point4))) %>% 
  # Combine Link_Source by column dropping the
  # FirstObs field to avoid duplication
  cbind(
    point4
  ) %>% 
  # Sort the data frame (by PCODE or ID, choose one)
  arrange(ID, PCODE)

tmp_pccf_wc4miss <- tmp_pccf_wc4miss %>% 
  mutate(
    DMT = "9"
  ) %>% 
  select(ID, PCODE, BrthYear, DMT)

# Determine records to either recode ----
# (using FSA and below) or to keep
# (norecode)

tmp_pccf_norecode <-
  bind_rows(
    tmp_pccf_hit1 %>% 
      mutate_all(as.character),
    tmp_pccf_hit2%>% 
      mutate_all(as.character),
    tmp_pccf_wc4miss%>% 
      mutate_all(as.character)
    ) %>% 
      mutate(
        PCODE3 = substr(PCODE, 1, 3)
        ) 

### Residential Coding ----
if(codeVersion %in% 0){
  
  tmp_pccf_recode0 <- tmp_pccf_norecode %>% 
    filter(
      # With delivery type not included
      DMT %in% "9" | 
        toupper(DMT) %in% c("H","J","K","M","R","T","X") |
        (toupper(DMT) %in% c("Z") &
           toupper(DMTDIFF) %in% c("H","J","K","M","R","T","X"))
    )
  
  tmp_pccf_norecode <- tmp_pccf_norecode %>% 
    filter(
      # 'W' delivery type not included
      !( DMT %in% "9" | 
        toupper(DMT) %in% c("H","J","K","M","R","T","X") |
        ( toupper(DMT) %in% c("Z") &
           toupper(DMTDIFF) %in% c("H","J","K","M","R","T","X") )
       )
    )
### Institutional coding ----
} else if(codeVersion %in% 1){
  tmp_pccf_recode0 <- tmp_pccf_norecode %>% 
    filter(
      # With delivery type not included
      DMT %in% "9" | 
        toupper(DMT) %in% c("H","J","K","M","R","T","X") |
        (toupper(DMT) %in% c("Z") &
           toupper(DMTDIFF) %in% c("H","J","K","M","R","T","X"))
    )
  
  tmp_pccf_norecode <- tmp_pccf_norecode %>% 
    filter(
      # 'W' and 'M' delivery type not included
      !( DMT %in% "9" | 
           toupper(DMT) %in% c("H","J","K","R","T","X") |
           ( toupper(DMT) %in% c("Z") &
               toupper(DMTDIFF) %in% c("H","J","K","R","T","X") )
      )
    )
}

# Imputes SGC from FSA (first 3 characters) ----
# for problem records not matched previously

tmp_pccf_wc3dups <- myfiles1[[which(grepl("pccf_wc3dups", temp_pccf))]] %>% 
  mutate(
    CMA = SAC,
    # Change the SAC MIZ codes to CMA
    CMA = ifelse(
      CMA %in% c("996", "997", "998", "999"),
      "000",
      CMA)
    )

tmp_pccf_wc3dups <- merge(
  tmp_pccf_wc3dups %>% 
    mutate(inDups = row_number()),
  myfiles1[[which(grepl("wc3point", temp_pccf))]] %>% 
    mutate(inPnt = row_number()) %>% 
    rename(SumTWT = TWT),
  by = "PCODE3",
  all = TRUE
) %>%
  filter(!is.na(inDups)) %>% 
  mutate(
    # Calculate weights
    WT = ifelse(
      PC3DAWT > 0,
      PC3DAWT/SumTWT,
      0)) %>%
  group_by(PCODE3) %>%
  mutate(#TWT = 0, # Initialize TWT
         TWT = ifelse(
           row_number() %in% 1,
           WT,
           cumsum(WT)
         ),
         TWT = ifelse(
           row_number() %in% n(),
           1,
           TWT)
  ) %>% 
  ungroup() 

# Specify seeds
set.seed(seedVal)

tmp_pccf_uimpute <- tmp_pccf_recode0 %>% 
  mutate(
    # Generate random number for probabilistic assignment
    RAND3 = runif(n = max(nrow(tmp_pccf_recode0)))
  ) %>% 
  # Order rows by PCODE3 and RAND3
  arrange(PCODE3, RAND3) 

tmp_pccf_wc3hit <- merge(
  tmp_pccf_uimpute %>% 
    mutate(inHlth = row_number()),
  myfiles1[[which(grepl("wc3point", temp_pccf))]] %>% 
    mutate(inPnt = row_number()),
  by = "PCODE3",
  all = TRUE
) %>% 
  mutate(
    # Since PCCF_WCDUPS does not have the 
    # DB and we do not want to retain the 
    # old DB as it will result in a mismatch,
    # reset DB to missing
    DB = ""
  ) 

### Not matched to unique PCODE3 ----
tmp_pccf_wc3miss <- tmp_pccf_wc3hit %>% 
  filter(!is.na(inHlth) & is.na(inPnt))

### Matched to unique PCODE3 ----
tmp_pccf_wc3hit <- tmp_pccf_wc3hit %>% 
  filter(!is.na(inHlth) & !is.na(inPnt))

# Do indexed search in records found on duplicates
# using the FirstObs variable
#   - Pointer for first observation on wc3dups

i5 <- tmp_pccf_wc3hit[,c("ID","FirstObs","RAND3")]

# Get the i-th observation from tmp_pccf_wc3dups

point5 <- as.data.frame(tmp_pccf_wc3dups) %>% 
  filter(tmp_pccf_wc3dups[["FirstObs"]] %in% i5[["FirstObs"]]) %>% 
  # Add RAND3 field from "tmp_pccf_wc3hit" 
  # stored in i5
  merge(i5,
        by = "FirstObs") %>% 
  # Compare RAND3 and TWT. Create Link_Source
  # Geocoding source is 3-character imputation
  mutate(Link_Source = ifelse(
    # Imputed from FSA-DA population weights
    RAND3 < TWT,
    "3",
    NA
  ),
  RPF = ifelse(
    # Imputed from FSA-DA population weights
    RAND3 < TWT,
    "6",
    NA
  )) %>% 
  # Remove not created Link_Source
  filter(!is.na(Link_Source)) %>%
  group_by(ID) %>%
  # Keep the ones with greatest weight
  filter(WT %in% max(WT)) %>%
  ungroup() %>% 
  # If the weights are equal, keep first appearance
  distinct(ID, .keep_all = TRUE) %>%
  arrange(FirstObs) 

## Add Link_Source to "pccf_wc3hit"
# Keep only selected fields

tmp_pccf_wc3hit <- tmp_pccf_wc3hit %>% 
  # Sort by FirstObs
  arrange(FirstObs) %>% 
  select(setdiff(names(tmp_pccf_wc3hit),
                 names(point5))) %>% 
  # Combine Link_Source by column dropping the
  # FirstObs field to avoid duplication
  cbind(
    point5
  ) %>% 
  # Sort the data frame (by PCODE or ID, choose one)
  arrange(ID, PCODE)

if(codeVersion %in% 0){
  tmp_pccf_wc3hit <- tmp_pccf_wc3hit %>% 
  # Note: the service areas of above DMTs can be
  # outside the FSA so don't impute full geography
  # Note that DB LAT LONG retained here, however,
  # from FSA input
    mutate(
      DA = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
    # If rural only assign CMA, PR/CMA and CD;
    # if urban retain all
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
      "9999",
    # Urban and DMT=ABEG
      ifelse(
        !substr(PCODE, 2, 2) %in% "0" &
          toupper(DMT) %in% c("A", "B", "E", "G"),
        "9999",
        DA
      )
      ),
      DB = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
        "",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "",
          DB
        )
      ),
      CSD = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
        "999",
        CSD
      ),
      CTname = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")) &
          Tracted %in% "1",
        "9999.99",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "9999.99",
          CTname
        )
      )
    )
} else if(codeVersion %in% 1){
  tmp_pccf_wc3hit <- tmp_pccf_wc3hit %>% 
    mutate(
    DA = ifelse(
      substr(PCODE, 2, 2) %in% "0" |
        toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
        # If rural only assign CMA, PR/CMA and CD;
        # if urban retain all
        (toupper(DMT) %in% "Z" & 
           toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
      "9999",
      # Urban and DMT=ABEG
      ifelse(
        !substr(PCODE, 2, 2) %in% "0" &
          toupper(DMT) %in% c("A", "B", "E", "G"),
        "9999",
        DA
      )
    ),
    DB = ifelse(
      substr(PCODE, 2, 2) %in% "0" |
        toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
        (toupper(DMT) %in% "Z" & 
           toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
      "",
      # Urban and DMT=ABEG
      ifelse(
        !substr(PCODE, 2, 2) %in% "0" &
          toupper(DMT) %in% c("A", "B", "E", "G"),
        "",
        DB
      )
    ),
    CSD = ifelse(
      substr(PCODE, 2, 2) %in% "0" |
        toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
        (toupper(DMT) %in% "Z" & 
           toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
      "999",
      CSD
    ),
    CTname = ifelse(
      substr(PCODE, 2, 2) %in% "0" |
        toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
        (toupper(DMT) %in% "Z" & 
           toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")) &
        Tracted %in% "1",
      "9999.99",
      # Urban and DMT=ABEG
      ifelse(
        !substr(PCODE, 2, 2) %in% "0" &
          toupper(DMT) %in% c("A", "B", "E", "G"),
        "9999.99",
        CTname
      )
    )
  )
}

tmp_pccf_wc3miss <- tmp_pccf_wc3miss %>% 
  select(ID, PCODE, PCODE3, BrthYear, DMT, H_DMT,
         DMTDIFF, RAND3)

# Imputes SGC from FSA for problem records not ----
# matched previously (1&2 characters of FSA)

tmp_pccf_wc2dups <- myfiles1[[which(grepl("pccf_wc2dups", temp_pccf))]] %>% 
  mutate(
    CMA = SAC,
    # Change the SAC MIZ codes to CMA
    CMA = ifelse(
      CMA %in% c("996", "997", "998", "999"),
      "000",
      CMA))

tmp_pccf_wc2dups <- merge(
  tmp_pccf_wc2dups %>% 
    mutate(inDups = row_number()),
  myfiles1[[which(grepl("wc2point", temp_pccf))]] %>% 
    mutate(inPnt = row_number()) %>% 
    rename(SumTWT = TWT),
  by = "PCODE2",
  all = TRUE
) %>%
  filter(!is.na(inDups)) %>% 
  mutate(
    # Calculate weights
    WT = ifelse(
      PC2DAWT > 0,
      PC2DAWT/SumTWT,
      0)) %>%
  group_by(PCODE2) %>%
  mutate(#TWT = 0, # Initialize TWT
         TWT = ifelse(
           row_number() %in% 1,
           WT,
           cumsum(WT)
         ),
         TWT = ifelse(
           row_number() %in% n(),
           1,
           TWT)
  ) %>% 
  ungroup() 

tmp_pccf_recode <- tmp_pccf_wc3miss %>% 
  mutate(
    PCODE2 = substr(PCODE, 1, 2)
  ) %>% 
  select("ID", "PCODE", "PCODE2", "BrthYear", "DMT", "H_DMT", "DMTDIFF", "RAND3") %>% 
  arrange(PCODE)

tmp_pccf_wc2hit <- merge(
  tmp_pccf_recode %>% 
    mutate(inRecode = row_number()),
  myfiles1[[which(grepl("wc2point", temp_pccf))]] %>% 
    mutate(inPnt = row_number()),
  # FSA
  by = "PCODE2",
  all = TRUE
) 

### Not matched to unique PCODE2 ----
tmp_pccf_wc2miss <- tmp_pccf_wc2hit %>% 
  filter(!is.na(inRecode) & is.na(inPnt))

### Matched to unique PCODE2 ----
tmp_pccf_wc2hit <- tmp_pccf_wc2hit %>% 
  filter(!is.na(inRecode) & !is.na(inPnt))

# Do indexed search in records found on duplicates
# using the FirstObs variable
#   - Pointer for first observation on wc3dups

i6 <- tmp_pccf_wc2hit[,c("ID","FirstObs","RAND3")]

# Get the i-th observation from tmp_pccf_wc2dups

point6 <- tmp_pccf_wc2dups %>% 
  filter(tmp_pccf_wc2dups[["FirstObs"]] %in% i6[["FirstObs"]]) %>% 
  # Add RAND3 field from "tmp_pccf_wc2hit" 
  # stored in i6
  merge(i6,
        by = "FirstObs") %>% 
  # Compare RAND3 and TWT. Create Link_Source
  # Geocoding source is 2-character imputation
  mutate(Link_Source = ifelse(
    # Imputed from FSA-DA population weights
    RAND3 < TWT,
    "2",
    NA
  ),
  RPF = ifelse(
    # PR and CMA imputed within first 2-characters
    RAND3 < TWT,
    "8",
    NA
  )) %>% 
  # Remove not created Link_Source
  filter(!is.na(Link_Source)) %>%
  group_by(ID) %>%
  # Keep the ones with greatest weight
  filter(WT %in% max(WT)) %>%
  ungroup() %>% 
  # If the weights are equal, keep first appearance
  distinct(ID, .keep_all = TRUE) %>%
  arrange(FirstObs) 

## Add Link_Source to "pccf_wc2hit"
# Keep only selected fields

tmp_pccf_wc2hit <- tmp_pccf_wc2hit %>% 
  # Sort by FirstObs
  arrange(FirstObs) %>% 
  select(setdiff(names(tmp_pccf_wc2hit),
                 names(point6))) %>% 
  # Combine Link_Source by column dropping the
  # FirstObs field to avoid duplication
  cbind(
    point6 
  ) %>% 
  # Sort the data frame (by PCODE or ID, choose one)
  arrange(ID, PCODE)

if(codeVersion %in% 0){
  tmp_pccf_wc2hit <- tmp_pccf_wc2hit %>% 
    # Note: the service areas of above DMTs can be
    # outside the FSA so don't impute full geography
    # Note that DB LAT LONG retained here, however,
    # from FSA input
    mutate(
      FED = NA,
      ER = NA,
      CTname = NA,
      DB = NA,
      DA = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          # If rural only assign CMA, PR/CMA and CD;
          # if urban retain all
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
        "9999",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "9999",
          DA
        )
      ),
      DB = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
        "",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "",
          DB
        )
      ),
      FED = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
        "999",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "999",
          FED
        )
      ),
      ER = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
        "99",
        ER
      ),
      CSD = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")),
        "999",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "999",
          CSD
        )
      ),
      CTname = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")) &
          Tracted %in% "1",
        "9999.99",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "9999.99",
          CTname
        )
      )
    )
} else if(codeVersion %in% 1){
  tmp_pccf_wc2hit <- tmp_pccf_wc2hit %>% 
    mutate(
      FED = NA,
      ER = NA,
      CTname = NA,
      DB = NA,
      DA = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
          # If rural only assign CMA, PR/CMA and CD;
          # if urban retain all
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
        "9999",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "9999",
          DA
        )
      ),
      DB = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
        "",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "",
          DB
        )
      ),
      FED = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
          # If rural only assign CMA, PR/CMA and CD;
          # if urban retain all
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
        "999",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "999",
          FED
        )
      ),
      ER = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
          # If rural only assign CMA, PR/CMA and CD;
          # if urban retain all
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
        "99",
        ER
      ),
      CSD = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")),
        "999",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "999",
          CSD
        )
      ),
      CTname = ifelse(
        substr(PCODE, 2, 2) %in% "0" |
          toupper(DMT) %in% c("H", "J", "K", "R", "T", "X") |
          (toupper(DMT) %in% "Z" & 
             toupper(DMTDIFF) %in% c("H", "J", "K", "R", "T", "X")) &
          Tracted %in% "1",
        "9999.99",
        # Urban and DMT=ABEG
        ifelse(
          !substr(PCODE, 2, 2) %in% "0" &
            toupper(DMT) %in% c("A", "B", "E", "G"),
          "9999.99",
          CTname
        )
      )
    )
}

tmp_pccf_wc2miss <- tmp_pccf_wc2miss %>% 
  mutate(
    PCODE1 = substr(PCODE, 1, 1),
    PR = case_when(
      toupper(PCODE1) %in% "A" ~ "10",
      toupper(PCODE1) %in% "B" ~ "12",
      toupper(PCODE1) %in% "C" ~ "11",
      toupper(PCODE1) %in% "E" ~ "13",
      toupper(PCODE1) %in% c("G","H","J") ~ "24",
      toupper(PCODE1) %in% c("K","L","M","N","P") ~ "35",
      toupper(PCODE1) %in% "R" ~ "46",
      toupper(PCODE1) %in% "S" ~ "47",
      toupper(PCODE1) %in% "T" ~ "48",
      toupper(PCODE1) %in% "V" ~ "59",
      # X is always NWT
      toupper(PCODE1) %in% "X" ~ "61",
      toupper(PCODE1) %in% "Y" ~ "60",
      TRUE ~ "99"
    ),
    Link_Source = case_when(
      toupper(PCODE1) %in% c("A","B","C","E","G",
                             "H","J","K","L","M",
                             "N","P","R","S","T",
                             "V","Y") ~ "1",
      TRUE ~ "0"
    ),
    RPF = "9",
    DMT = "9"
  )

# Merge recoded problem files back into main hlth file in 2 parts: ----
# records with block & those without block

if(codeVersion %in% 0){
  tmp_pccf_hblk <- bind_rows(
    tmp_pccf_wcfhit %>% 
      mutate_all(as.character),
    tmp_pccf_wc5hit%>% 
      mutate_all(as.character),
    tmp_pccf_wc4hit%>% 
      mutate_all(as.character),
    tmp_pccf_norecode%>% 
      mutate_all(as.character),
    tmp_pccf_wc3hit%>% 
      mutate_all(as.character),
    tmp_pccf_wc2hit%>% 
      mutate_all(as.character), 
    tmp_pccf_wc2miss%>% 
      mutate_all(as.character)
  ) %>% 
    select(
      ID,PCODE,BrthYear,PR,CD,DA,CSD,CSDname,CSDtype,SAC,CTname,Tracted,FED,DB,Rep_Pt_Type,
      LAT,LONG,SLI,PCtype,Comm_Name,DMT,H_DMT,DMTDIFF,PO,QI,Source,Link_Source,RPF,CMA,
      POP_CNTR_RA,POP_CNTR_RA_type,POP_CNTR_RA_SIZE_CLASS,nCD,nCSD,nDA,nBLK
    )
} else if(codeVersion %in% 1){
  tmp_pccf_hblk <- bind_rows(
    tmp_pccf_wc5hit%>% 
      mutate_all(as.character),
    tmp_pccf_wc4hit%>% 
      mutate_all(as.character),
    tmp_pccf_norecode%>% 
      mutate_all(as.character),
    tmp_pccf_wc3hit%>% 
      mutate_all(as.character),
    tmp_pccf_wc2hit%>% 
      mutate_all(as.character), 
    tmp_pccf_wc2miss%>% 
      mutate_all(as.character)
  ) %>% 
    select(
      ID,PCODE,BrthYear,PR,CD,DA,CSD,CSDname,CSDtype,SAC,CTname,Tracted,FED,DB,Rep_Pt_Type,
      LAT,LONG,SLI,PCtype,Comm_Name,DMT,H_DMT,DMTDIFF,PO,QI,Source,Link_Source,RPF,CMA,
      POP_CNTR_RA,POP_CNTR_RA_type,POP_CNTR_RA_SIZE_CLASS,nCD,nCSD,nDA,nBLK
    )
}

### Records without block ----
tmp_pccf_hxblk <- tmp_pccf_hblk %>% 
  filter(DB %in% c(""," ") | is.na(DB)) 

### Records with block ----
tmp_pccf_hblk <- tmp_pccf_hblk %>% 
  filter(!(DB %in% c(""," ") | is.na(DB))) 

# Impute blocks for any records with DA but no DB ----

geo_dablk <- merge(
  myfiles2[[which(grepl("geo_gaf21", temp_georef))]] %>% 
    mutate(inBlk = row_number()) %>% 
    select(PR, CD, DA, BLK, DBPOP2021, inBlk),
  myfiles2[[which(grepl("geo_dablkpnt", temp_georef))]] %>% 
    mutate(inPnt = row_number()) %>% 
    select(PR, CD, DA, DA21pop, inPnt),
  by = c("PR","CD","DA"),
  all = TRUE
) %>%
  filter(!is.na(inBlk) &
           !is.na(inPnt)) %>% 
  mutate(
    # Calculate weights
    WT = ifelse(
      DA21pop > 0,
      DBPOP2021/DA21pop,
      0)) %>%
  group_by(PR, CD, DA) %>%
  mutate(#TWT = 0, # Initialize TWT
         TWT = ifelse(
           row_number() %in% 1,
           WT,
           cumsum(WT)
         ),
         TWT = ifelse(
           row_number() %in% n(),
           1,
           TWT),
         FIRSTOBS = min(inBlk)
  ) %>% 
  ungroup()

# Specify seeds
set.seed(seedVal)

tmp_pccf_hxblk <- tmp_pccf_hxblk %>% 
  mutate(
    # Generate random number for probabilistic assignment
    RAND4 = runif(n = max(nrow(tmp_pccf_hxblk)))
  ) %>% 
  # Order rows by PCODE3 and RAND3
  arrange(PR, CD, DA, RAND4) 

tmp_pccf_hxblkhit <- merge(
  tmp_pccf_hxblk %>% 
    mutate(inHxBlk = row_number()),
  myfiles2[[which(grepl("geo_dablkpnt", temp_georef))]] %>% 
    mutate(inBlkPnt = row_number()) %>% 
    select(-CSD),
  by = c("PR", "CD", "DA"),
  all = TRUE
) %>% 
  filter(!is.na(ID))

### Not matched to unique PR, CD, DA ----
tmp_pccf_hxblkmiss <- tmp_pccf_hxblkhit %>% 
  filter(!is.na(inHxBlk) & is.na(inBlkPnt))

### Matched to unique PR, CD, DA ----
tmp_pccf_hxblkhit <- tmp_pccf_hxblkhit %>% 
  filter(!is.na(inHxBlk) & !is.na(inBlkPnt))

# Do indexed search in records found on duplicates
# using the FirstObs variable
#   - Pointer for first observation on wc3dups

i7 <- tmp_pccf_hxblkhit[,c("ID","FIRSTOBS","RAND4")]

# Get the i-th observation from tmp_pccf_wc3dups

point7 <- geo_dablk %>% 
  filter(geo_dablk[["FIRSTOBS"]] %in% i7[["FIRSTOBS"]]) %>% 
  # Add RAND3 field from "tmp_pccf_wc3hit" 
  # stored in i7
  merge(i7,
        by = "FIRSTOBS") %>% 
  # Compare RAND4 and TWT. Create DB
  # Geocoding source is 3-character imputation
  mutate(DB = ifelse(
    RAND4 <= TWT,
    BLK,
    NA
  )) %>% 
  # Remove not created DB
  # filter(!is.na(DB)) %>%
  group_by(ID) %>%
  # Keep the ones with greatest weight
  filter(WT %in% max(WT)) %>%
  ungroup() %>% 
  # If the weights are equal, keep first appearance
  distinct(ID, .keep_all = TRUE) %>%
  arrange(as.character(FIRSTOBS)) 

## Add DB to "hxblkhit"
# Keep only selected fields

tmp_pccf_hxblkhit <- tmp_pccf_hxblkhit %>% 
  # Sort by FirstObs
  arrange(as.character(FIRSTOBS)) %>%
  select(setdiff(names(tmp_pccf_hxblkhit),
                 names(point7))) %>% 
  # Combine DB by column dropping the
  # FirstObs field to avoid duplication
  cbind(
    point7 
  ) %>% 
  # Sort the data frame (by PCODE or ID, choose one)
  arrange(ID, PCODE) %>% 
  select(-NBLK, -FIRSTOBS, -DA21pop,
         -BLK, -DBPOP2021, -WT, -TWT, -RAND4)

# The file is now substantively geocoded. ----
# The program now completes a series of 
# checks and codes the remainder of the supplementary codes.

tmp_geocoded00 <- bind_rows(
  tmp_pccf_hxblkhit %>% 
    mutate_all(as.character),
  tmp_pccf_hxblkmiss %>% 
    mutate_all(as.character),
  tmp_pccf_hblk %>% 
    mutate_all(as.character)
) %>% 
  mutate(
    PR = ifelse(
      is.na(PR) | PR %in% c("", " "),
      99,
      PR
    )
  ) %>% 
  select(
    ID, PCODE, BrthYear, PR, CD, DA, DB, CSD, SAC,
    CMA, Tracted, CTname, FED, Rep_Pt_Type,
    RPF, LAT, LONG, SLI, PCtype, Comm_Name,
    DMT, H_DMT, DMTDIFF, PO, QI, Source,
    Link_Source, nCD, nCSD, nDA, nBLK) %>% 
  arrange(PCODE)

 # Add air stage delivery variable ----

tmp_geocoded01 <- merge(
  tmp_geocoded00 %>% 
    mutate(inHlth = row_number()) ,
  myfiles1[[which(grepl("airstage", temp_pccf))]] %>% 
    mutate(inAir = row_number()),
  by = "PCODE",
  all = TRUE
) %>%
  filter(!is.na(inHlth)) %>% 
  mutate(
    Airlift = ifelse(
      !is.na(inHlth) & !is.na(inAir),
      "*",
      NA
    ),
    Comm_Name = ifelse(is.na(Comm_Name.x),
                       toupper(Comm_Name.y),
                       toupper(Comm_Name.x))
  ) %>% 
  select(-Comm_Name.x, -Comm_Name.y)

# Determine if the area is primarily institutional ----

tmp_geocoded02 <- merge(
  tmp_geocoded01 %>% 
    mutate(inHlth = row_number()),
  myfiles1[[which(grepl("instflg", temp_pccf))]],
  by = "PCODE",
  all = TRUE
) %>%
  filter(!is.na(inHlth)) %>%
  arrange(PCODE) %>% 
  mutate(
    InstFlag = ifelse(
      !toupper(Link_Source) %in% c("F","D"),
      "",
      InstFlag
    )
      )

# Check if DMT E,G,M are possible residences ----

cpc_egmres <- myfiles1[[which(grepl("egmres", temp_pccf))]] %>% 
  arrange(PCODE) %>%
  group_by(PCODE) %>%
  filter(row_number() %in% 1) %>%
  ungroup()

tmp_geocoded03 <- merge(
  tmp_geocoded02 %>% 
    mutate(inHlth = row_number()),
  cpc_egmres %>% 
    mutate(inEGM = row_number()),
  by = "PCODE",
  all = TRUE
) %>%
  filter(!is.na(inHlth)) %>%
  arrange(PCODE) %>% 
  mutate(
    ResFlag = case_when(
      is.na(inEGM) & toupper(DMT) %in% c("E","G","M") ~ "?",
      toupper(DMT) %in% c("A","B") ~ " ",
      TRUE ~ ResFlag
    )
  ) 

### Residential Coding only ----
if(codeVersion %in% 0){
  tmp_geocoded03 <- tmp_geocoded03 %>%
    mutate(
      CD = ifelse(
      ResFlag %in% "-" & CD %in% c("", " ", NA),
      "00",
      CD
    ),
    CSD = ifelse(
      ResFlag %in% "-" & CSD %in% c("", " ", NA),
      "999",
      CSD
    ),
    FED = ifelse(
      ResFlag %in% "-" & FED %in% c("", " ", NA),
      "999",
      FED
    ),
    DA = ifelse(
      ResFlag %in% "-" & DA %in% c("", " ", NA),
      "9999",
      DA
    ),
    DB = ifelse(
      ResFlag %in% "-" & DB %in% c("", " ", NA),
      " ",
      DB
    ),
    LAT = ifelse(
      ResFlag %in% "-" & LAT %in% c("", " ", NA),
      NA,
      LAT
    ),
    LONG = ifelse(
      ResFlag %in% "-" & LONG %in% c("", " ", NA),
      NA,
      LONG
    ),
    Link = NA,
    Link = ifelse(
      ResFlag %in% "-" & Link %in% c("", " ", NA),
      "2",
      NA
    ),
    CT = NA,
    CT = ifelse(
      ResFlag %in% "-" & Tracted %in% "0",
      "0000.00",
      ifelse(
        ResFlag %in% "-" & CT %in% c("", " ", NA),
        "9999.99",
        NA
        )
    )
  ) 
}

# Adds names and addresses for office & institutional coding ----

tmp_geocoded04 <- merge(
  tmp_geocoded03 %>% 
    mutate(inHlth = row_number()),
  myfiles1[[which(grepl("nadr", temp_pccf))]] %>% 
    mutate(inInst = row_number()),
  by = "PCODE",
  all = TRUE
) %>%
  filter(!is.na(inHlth)) %>%
  mutate(
    NumAdr = case_when(
      NumAdr >= 9 ~ 9,
      # 9+ adr for rural
      substr(PCODE, 2, 2 ) %in% 0 ~ 9,
      toupper(Link_Source) %in% "F" ~ 1,
      is.na(NumAdr) & 
        (toupper(DMT) %in% "K" |
           (toupper(DMT) %in% "W" &
              toupper(DMTDIFF) %in% "K")) ~ 9,
      is.na(NumAdr) &
        (toupper(DMT) %in% c("B", "E", "G", "M") |
           (toupper(DMT) %in% "W" &
              toupper(DMTDIFF) %in% c("B", "E", "G", "M"))) ~ 1,
      TRUE ~ NumAdr
    )
  ) %>%
  arrange(PR, CD, DA, DB) 

# Merge back with Geographic Attribute File to get ----
# the remainder of the codes
## - Start at the Dissemination Block level, ----
## then move up to the DA, CSD, etc...

tmp_geocoded04 <- tmp_geocoded04 %>% 
  select(
    ID, PCODE, BrthYear, PR, CD, DA, DB, Rep_Pt_Type, RPF, LAT,
    LONG, SLI, PCtype, Comm_Name, Airlift, InstFlag,
    ResFlag, BldgName, NumAdr, DMT, H_DMT, DMTDIFF,
    PO, QI, Source, Link_Source, Tracted, nCD, nCSD,
    nDA, nBLK
    )

# Merge with DB-level GAF to get additional variables unique at the DB level ----

tmp_geocoded05 <- merge(
  tmp_geocoded04 %>% 
    mutate(inHlth = row_number()),
  myfiles2[[which(grepl("gaf21", temp_georef))]] %>% 
    arrange(PR, CD, DA, DB) %>% 
    select(
      PR, CD, DA, DB, DB_ir2021, PRabbr, PRfabbr,
      FEDname, ERuid, ERdguid, ERname, CDname, CSDname,
      CSDtype, CTname, CCSuid, CCSdguid, CCSname, DPLuid,
      DPLdguid, DPLname, DPLtype, CMApuid, CMAname, CMAtype,
      SACcode, SACtype, PopCtrRAPuid, PopCtrRAname, PopCtrRAtype,
      PopCtrRAclass, CARUID, CARname, CSIZE, CSIZEMIZ, 
  # PR CD DA - these values set to missing for recs
  # with no DBUID because they will not be merged
      FED, CSD, DB11uid, DA11uid, DA06uid, DA01uid,
      EA96uid, EA91uid, EA86uid, EA81uid, InuitLands,
      DA16UID, DB16UID, DB16DGUID
    ),
  by = c("PR", "CD", "DA", "DB"),
  all = TRUE
) %>%
  filter(!is.na(inHlth)) 
  
# Add health region and alternate health region codes ----

tmp_geocoded06 <- merge(
  tmp_geocoded05 %>% 
    mutate(inHlth = row_number()),
  myfiles2[[which(grepl("hrdef", temp_georef))]] %>% 
    arrange(PR, CD, DA, DB) %>% 
    select(
      PR, CD, DA, DB, HRuid, HRename, HRfname,
      AHRuid, AHRename, AHRfname
    ),
  by = c("PR", "CD", "DA", "DB"),
  all = TRUE
) %>%
  filter(!is.na(inHlth)) 

# Adds the income quintiles and immigrant terciles ----

tmp_geocoded07 <- merge(
  tmp_geocoded06 %>% 
    mutate(inHlth = row_number(),
           DAUID = paste0(PR, CD, DA)),
  myfiles2[[which(grepl("sesref", temp_georef))]] %>% 
    arrange(DAUID) %>%
    mutate(inSES = row_number()),
  by = c("DAUID"),
  all = TRUE
) %>%
  filter(!is.na(inHlth)) %>% 
  mutate(
    BTIPPE = case_when(
      is.na(inSES) ~ "99999999",
      TRUE ~ BTIPPE
    ),
    ATIPPE = case_when(
      is.na(inSES) ~ "99999999",
      TRUE ~ ATIPPE
    ),
    QABTIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ QABTIPPE
    ),
    QNBTIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ QNBTIPPE
    ),
    DABTIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ DABTIPPE
    ),
    DNBTIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ DNBTIPPE
    ),
    QAATIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ QAATIPPE
    ),
    QNATIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ QNATIPPE
    ),
    DAATIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ DAATIPPE
    ),
    DNATIPPE = case_when(
      is.na(inSES) ~ "999",
      TRUE ~ DNATIPPE
    ),
    IMPFLG = case_when(
      is.na(inSES) ~ "9",
      TRUE ~ IMPFLG
    )
  ) 

# Set missing values for output variables ----

final_hlthout <- tmp_geocoded07 %>% 
  mutate(
    # Date and time the process was run
    date_run = Sys.time()
  ) 

# Set missing values for geographic identifiers

final_hlthout$PR[final_hlthout$PR %in% c("", " ") |
                   is.na(final_hlthout$PR) |
                   final_hlthout$PR %in% "00"] <- "99"
  
final_hlthout$Link_Source[final_hlthout$PR %in% "99"] <- "0"
final_hlthout$CD[final_hlthout$CD %in% c("", " ") |
                   is.na(final_hlthout$CD)] <- "00"
final_hlthout$DA[final_hlthout$DA %in% c("", " ") |
                   is.na(final_hlthout$DA) |
                   final_hlthout$DA %in% "0000"] <- "9999"
final_hlthout$DB[final_hlthout$DB %in% c("", " ") |
                   is.na(final_hlthout$DB)] <- "000"
final_hlthout$CSD[final_hlthout$CSD %in% c("", " ") |
                   is.na(final_hlthout$CSD)] <- "999"
final_hlthout$SACtype[final_hlthout$SACtype %in% c("", " ") |
                    is.na(final_hlthout$SACtype)] <- "9"
final_hlthout$CMA <- final_hlthout$SACcode
# not CMA; rural MIZ
final_hlthout$CMA[final_hlthout$CMA %in% c("996", "997", "998", "999")] <- "000"
# missing CMA
final_hlthout$CMA[final_hlthout$CMA %in% c("", " ") |
                        is.na(final_hlthout$CMA)] <- "999"
final_hlthout$Tracted <- "0"
final_hlthout$Tracted[final_hlthout$SACtype %in% c("1","2")] <- "1"
final_hlthout$Tracted[final_hlthout$SACtype %in% c("9")] <- "9"
# CT unknown but applicable, i.e., in tracted CMA/CA
final_hlthout$CTname[final_hlthout$Tracted %in% "1" &
                       final_hlthout$CTname %in% "0000.00"] <- "9999.99"
final_hlthout$CTname[final_hlthout$Tracted %in% "1" &
                       (final_hlthout$CTname %in% c("", " ") |
                       is.na(final_hlthout$CTname))] <- "9999.99"
final_hlthout$CTname[final_hlthout$Tracted %in% "1" &
                       final_hlthout$CTname %in% "0000.00"] <- "9999.99"
final_hlthout$CTname[final_hlthout$CTname %in% "0000000"] <- "9999.99"
final_hlthout$CTname[final_hlthout$CTname %in% c("", " ") |
                       is.na(final_hlthout$CTname)] <- "9999.99"
# Not in Census Tracted area or CMA/CA unknown
final_hlthout$CTname[final_hlthout$Tracted %in% "0"] <- "0000.00"
final_hlthout$RPF[is.na(final_hlthout$LAT)] <- "9"
final_hlthout$Rep_Pt_Type[is.na(final_hlthout$LAT)] <- "9"
final_hlthout$Rep_Pt_Type[is.na(final_hlthout$Rep_Pt_Type) |
                            final_hlthout$Rep_Pt_Type %in% c("", " ")] <- "9"
final_hlthout$DPLuid[is.na(final_hlthout$DPLuid)] <- paste0(final_hlthout$PR, " ")
final_hlthout$DPLuid[final_hlthout$DPLuid %in% c("", " ")] <- paste0(final_hlthout$PR, " ")
final_hlthout$FED[is.na(final_hlthout$FED) |
                            final_hlthout$FED %in% c("", " ")] <- "999"
final_hlthout$ERuid[is.na(final_hlthout$ERuid)] <- paste0(final_hlthout$PR, "99")
final_hlthout$ERuid[final_hlthout$ERuid %in% c("", " ")] <- paste0(final_hlthout$PR, "99")
final_hlthout$CARUID[final_hlthout$PR %in% c("60", "61", "62")] <- paste0(final_hlthout$PR, "00")
final_hlthout$CARUID[final_hlthout$CARUID %in% c("", " ") |
                       is.na(final_hlthout$CARUID)] <- paste0(final_hlthout$PR, "99")
final_hlthout$CCSuid[final_hlthout$CCSuid %in% c("", " ") |
                       is.na(final_hlthout$CCSuid)] <- "999"
final_hlthout$PopCtrRAPuid[final_hlthout$PopCtrRAPuid %in% c("", " ")] <- paste0(final_hlthout$PR, "9999")
final_hlthout$PopCtrRAPuid[is.na(final_hlthout$PopCtrRAPuid)] <- paste0(final_hlthout$PR, "9999")
final_hlthout$SACtype[final_hlthout$SACtype %in% c("", " ") |
                             is.na(final_hlthout$SACtype)] <- "9"
final_hlthout$Tracted[final_hlthout$SACtype %in% c("9")] <- "9"
final_hlthout$CMAtype[final_hlthout$CMAtype %in% c("", " ") |
                        is.na(final_hlthout$CMAtype)] <- "9"
final_hlthout$PopCtrRAtype[final_hlthout$PopCtrRAtype %in% c("", " ") |
                        is.na(final_hlthout$PopCtrRAtype)] <- "9"
final_hlthout$PopCtrRAclass[final_hlthout$PopCtrRAclass %in% c("", " ") |
                             is.na(final_hlthout$PopCtrRAclass)] <- "9"
final_hlthout$QI[final_hlthout$QI %in% c("", " ") |
                              is.na(final_hlthout$QI)] <- "999"
final_hlthout$Source[final_hlthout$Source %in% c("", " ") |
                              is.na(final_hlthout$Source)] <- "9"
final_hlthout$DB_ir2021[final_hlthout$DB_ir2021 %in% c("", " ") |
                              is.na(final_hlthout$DB_ir2021)] <- "9"
final_hlthout$CSIZE[final_hlthout$CSIZE %in% c("", " ") |
                              is.na(final_hlthout$CSIZE)] <- "9"
final_hlthout$CSIZEMIZ[final_hlthout$CSIZEMIZ %in% c("", " ") |
                              is.na(final_hlthout$CSIZEMIZ)] <- "9"
final_hlthout$DMT[final_hlthout$DMT %in% c("", " ") |
                         is.na(final_hlthout$DMT)] <- "9"
final_hlthout$H_DMT[final_hlthout$H_DMT %in% c("", " ") |
                    is.na(final_hlthout$H_DMT)] <- "9"
final_hlthout$Link[final_hlthout$Link %in% c("", " ") |
                    is.na(final_hlthout$Link)] <- "0"
final_hlthout$Link_Source[final_hlthout$Link_Source %in% c("", " ") |
                    is.na(final_hlthout$Link_Source)] <- "0"
final_hlthout$SLI[final_hlthout$SLI %in% c("", " ") |
                            is.na(final_hlthout$SLI)] <- "9"
final_hlthout$PCtype[final_hlthout$PCtype %in% c("", " ") |
                    is.na(final_hlthout$PCtype)] <- "9"
final_hlthout$PO[final_hlthout$PO %in% c("", " ") |
                    is.na(final_hlthout$PO)] <- "2"
final_hlthout$InuitLands[final_hlthout$InuitLands %in% c("", " ") |
                    is.na(final_hlthout$InuitLands)] <- "9"
final_hlthout$BTIPPE[final_hlthout$BTIPPE %in% c("", " ") |
                    is.na(final_hlthout$BTIPPE)] <- "99999999"
final_hlthout$ATIPPE[final_hlthout$ATIPPE %in% c("", " ") |
                       is.na(final_hlthout$ATIPPE)] <- "99999999"
final_hlthout$QAIPPE[final_hlthout$QABTIPPE %in% c("", " ") |
                       is.na(final_hlthout$QABTIPPE)] <- "999"
final_hlthout$QNIPPE[final_hlthout$QNBTIPPE %in% c("", " ") |
                       is.na(final_hlthout$QNBTIPPE)] <- "999"
final_hlthout$DAIPPE[final_hlthout$DABTIPPE %in% c("", " ") |
                       is.na(final_hlthout$DABTIPPE)] <- "999"
final_hlthout$DNIPPE[final_hlthout$DNBTIPPE %in% c("", " ") |
                       is.na(final_hlthout$DNBTIPPE)] <- "999"
final_hlthout$QAATIPPE[final_hlthout$QAATIPPE %in% c("", " ") |
                       is.na(final_hlthout$QAATIPPE)] <- "999"
final_hlthout$QNATIPPE[final_hlthout$QNATIPPE %in% c("", " ") |
                       is.na(final_hlthout$QNATIPPE)] <- "999"
final_hlthout$DAATIPPE[final_hlthout$DAATIPPE %in% c("", " ") |
                       is.na(final_hlthout$DAATIPPE)] <- "999"
final_hlthout$DNATIPPE[final_hlthout$DNATIPPE %in% c("", " ") |
                       is.na(final_hlthout$DNATIPPE)] <- "999"
final_hlthout$IMPFLG[final_hlthout$IMPFLG %in% c("", " ") |
                       is.na(final_hlthout$IMPFLG)] <- "9"
final_hlthout$EA81uid[final_hlthout$EA81uid %in% c("", " ") |
                       is.na(final_hlthout$EA81uid)] <- "99999999"
final_hlthout$EA86uid[final_hlthout$EA86uid %in% c("", " ") |
                        is.na(final_hlthout$EA86uid)] <- "99999999"
final_hlthout$EA91uid[final_hlthout$EA91uid %in% c("", " ") |
                        is.na(final_hlthout$EA91uid)] <- "99999999"
final_hlthout$EA96uid[final_hlthout$EA96uid %in% c("", " ") |
                        is.na(final_hlthout$EA96uid)] <- "99999999"
final_hlthout$DA01uid[final_hlthout$DA01uid %in% c("", " ") |
                        is.na(final_hlthout$DA01uid)] <- "99009999"
final_hlthout$DA06uid[final_hlthout$DA06uid %in% c("", " ") |
                        is.na(final_hlthout$DA06uid)] <- "99009999"
final_hlthout$DA11uid[final_hlthout$DA11uid %in% c("", " ") |
                        is.na(final_hlthout$DA11uid)] <- "99009999"
final_hlthout$DA16UID[final_hlthout$DA16UID %in% c("", " ") |
                        is.na(final_hlthout$DA16UID)] <- "99009999"
final_hlthout$DB11uid[final_hlthout$DB11uid %in% c("", " ") |
                        is.na(final_hlthout$DB11uid)] <- "99009999"
final_hlthout$DB16UID[final_hlthout$DB16UID %in% c("", " ") |
                        is.na(final_hlthout$DB16UID)] <- "99009999"
# missing but applicable 
final_hlthout$HRuid[final_hlthout$PR %in% "35" & 
                      (final_hlthout$HRuid %in% c("", " ") |
                        is.na(final_hlthout$HRuid))] <- paste0(final_hlthout$PR, "99")
# not applicable
final_hlthout$HRuid[!final_hlthout$PR %in% "35" & 
                      (final_hlthout$HRuid %in% c("", " ") |
                         is.na(final_hlthout$HRuid))] <- paste0(final_hlthout$PR, " ")
# missing but applicable 
final_hlthout$AHRuid[final_hlthout$PR %in% "35" & 
                      (final_hlthout$AHRuid %in% c("", " ") |
                         is.na(final_hlthout$AHRuid))] <- paste0(final_hlthout$PR, "99")
# not applicable
final_hlthout$AHRuid[!final_hlthout$PR %in% "35" & 
                      (final_hlthout$AHRuid %in% c("", " ") |
                         is.na(final_hlthout$AHRuid))] <- paste0(final_hlthout$PR, " ")

 # Create DBuid, DAuid, CDuid, CSDuid and FEDuid ----
final_hlthout$DBuid = paste0(final_hlthout$PR,
                             final_hlthout$CD,
                             final_hlthout$DA,
                             final_hlthout$DB)
final_hlthout$DAuid = paste0(final_hlthout$PR,
                             final_hlthout$CD,
                             final_hlthout$DA)
final_hlthout$CDuid = paste0(final_hlthout$PR,
                             final_hlthout$CD)
final_hlthout$CSDuid = paste0(final_hlthout$PR,
                             final_hlthout$CD,
                             final_hlthout$CSD)
final_hlthout$FEDuid = paste0(final_hlthout$PR,
                              final_hlthout$CD,
                              final_hlthout$FED)

# Calculate the number of missing at each primary geographic level ----
final_hlthout$PRok <- ifelse(!final_hlthout$PR %in% "99", 1, 0)
final_hlthout$CDok <- ifelse(!final_hlthout$CD %in% "00", 1, 0)
final_hlthout$CMAok <- ifelse(!final_hlthout$CD %in% "999", 1, 0)
final_hlthout$CSDok <- ifelse(!final_hlthout$CSD %in% "999", 1, 0)
final_hlthout$CTok <- ifelse(!final_hlthout$CTname %in% "9999.99", 1, 0)
final_hlthout$DAok <- ifelse(!final_hlthout$DA %in% "9999", 1, 0)
final_hlthout$DBok <- ifelse(!final_hlthout$DB %in% "000", 1, 0)

final_hlthout <- final_hlthout %>% 
  mutate(
    CCSum = 9,
    CCSum = case_when(
      DBok %in% 1 & DAok %in% 1 ~ 5,
      CSDok %in% 1 & CMAok %in% 1 & DAok %in% 0 ~ 4,
      CDok %in% 1 & CMAok %in% 1 & DAok %in% 0 ~ 3,
      PRok %in% 1 & (CDok %in% 1 | CMAok %in% 1) & DAok %in% 0 ~ 2,
      PRok %in% 1 & CDok %in% 0 & CMAok %in% 0 & DAok %in% 0 ~ 1,
      PRok %in% 0 ~ 0,
      TRUE ~ CCSum
    ),
    # Create link type field (LINK) but first initialize variables LTx
    lt0 = 0,lt1 = 0,lt2 = 0,lt3 = 0, lt4 = 0, lt5 = 0, lt6 = 0, lt7 = 0, lt9 = 0,
    Link =  case_when(
      DMT %in% "9" ~ "0",
      # Not full match--although partial matching may have been done
      DA %in% "9999" & 
        (toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
           (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")) ) ~ "1",
      # Option only for INSTITUTIONAL
      (toupper(DMT) %in% "M" |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("M"))) &
        codeVersion %in% 1 & 
        !toupper(Link_Source) %in% "C" ~ "4",
      (toupper(DMT) %in% "E" |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("E"))) &
        ResFlag %in% "-" ~ "2",
      (toupper(DMT) %in% "E" |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("E"))) &
        ResFlag %in% c("+","@") ~ "9",
      (toupper(DMT) %in% "E" |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("E"))) &
        ResFlag %in% c(" ", "?") ~ "3",
      # non-residential
      (toupper(DMT) %in% c("G","M") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("G","M"))) &
        ResFlag %in% "-" ~ "2",
      # commercial or institutional building
      (toupper(DMT) %in% c("G","M") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("G","M"))) ~ "4",
      # retired postal code with no known DMT problems
      toupper(DMT) %in% c("Z") ~ "5",
      # multiple DA match using PCCF dups - unweighted allocation
      nDA > 1 & toupper(Link_Source) %in% "D" ~ "6",
      # multiple match using wcf - weighted allocation
      toupper(Link_Source) %in% "C" ~ "7",
      # PCCF dups where nDA=1 or PCCF Unique (link_source='F')
      nDA %in% 1 | toupper(Link_Source) %in% "F" ~ "9",
      TRUE ~ Link
    ),
    RPF = case_when(
      DMT %in% "9" ~ "9",
      # # Not full match--although partial matching may have been done
      DA %in% "9999" & 
        (toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") |
           (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")) ) ~ "8",
      TRUE ~ RPF
    ),
    lt0 = ifelse(
      DMT %in% "9",
      1,
      lt0
      ),
    # Not full match--although partial matching may have been done
    lt1 = ifelse(
      DA %in% "9999" & 
        (toupper(DMT) %in% c("H", "J", "K", "M", "R", "T", "X") | 
           (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("H", "J", "K", "M", "R", "T", "X")) ),
      1,
      ifelse(
        (toupper(DMT) %in% "M" |
           (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("M"))) &
          codeVersion %in% 1 & 
          !toupper(Link_Source) %in% "C",
        "4",
        lt1
      )),
    # non-residential
    lt2 = ifelse(
      (toupper(DMT) %in% "E" |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("E"))) &
        ResFlag %in% "-",
      1,
      lt2
    ),
    # residential-ok
    lt9 = ifelse(
      (toupper(DMT) %in% "E" |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("E"))) &
        ResFlag %in% c("+","@"),
      1,
      lt9
    ),
    # business building-may be non-residential
    lt3 = ifelse(
      (toupper(DMT) %in% "E" |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("E"))) &
        ResFlag %in% c(" ","?"),
      1,
      lt3
    ),
    # non-residential
    lt2 = ifelse(
      (toupper(DMT) %in% c("G","M") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("G","M"))) &
        ResFlag %in% "-",
      1,
      lt2
    ),
    # commercial or institutional building
    lt4 = ifelse(
      (toupper(DMT) %in% c("G","M") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("G","M"))),
      1,
      lt4
    ),
    # retired postal code with no known DMT problems
    lt5 = ifelse(
      toupper(DMT) %in% c("Z"),
      1,
      lt5
    ),
    # multiple DA match using PCCF dups - unweighted allocation
    lt6 = ifelse(
      nDA > 1 & toupper(Link_Source) %in% "D",
      1,
      lt6
    ),
    # multiple match using wcf - weighted allocation
    lt7 = ifelse(
      toupper(Link_Source) %in% "C",
      1,
      lt7
    ),
    # PCCF dups where nDA=1 or PCCF Unique (link_source='F')
    lt9 = ifelse(
      nDA %in% 1 | toupper(Link_Source) %in% "F",
      1,
      lt9
    ),
    PREC = case_when(
      # 1 blkface (a-g)
      (toupper(DMT) %in% c("A", "B", "E", "G") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("A", "B", "E", "G", "", " ", NA)) &
         toupper(Link_Source) %in% "F" & Rep_Pt_Type %in% "1") ~ "9",
      # 1 blk (a b e g)
      (toupper(DMT) %in% c("A", "B", "E", "G") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("A", "B", "E", "G", "", " ", NA)) &
         toupper(Link_Source) %in% c("F","D") & Rep_Pt_Type %in% "2" & nBLK %in% 1) ~ "8",
      # 1 da (a b e g)
      (toupper(DMT) %in% c("A", "B", "E", "G") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("A", "B", "E", "G", "", " ", NA)) &
         toupper(Link_Source) %in% c("F","D","C") & nDA %in% 1) ~ "7",
      # 2+ da's (abeg)
      (toupper(DMT) %in% c("A", "B", "E", "G") |
         (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("A", "B", "E", "G", "", " ", NA)) &
         toupper(Link_Source) %in% c("F","D","C") & nDA > 1) ~ "6",
      # 1+ da (dmt h-x)
      toupper(Link_Source) %in% c("C") ~ "5",
      # from wc5 pop wt
      toupper(Link_Source) %in% c("5") ~ "4",
      # from wc4 pop wt
      toupper(Link_Source) %in% c("4") ~ "3",
      # from wc3 pop wt
      toupper(Link_Source) %in% c("3") ~ "2",
      # from from wc2 or wc1(PR)
      toupper(Link_Source) %in% c("1","2") ~ "1",
      # no geog coding
      toupper(Link_Source) %in% c("0") ~ "0",
      # non-res pcode
      toupper(Link_Source) %in% c("F","D") & CSD %in% "999" ~ "1",
      # logic check
      TRUE ~ "#"
    ),
    PREC = case_when(
      PREC %in% "#" &
        (toupper(DMT) %in% c("H", "J", "K", "T", "W", "X") |
           (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("H", "J", "K", "T", "W", "X"))) ~ "5",
      # 'M' indicates delivery to a large volume receivers (PO box), however,
      # the ultimate destination of an 'M' is usually within the same FSA as the PO Box 
      PREC %in% "#" &
        (toupper(DMT) %in% c("M") |
           (toupper(DMT) %in% "Z" & toupper(DMTDIFF) %in% c("M"))) ~ "2",
      TRUE ~ PREC
    ),
    RPF = case_when(
      RPF %in% c("", " ", NA) &
        toupper(Link_Source) %in% c("F","D") ~ "3",
      RPF %in% c("", " ", NA) &
        toupper(Link_Source) %in% c("C") &
        Rep_Pt_Type %in% "2" ~ "4",
      RPF %in% c("", " ", NA) &
        toupper(Link_Source) %in% c("C") &
        Rep_Pt_Type %in% "3" ~ "5",
      RPF %in% c("", " ", NA) &
        toupper(Link_Source) %in% c("C","4","5") ~ "6",
      RPF %in% c("", " ", NA) &
        toupper(Link_Source) %in% c("1","2","3") ~ "8",
      TRUE ~ RPF
    ),
    Tot = 1,
    PCCFplus_Release = version,
    Random_Seed = seedVal,
    geoCodeType = ifelse(
      codeVersion %in% 1,
      "Institutional",
      "Residential"
    )
  )

# Create the problem file and add additional variables ----

final_problem <- final_hlthout %>% 
  mutate(
    # DEFINE MESSAGES RELATED TO LINK TYPES
    # NOTE: MOST SERIOUS PROBLEM (LOWEST LINK TYPE) TAKES
    # PRECEDENCE
    Message = case_when(
      Link %in% "0" ~ "0 ERROR: NO MATCH TO PCCF - CHECK PCODE/ADDRESS OR CODE MANUALLY",
      Link %in% "1" ~ "1 ERROR: LINKED TO PO GEOGRAPHY - CODE MANUALLY IF ADDRESS AVAILABLE",
      Link %in% "2" ~ "2 WARNING: NON-RESIDENTIAL PCODE - CHECK PCODE/ADDRESS (LEGITIMATE RESIDENCE?)",
      Link %in% "3" ~ "3 WARNING: BUSINESS BUILDING - CHECK PCODE/ADDRESS (LEGITIMATE RESIDENCE?)",
      Link %in% "4" ~ "4 WARNING: COMMERCIAL/INSTITUTIONAL - CHECK PCODE/ADDRESS (LEGITIMATE RESIDENCE?)",
      Link %in% "5" ~ "5 NOTE: RETIRED PCODE - EXPECTED IN ADMINISTRATIVE DATA FILES",
      Link %in% "6" ~ "6 NOTE: MULTIPLE MATCH DISSEMINATION AREA - USING UNWEIGHTED ALLOCATION",
      Link %in% "7" ~ "7 NOTE: WEIGHTED ALLOCATION USING 6-DIGIT WCF",
      Link %in% "9" ~ ". NO ERROR, WARNING OR NOTE - NO ACTION REQUIRED"
    )
  ) %>% 
  arrange(PCODE)

pccf_lookup <- myfiles1[[which(grepl("bldgnam", temp_pccf))]] %>% 
  mutate(
    ADR = paste0(trimws(NameAdr), " ", Comm_Name),
    nADR = case_when(
      # TEXT FILE WAS MADE UNIQUE
      NumAdr %in% c("", " ", NA) ~ 1,
      # 9=> 9+ ADDRESS RANGES
      NumAdr >= 9 ~ 9,
      TRUE ~ NumAdr
    )
  ) %>% 
  arrange(PCODE) %>% 
  group_by(PCODE) %>% 
  filter(row_number() %in% n()) %>% 
  ungroup()

final_problem <- merge(
  final_problem %>%
    mutate(inProb = row_number()),
  pccf_lookup %>%
    mutate(inLook = row_number()) %>% 
    select(PCODE, ADR),
  by = "PCODE",
  all = TRUE
) %>%
  filter(!is.na(inProb)) %>% 
  arrange(Link, ResFlag, PCODE, PR, CD, DA)

# Summarise the final_hlthout and final_problem files ----

final_problem <- final_problem %>% 
  mutate(
    fmt_link = case_when(
      Link %in% "0" ~ "0| Error: No match to PCCF",
      Link %in% "1" ~ "1| Error: Link to PO Geography",
      Link %in% "2" ~ "2| Warning: Non-residential",
      Link %in% "3" ~ "3| Warning: Business building",
      Link %in% "4" ~ "4| Warning: Commercial/Institutional",
      Link %in% "5" ~ "5| Note: Retired postal code",
      Link %in% "6" ~ "6| Note: Multi-DA, unweighted allocation",
      Link %in% "7" ~ "7| Note: WCF, weighted allocation",
      Link %in% "9" ~ "9| No error, note or warning"
    ),
    fmt_linksrc = case_when(
      toupper(Link_Source) %in% "F" ~ "F| Fully coded, PCCF unique record",
      toupper(Link_Source) %in% "D" ~ "D| Fully coded, PCCF duplicate record",
      toupper(Link_Source) %in% "C" ~ "C| Fully coded, WCF record",
      toupper(Link_Source) %in% "5" ~ "5| Fully coded, 5-character imputation",
      toupper(Link_Source) %in% "4" ~ "4| Fully coded, 4-character imputation",
      toupper(Link_Source) %in% "3" ~ "3| Partially coded, 3-character imputation",
      toupper(Link_Source) %in% "2" ~ "2| Partially coded, 2-character imputation",
      toupper(Link_Source) %in% "1" ~ "1| Province code from 1-character",
      toupper(Link_Source) %in% "0" ~ "0| No geographic codes assigned",
      toupper(Link_Source) %in% "V" ~ "V| Fully matched, V1H or V9G (BC Old)",
      toupper(Link_Source) %in% "9" ~ "9| Not Applicable / Missing"
    ),
    fmt_src = case_when(
      toupper(Source) %in% "1" ~ "1| Automated coding to 2011 Census",
      toupper(Source) %in% "2" ~ "2| Geocoded using 2011 Census response",
      toupper(Source) %in% "3" ~ "3| Converted from 2006 Census geography",
      toupper(Source) %in% "4" ~ "4| Manually geocoded",
      toupper(Source) %in% "9" ~ "9| Not Applicable / Missing"
    ),
    fmt_prec = case_when(
      PREC %in% "0" ~ "0| No geographic coding",
      PREC %in% "1" ~ "1| Imputed from 2 (WC2) or 1 character (PR)",
      PREC %in% "2" ~ "2| Imputed from 3 characters (WC3)",
      PREC %in% "3" ~ "3| Imputed from 4 characters (WC4)",
      PREC %in% "4" ~ "4| Imputed from 5 characters (WC5)",
      PREC %in% "5" ~ "5| 1 or more DAs (WC6) (DMT=HtoX)",
      PREC %in% "6" ~ "6| 2 or more DAs (DMT=ABEG)",
      PREC %in% "7" ~ "7| 1 Dissemination Area (DMT=ABEG)",
      PREC %in% "8" ~ "8| 1 Dissemination Block (DMT=ABEG)",
      PREC %in% "9" ~ "9| 1 Block Face (DMT=ABEG)"
    ),
    fmt_ncsd = case_when(
      nCSD %in% "1" ~ "1| 1 CSD",
      nCSD %in% "2" ~ "2| 2 CSDs",
      nCSD %in% "3" ~ "3| 3 CSDs",
      nCSD %in% "4" ~ "4| Imputed from 5 characters (WC5)",
      nCSD %in% "5" ~ "5| 1 or more DAs (WC6) (DMT=HtoX)",
      nCSD %in% "6" ~ "6| 2 or more DAs (DMT=ABEG)",
      nCSD %in% "7" ~ "7| 1 Dissemination Area (DMT=ABEG)",
      nCSD %in% "8" ~ "8| 1 Dissemination Block (DMT=ABEG)",
      nCSD %in% "9" ~ "9| 1 Block Face (DMT=ABEG)",
      TRUE ~ "Missing"
    ),
    fmt_reppt = case_when(
      Rep_Pt_Type %in% "1" ~ "1| Block face",
      Rep_Pt_Type %in% "2" ~ "2| Dissemination block",
      Rep_Pt_Type %in% "3" ~ "3| Dissemination area",
      Rep_Pt_Type %in% "4" ~ "4| Census Subdivision",
      Rep_Pt_Type %in% "9" ~ "9| Not applicable / Missing"
    ),
    fmt_rpf = case_when(
      RPF %in% "1" ~ "1| Block-face point (Link-Source=F,D)",
      RPF %in% "2" ~ "2| DB point (Link-Source=F,D)",
      RPF %in% "3" ~ "3| DB point imputed within DA (Link_Source=F,D)",
      RPF %in% "4" ~ "4| DB point imputed within set of possible DAs (Link_Source=C)",
      RPF %in% "5" ~ "5| DA point imputed within set of possible DAs (Link_Source=C)",
      RPF %in% "6" ~ "6| DA point imputed within partial Postal Code (Link_Source=3,4,5)",
      RPF %in% "8" ~ "8| Centroid imputed by from first 1 or 2 characters (Link_Source=2,1)",
      RPF %in% "9" ~ "9| Not applicable / Missing"
    ),
    fmt_resf = case_when(
      ResFlag %in% "+" ~ "+| Possible residence",
      ResFlag %in% "-" ~ "-| Improbable residence",
      ResFlag %in% "?" ~ "?| DMT=E,G,M, residence undetermined",
      ResFlag %in% c(" ","", NA) ~ "(blank)| Not in DMT=E,G,M"
    ),
    fmt_insf = case_when(
      toupper(InstFlag) %in% "E" ~ "E| School or university residence",
      toupper(InstFlag) %in% "H" ~ "H| Hospital",
      toupper(InstFlag) %in% "M" ~ "M| Military bases",
      toupper(InstFlag) %in% "N" ~ "N| Nursing homes",
      toupper(InstFlag) %in% "S" ~ "S| Seniors residences",
      toupper(InstFlag) %in% "R" ~ "R| Religious",
      toupper(InstFlag) %in% "P" ~ "P| Prisons, jails",
      toupper(InstFlag) %in% "T" ~ "T| Hotels, motels",
      toupper(InstFlag) %in% "U" ~ "U| Other",
      toupper(InstFlag) %in% c(""," ", NA) ~ "(blank)| Not applicable or missing"
    ),
    fmt_pctyp = case_when(
      toupper(PCtype) %in% "0" ~ "0| Unknown",
      toupper(PCtype) %in% "1" ~ "1| Street address with letter carrier",
      toupper(PCtype) %in% "2" ~ "2| Street address with route service",
      toupper(PCtype) %in% "3" ~ "3| Post Office box",
      toupper(PCtype) %in% "4" ~ "4| Route service",
      toupper(PCtype) %in% "5" ~ "5| General delivery",
      toupper(PCtype) %in% "9" ~ "9| Not applicable / Missing"
    ),
    fmt_dmt = case_when(
      toupper(DMT) %in% "A" ~ "A| Letter carrier to street address",
      toupper(DMT) %in% "B" ~ "B| Letter carrier to apartment building",
      toupper(DMT) %in% "E" ~ "E| Delivery to business building",
      toupper(DMT) %in% "G" ~ "G| Delivery to large volume receiver",
      toupper(DMT) %in% "H" ~ "H| Delivery via rural route",
      toupper(DMT) %in% "J" ~ "J| General delivery",
      toupper(DMT) %in% "K" ~ "K| Small PO Box (not community box)",
      toupper(DMT) %in% "M" ~ "M| Large volume receiver (Large PO Box)",
      toupper(DMT) %in% "T" ~ "T| Suburban route service",
      toupper(DMT) %in% "W" ~ "W| Rural postal code",
      toupper(DMT) %in% "X" ~ "X| Mobile route",
      toupper(DMT) %in% "Z" ~ "Z| Retired postal code",
      toupper(DMT) %in% "9" ~ "9| Not applicable / Missing"
    )
  )

# Format the 'final_hlthout' and 'final_problem' file and
# clean up processing variables

outdata <- final_hlthout %>% 
  mutate(
    Version = version,
    coder = ifelse(
      codeVersion %in% 0,
      "R",
      "I"
    ),
    CARuid = CARUID,
    DA16uid = DA16UID,
    DB16uid = DB16UID
  ) %>% 
  select(
    "coder", "Version", "ID", "PCODE", "PR", "DAuid", "DB", "DB_ir2021", "CSDuid", "CSDname", "CSDtype",
    "CMA", "CMAtype", "CMAname", "CTname", "Tracted", "SACcode", "SACtype", "CCSuid",
    "FEDuid", "FEDname", "DPLuid", "DPLtype", "DPLname",
    "ERuid", "ERname", "CARuid", "CARname",
    "PopCtrRAPuid", "PopCtrRAname", "PopCtrRAtype", "PopCtrRAclass", "CSIZE", "CSIZEMIZ",
    "HRuid", "HRename", "HRfname", "AHRuid", "AHRename", "AHRfname",
    "SLI", "Rep_Pt_Type", "RPF", "PCtype", "DMT", "H_DMT", "DMTDIFF", "PO", "QI", "Source", "LAT", "LONG",
    "Link_Source", "Link", "nCD", "nCSD", "PREC", "Comm_Name", "Airlift", "InstFlag", "ResFlag",
    "InuitLands", "BTIPPE", "ATIPPE", "QABTIPPE", "QNBTIPPE", "DABTIPPE", "DNBTIPPE", "QAATIPPE", "QNATIPPE",
    "DAATIPPE", "DNATIPPE", "IMPFLG", "DA11uid", "DB11uid", "DA06uid", "DA01uid", "EA96uid", "EA91uid",
    "EA86uid", "EA81uid", "DA16uid", "DB16uid"
  ) %>% 
  arrange(PCODE)

outdata_prblm <- final_problem %>% 
  mutate(
    Version = version,
    coder = ifelse(
      codeVersion %in% 0,
      "R",
      "I"
    ),
    CARuid = CARUID,
    DA16uid = DA16UID,
    DB16uid = DB16UID
  ) %>% 
  select(
    "coder", "Version", "ID", "PCODE", "Message", "PR", "DAuid", "DB", "CSDuid", "CSDtype",
    "CMA", "CTname", "DMT", "DMTDIFF", "Link_Source", "Link", "InstFlag", "ResFlag",
    "ADR"
  ) %>% 
  arrange(Link, DMT, PCODE, CSDuid, DAuid, ID)

# Perform the final cleanup to remove all input and
# processing files 

rm(list = setdiff(ls(), c("outdata","outdata_prblm",
                          'outData', "outName")))

arrow::write_parquet(outdata %>% 
                     # group_by(PCODE) %>%
                     #   tidyr::fill(c(CSDuid, CSDname, CSDtype,
                     #                 CMAname, CMAtype, SACcode,
                     #                 FEDuid, FEDname, ERuid, ERname,
                     #                 CARuid, CARname,
                     #                 HRuid, HRename, HRfname),
                     #               .direction = "downup") %>%
                       ungroup(),
                     paste0(outData,outName,".parquet"))

arrow::write_parquet(outdata_prblm,
                     paste0(outData,outName,"_problem.parquet"))

return(outdata)
}

