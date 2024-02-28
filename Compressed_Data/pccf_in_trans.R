# Translate the official '.sas' scripts to '.R'
# Use '.parquet' (https://www.databricks.com/glossary/what-is-parquet)
# dataset instead of '.sas7bdat'

pccf_in <- paste0(getwd(),"/Compressed_Data/pccf_in")

temp <- list.files("H:\\RCP\\RCP_Data\\TeixeiEC\\PCCF\\2023\\PCCF8A1\\Data_SAS\\pccf_in",
                   pattern = ".sas7bdat")

myfiles <- lapply(file.path(paste0("H:\\RCP\\RCP_Data\\TeixeiEC\\PCCF\\2023\\PCCF8A1\\Data_SAS\\pccf_in\\",
                                   temp)),
                  haven::read_sas)

mapply(arrow::write_parquet,
       myfiles,
       sink = gsub(".sas7bdat",
                   ".parquet",
                   file.path(paste0("H:\\RCP\\RCP_Data\\TeixeiEC\\PCCF\\2023\\PCCF8A1\\Compressed_Data\\pccf_in\\",
                                    temp))))

# Geography reference files ----

temp <- list.files("H:\\RCP\\RCP_Data\\TeixeiEC\\PCCF\\2023\\PCCF8A1\\Data_SAS\\georef_in",
                   pattern = ".sas7bdat")

myfiles <- lapply(file.path(paste0("H:\\RCP\\RCP_Data\\TeixeiEC\\PCCF\\2023\\PCCF8A1\\Data_SAS\\georef_in\\",
                                   temp)),
                  haven::read_sas)

mapply(arrow::write_parquet,
       myfiles,
       sink = gsub(".sas7bdat",
                   ".parquet",
                   file.path(paste0("H:\\RCP\\RCP_Data\\TeixeiEC\\PCCF\\2023\\PCCF8A1\\Compressed_Data\\georef_in\\",
                                    temp))))
