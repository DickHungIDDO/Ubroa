  
  ###################################################################
  # Aim: a trailer made P21 rule checking system for IDDO data upload.
  # Date: 17012022
  # Author: Dick Hung
  #
  # Version 1.0: 1. program initiation  
  # Version 2.0: 1. input data chunk by chunk_size parameter
  #              2. glimpse have been introduced for speed up process
  #              3. hyperlink for Rule Line Number and Data Line Number has
  #                 been remarked to speed up processing
  # Version 3.0: 1. Check for blank value
  #              2. check rules for NULL test name
  #              3. vlistb include blank in list
  #              4. null, notnull check for null and not null value
  # Version 4.0: 1. Check string checking A-Z, a-z, ',', '-' , ' '
  #              2. Domain var names checking
  #              3. Unit conversion calculation, add Update column in result tab
  #              4. Remarked data details attachment for output
  #              5. Add No. column in rule file
  #              6. Add domain_TESTCD for non XXTESTCD domain
  #              7. Add unit table for unit checking
  # Version 5.0: 1. Check for NI and VS Domain conversion
  # Version 6.0: 1. Check for MB and Cross Domain conversion
  ####################################################################
  
  
  # rm(list = ls()) 
  
  # install.packages("survPen")
  # library(survPen)
  # install.packages("easyr")
  # library
  
  # install.packages("plyr")
  
  # install.packages("data.table")
  # install.packages("xlsx")
  # install.packages("WriteXLS")
  # 
  # install.packages("openxlsx")
  #  # loads library and doesn't require Java installed
  # install.packages("doSNOW")
  # #
  # install.packages("doParallel")
  # # 
  # install.packages("doMPI")
  # remotes::install_github("ycphs/openxlsx")
  
  # library(WriteXLS)
  # library(data.table)
  # library(xlsx)
  # install.packages("parallel")
  # install.packages("stringr") 
  
  # for parallel
  list.of.packages <- c(
    "parallel",
    "stringr" ,
    "openxlsx",
    "plyr",
    "dplyr",
    "measurements",
    "foreach",
    "doParallel",
   # "ranger",
  #  "palmerpenguins",
  #  "tidyverse",
  #  "kableExtra",
    # "doMPI",
    "doSNOW",
    "stringr"
  )
  
  
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

  if(length(new.packages) > 0){
    install.packages(new.packages, dep=TRUE)
  }
  
  #loading packages
  for(package.i in list.of.packages){
    suppressPackageStartupMessages(
      library(
        package.i, 
        character.only = TRUE
      )
    )
  }
  library(parallel) 
  library(stringr)
  # library(doSNOW)
  library(openxlsx)
  library(plyr)
  library(dplyr)
  library(measurements)
  library(data.table)
  # LD_LIBRARY_PATH=/usr/lib64/openmpi/lib R -q -e " library('Rmpi')"
  # library(Rmpi)
  
  ## 
  # if (!requireNamespace("BiocManager", quietly = TRUE))
  #   install.packages("measurements")
  # 
  # BiocManager::install("gpuMagic")
  # library(gpuMagic)
  
  
  pg_version <- 6.0
  massList <-  paste(conv_unit_options$mass, collapse=", ")
  
  # data and rules files 
  # para_file <- 'd:/R/V4a_debug/para.csv'
  # data_file <- 'd:/R/V4a_debug/VS_test_data_1.csv' # 'd:/dick/CVRAPID_RS_2021-09-20_4.csv'  #  
  # rule_file  <- 'd:/R/V4a_debug/VS_rules_1.csv'
  # domain_file  <- 'd:/dick/domain_TESTCD.csv'
  # unit_table_file  <- 'd:/R/V4a_debug/unit_table.csv'
  # vs_convert_file  <- 'd:/dick/vs_conv.csv'
  # rpt_dir <- 'd:/dick/'  # Remarked by Dick@30052022 parameters read in file.
  # start_chunk <- 1
  # chunk_size <- 1000
  rule_col_chk <- "No,DOMAIN,Target,operator,Min,Max,Update,REF,Desc"
  data_col_chk <- "STUDYID,DOMAIN,USUBJID"
  class_type_chk <- c("character","numeric","integer","logical","complex")
  
  rule_col_chk <- as.list(el(strsplit(rule_col_chk, ","))) 
  data_col_chk <- as.list(el(strsplit(data_col_chk, ","))) 
  #class_type_chk <- as.list(el(strsplit(class_type, ","))) 
  
  dtl_err_op_out <- "varchk,convchk" 
  dtl_err_op_out <- as.list(el(strsplit(dtl_err_op_out, ","))) 
  
  message('Start time: ', format(Sys.time(), "%d-%m-%Y %H:%M:%S")) 
  StartTime <-  format(Sys.time(), "%d-%m-%Y %H:%M:%S")
  
  
  # operator function 
  check_Sign <- function(x) {
    if (x>=0) {
      return (0)
    } 
    return(1)
  }
  
  check_Null <- function(x) {
    if (length(x)==0){
      return (0)
    } 
    else if (x==""){
      return (0)
    } 
    
  
    return(1)
  }
  
  check_val <- function(x,y) {
    
    # message(x," <---------->  ",y)
    if (x>y) {
      return (0)
    }
    else if (x<y) {
      return (1)
    }
    else if (x==y){
      return (2)
    }
    else if (x!=y){
      return (3)
    }
  }
  
  
  error_exit <- function(err_msg) {
    # 
    # message(err_msg) 
    csv_file_r_name <- paste0(rpt_dir,"IDDO_Res_P21C_",
                              substr(gsub("\\..*", "", gsub(".*/.*/", "", data_file)), 1, 3),"_",
                              substr(gsub("\\..*", "", gsub(".*/.*/", "", rule_file)), 1, 3),"_error_",
                              format(Sys.time(),"%d%m%Y_%H%M%S"),".xlsx")


    # OsheetName = list('Validation Summary', 'Dataset Summary','Issue Summary', 'Details', 'Rules')
    RptHeader <- "ODDI P21 Validation Report "
    Summary <- list()
    Summary <- append(Summary, RptHeader)
    Summary <- append(Summary, paste('Rules file: ',rule_file))
    Summary <- append(Summary, paste('Data file: ',data_file))
    Summary <- append(Summary, paste('Generate Date: ',format(Sys.time(), "%d-%m-%Y")))
    Summary <- append(Summary, paste('Software Version: ', pg_version))
    
    if (data_file_no > 1)
    {
      
      for (i in DOMAIN_P21)
      {  
         
        eval(parse(text = paste0("Summary <- append(Summary, paste('Total data size for ",i," Domain: ',nrow(",i,"_data_P21)))")))
      }
    
    }else{
      
        Summary <- append(Summary, paste('Total data size: ',nrow(data_P21)))
    }

    Summary <- append(Summary, paste(''))
    Summary <- append(Summary, paste(''))


    Summary <- append(Summary, err_msg)

    Out_Summary        <- data.frame(No=1:length(Summary))
    Out_Summary$Details <- Summary


    ## Create Sheets

    wb <- createWorkbook()
    glimpse(wb)
    addWorksheet(wb, "Validation Summary")
    writeData(wb, sheet = "Validation Summary", x = Out_Summary, startCol = 1)

    saveWorkbook(wb, csv_file_r_name  , overwrite = TRUE)

    stop(err_msg)
  }
  
  ########################################
  
  run_unitchk <- function(In_Domain, In_cur_testcd, In_test_cnt, OrulesIn_list, OdataIn_list, OrefIn_list,
                          OdescIn_ref, #OclassIn_ref,
                          OdomainIn_ref, OrstestcdIn_ref, OtargetIn_ref, OunitchkIn_ref, OunitchkUNITIN_ref,
                          OrulesIn_ref, OdataIn_ref, othIn_NotFound)
  { 
    
    tmp <- list()
    for (d_line_no in which(data_P21_cur[,"DOMAIN"] == In_Domain &
                            data_P21_cur[,cur_testcd] == In_cur_testcd &
                            data_P21_cur[,"VSORRESU"] == '')) #check for no unit
    {
      
      tmp <- append(tmp,d_line_no)
      
      OrulesIn_list <- append(OrulesIn_list, rules_P21[In_test_cnt,"No"])
      OdataIn_list <- append(OdataIn_list, d_line_no)
      OrefIn_list <- append(OrefIn_list, rules_P21[In_test_cnt,"REF"])
      OdescIn_ref <- append(OdescIn_ref, paste(rules_P21[In_test_cnt,"Desc"], ' (Data Unit not found)'))
      # OclassIn_ref <- append(OclassIn_ref, rules_P21[In_test_cnt,"Class"])
      OdomainIn_ref <- append(OdomainIn_ref, rules_P21[In_test_cnt,"DOMAIN"]) #paste( rules_P21[test_cnt,"DOMAIN"], ' aaa'))
      OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[In_test_cnt,cur_testcd])
      OtargetIn_ref <- append(OtargetIn_ref, rules_P21[In_test_cnt,"Target"]) 
      
      OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[In_test_cnt,], collapse=", "))
      OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", "))  
      OunitchkIn_ref <- append(OunitchkIn_ref," ")
      OunitchkUNITIN_ref <- append(OunitchkUNITIN_ref, ' ')
      
    }
    
    othIn_NotFound <- append(othIn_NotFound, paste('Data line:',paste(tmp, collapse=', ' ), 'Unit not found'))
    
    for (d_line_no in which(data_P21_cur[,"DOMAIN"] == In_Domain &
                            data_P21_cur[,cur_testcd] == In_cur_testcd &
                            data_P21_cur[,"VSORRESU"] != '' &
                            data_P21_cur[,rules_P21[In_test_cnt,"Update"]] == '')) #check for no update value
    {
      OrulesIn_list <- append(OrulesIn_list, rules_P21[In_test_cnt,"No"])
      OdataIn_list <- append(OdataIn_list, d_line_no)
      OrefIn_list <- append(OrefIn_list, rules_P21[In_test_cnt,"REF"])
      OdescIn_ref <- append(OdescIn_ref, rules_P21[In_test_cnt,"Desc"])
      # OclassIn_ref <- append(OclassIn_ref, rules_P21[In_test_cnt,"Class"])
      OdomainIn_ref <- append(OdomainIn_ref, rules_P21[In_test_cnt,"DOMAIN"]) #paste( rules_P21[test_cnt,"DOMAIN"], ' aaa'))
      OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[In_test_cnt,cur_testcd])
      OtargetIn_ref <- append(OtargetIn_ref, rules_P21[In_test_cnt,"Target"]) 
      
      OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[In_test_cnt,], collapse=", "))
      OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", "))  
      OunitchkIn_ref <- append(OunitchkIn_ref,as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Max"]]))
      
      tmp <- which(unit_table_P21[,"DOMAIN"] == In_Domain &
                   unit_table_P21[,cur_testcd] == In_cur_testcd &
                   unit_table_P21[,"VSORRESU"] ==  data_P21_cur[d_line_no,"VSORRESU"])
      OunitchkUNITIN_ref <- append(OunitchkUNITIN_ref, unit_table_P21[tmp,"VSSTRESU"])
      
      
    }
    
    
    #####################################
    for(test_cnt in which(unit_table_P21[,"Conversion"] == '' & #check for same unit, no  need conversion
                          unit_table_P21[,cur_testcd] == In_cur_testcd)) 
    { 
      for (d_line_no in which(data_P21_cur[,"DOMAIN"] == In_Domain &
                               data_P21_cur[,cur_testcd] == In_cur_testcd &
                              data_P21_cur[,"VSORRESU"] != '' &
                              data_P21_cur[,rules_P21[In_test_cnt,"Update"]] != '' & 
                              data_P21_cur[,"VSORRESU"] == unit_table_P21[test_cnt,"VSORRESU"]))
      { 
             if (as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Max"]])!= 
                 as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update"]])|
                 data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update"]] == "")
             { 
               OrulesIn_list <- append(OrulesIn_list, rules_P21[In_test_cnt,"No"])
               OdataIn_list <- append(OdataIn_list, d_line_no)
               OrefIn_list <- append(OrefIn_list, rules_P21[In_test_cnt,"REF"])
               OdescIn_ref <- append(OdescIn_ref, rules_P21[In_test_cnt,"Desc"])
               # OclassIn_ref <- append(OclassIn_ref, rules_P21[In_test_cnt,"Class"])
               OdomainIn_ref <- append(OdomainIn_ref, rules_P21[In_test_cnt,"DOMAIN"]) #paste( rules_P21[test_cnt,"DOMAIN"], ' aaa'))
               OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[In_test_cnt,cur_testcd])
               OtargetIn_ref <- append(OtargetIn_ref, rules_P21[In_test_cnt,"Target"]) 
               
               OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[In_test_cnt,], collapse=", "))
               OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", ")) 
               if (data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Max"]] == '')
               {
                 OunitchkIn_ref <- append(OunitchkIn_ref,'Source/ Unit is missing!!')
                 
               }
               else
               {
                 OunitchkIn_ref <- append(OunitchkIn_ref,as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Max"]]))
                 
               }
               OunitchkUNITIN_ref <- append(OunitchkUNITIN_ref, unit_table_P21[test_cnt,"VSSTRESU"])
                
             } 
      }
    }
     
    for(test_cnt in which(unit_table_P21[,"Conversion"] != ''&  #check for different unit, need conversion
                          unit_table_P21[,cur_testcd] == In_cur_testcd)) 
    {
      
      for (d_line_no in which(data_P21_cur[,"DOMAIN"] == In_Domain &
                              data_P21_cur[,cur_testcd] == In_cur_testcd &
                              data_P21_cur[,"VSORRESU"] != '' &
                              data_P21_cur[,rules_P21[In_test_cnt,"Update"]] != '' &
                              data_P21_cur[,"VSORRESU"] == unit_table_P21[test_cnt,"VSORRESU"]))
      {
        if (round(eval(parse(text=paste0(sub("X", paste0("data_P21_cur[d_line_no,\"",
                                  rules_P21[In_test_cnt,"Max"],"\"] "),unit_table_P21[test_cnt,"Conversion"])))), digits = 2)
            != as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update"]])|
            data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update"]] == "")
        {
          OrulesIn_list <- append(OrulesIn_list, rules_P21[In_test_cnt,"No"])
          OdataIn_list <- append(OdataIn_list, d_line_no)
          OrefIn_list <- append(OrefIn_list, rules_P21[In_test_cnt,"REF"])
          OdescIn_ref <- append(OdescIn_ref, rules_P21[In_test_cnt,"Desc"])
          # OclassIn_ref <- append(OclassIn_ref, rules_P21[In_test_cnt,"Class"])
          OdomainIn_ref <- append(OdomainIn_ref, rules_P21[In_test_cnt,"DOMAIN"]) #paste( rules_P21[test_cnt,"DOMAIN"], ' aaa'))
          OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[In_test_cnt,cur_testcd])
          OtargetIn_ref <- append(OtargetIn_ref, rules_P21[In_test_cnt,"Target"]) 
          
          OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[In_test_cnt,], collapse=", "))
          OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", ")) 
          if (data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Max"]] == '')
          {
            OunitchkIn_ref <- append(OunitchkIn_ref,'Source/ Unit is missing!!')
            
          }
          else
          {
            OunitchkIn_ref <- append(OunitchkIn_ref,
                                     round(eval(parse(text=paste0(sub("X", paste0("data_P21_cur[d_line_no,\"",
                                     unit_table_P21[test_cnt,"Var"],"\"] "),unit_table_P21[test_cnt,"Conversion"])))), digits = 2))
            
          }
          OunitchkUNITIN_ref <- append(OunitchkUNITIN_ref, unit_table_P21[test_cnt,"VSSTRESU"])
          
        } 
        
      }
    }
    
    
    # update parent 
    eval.parent(substitute( Orules_list <- OrulesIn_list ))
    eval.parent(substitute( Odata_list <- OdataIn_list ))
    eval.parent(substitute( Oref_list <- OrefIn_list ))
    eval.parent(substitute( Odesc_ref <- OdescIn_ref ))
    # eval.parent(substitute( Oclass_ref <- OclassIn_ref ))
    eval.parent(substitute( Odomain_ref <- OdomainIn_ref ))
    eval.parent(substitute( Orstestcd_ref <- OrstestcdIn_ref ))
    eval.parent(substitute( Otarget_ref <- OtargetIn_ref ))
    eval.parent(substitute( Ounitchk_ref <- OunitchkIn_ref ))
    eval.parent(substitute( OunitchkUNIT_ref <- OunitchkUNITIN_ref ))
    
    eval.parent(substitute( Orules_ref <- OrulesIn_ref  ))
    eval.parent(substitute( Odata_ref <- OdataIn_ref ))
    eval.parent(substitute( oth_NotFound <- othIn_NotFound ))  
    
    
    
  }
  
  
  
  
  #############################################
  
  
  run_convchk <- function(In_Domain, In_cur_testcd, In_test_cnt, OrulesIn_list, OdataIn_list, OrefIn_list,
                          OdescIn_ref, #OclassIn_ref,
                          OdomainIn_ref, OrstestcdIn_ref, OtargetIn_ref, OunitchkIn_ref, OunitchkUNITIN_ref,
                          OrulesIn_ref, OdataIn_ref, othIn_NotFound)
  { 
    
    
    
    tmp <- list()
    for (d_line_no in which(data_P21_cur[,"DOMAIN"] == In_Domain &
                            data_P21_cur[,cur_testcd] == In_cur_testcd &
                            data_P21_cur[,"MBTSTDTL"] == "QUANTIFICATION" &
                            ( data_P21_cur[,"MBSTRESC"] == '' |
                              data_P21_cur[,"MBSTRESN"] == '' |
                              data_P21_cur[,"MBSTRESU"] == ''  ))) #check for no unit
    {
      
      tmp <- append(tmp,d_line_no)
      
      OrulesIn_list <- append(OrulesIn_list, rules_P21[In_test_cnt,"No"])
      OdataIn_list <- append(OdataIn_list, d_line_no)
      OrefIn_list <- append(OrefIn_list, rules_P21[In_test_cnt,"REF"])
      OdescIn_ref <- append(OdescIn_ref, paste(rules_P21[In_test_cnt,"Desc"], ' (Data Unit not found)'))
      # OclassIn_ref <- append(OclassIn_ref, rules_P21[In_test_cnt,"Class"])
      OdomainIn_ref <- append(OdomainIn_ref, rules_P21[In_test_cnt,"DOMAIN"]) #paste( rules_P21[test_cnt,"DOMAIN"], ' aaa'))
      OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[In_test_cnt,cur_testcd])
      OtargetIn_ref <- append(OtargetIn_ref, rules_P21[In_test_cnt,"Target"]) 
      
      OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[In_test_cnt,], collapse=", "))
      OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", "))  
      OunitchkIn_ref <- append(OunitchkIn_ref," ")
      OunitchkUNITIN_ref <- append(OunitchkUNITIN_ref, ' ')
      
    }
    
    othIn_NotFound <- append(othIn_NotFound, paste('Data line:',paste(tmp, collapse=', ' ), 'Unit not found'))
    
         ##################################### 
    for(test_cnt in which(MB_conv_P21[,"Conversion"] != ''&  #check for different unit, need conversion
                          MB_conv_P21[,cur_testcd] == In_cur_testcd &
                          MB_conv_P21[,"MBTESTCD"] == data_P21_cur[d_line_no,"MBTESTCD"] &
                          MB_conv_P21[,"MBORRESU"] == data_P21_cur[d_line_no,"MBORRESU"])) 
    {
      
      for (d_line_no in which(data_P21_cur[,"DOMAIN"] == In_Domain &
                              data_P21_cur[,cur_testcd] == In_cur_testcd &
                              data_P21_cur[,"MBTSTDTL"] == "QUANTIFICATION" &
                              data_P21_cur[,"MBSTRESC"] != '' &
                              data_P21_cur[,"MBSTRESN"] != '' &
                              data_P21_cur[,"MBSTRESU"] != ''  )) 
      {
        message(data_P21_cur[d_line_no, rules_P21[In_test_cnt,"Target"]])
        message('TEST................')
        
        eval(parse(text = paste0('v1 <- data_P21_cur[',d_line_no, ', rules_P21[',In_test_cnt,',\"Target\"]]')))
        
        # message(test_cnt) 
        # data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Max"]]
        message(round(eval(parse(text = paste0(v1,' ',sub('X','',MB_conv_P21[test_cnt,"Conversion"])))), digits = 2))
        message('TEST 2................')
        # message(as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update"]]))
        # message(as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Target"]]))
        # 
        # message('                ')
        
        
        err_desc <- ''
        if ((round(eval(parse(text = paste0(v1,' ',sub('X','',MB_conv_P21[test_cnt,"Conversion"])))), digits = 2)
             != round(as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update"]]), digits = 2)))
        {
          err_desc <- 'Conversion Calculation Error'
        }
        
        else if (round(as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Max"]]), digits = 2)
             != round(as.numeric(data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update"]]), digits = 2))
        {
          err_desc <- paste0(rules_P21[In_test_cnt,"Max"],' != ',rules_P21[In_test_cnt,"Update"])
        }
        else if (MB_conv_P21[which(MB_conv_P21[,cur_testcd] ==  In_cur_testcd &
                               MB_conv_P21[,rules_P21[In_test_cnt,"Min"]] == data_P21_cur[d_line_no, rules_P21[In_test_cnt,"Min"]] &  
                               MB_conv_P21[,"DOMAIN"] ==  In_Domain   ), rules_P21[In_test_cnt,"Update_1"]]
             != data_P21_cur[d_line_no,rules_P21[In_test_cnt,"Update_1"]])
        {
          err_desc <- paste0(rules_P21[In_test_cnt,"Update_1"],' value incorrect')
        }
        if(err_desc!='')  
        {
          message(err_desc)
          OrulesIn_list <- append(OrulesIn_list, rules_P21[In_test_cnt,"No"])
          OdataIn_list <- append(OdataIn_list, d_line_no)
          OrefIn_list <- append(OrefIn_list, rules_P21[In_test_cnt,"REF"])
          OdescIn_ref <- append(OdescIn_ref, paste0(rules_P21[In_test_cnt,"Desc"],' | ',err_desc))
          # OclassIn_ref <- append(OclassIn_ref, rules_P21[In_test_cnt,"Class"])
          OdomainIn_ref <- append(OdomainIn_ref, rules_P21[In_test_cnt,"DOMAIN"]) #paste( rules_P21[test_cnt,"DOMAIN"], ' aaa'))
          OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[In_test_cnt,cur_testcd])
          OtargetIn_ref <- append(OtargetIn_ref, rules_P21[In_test_cnt,"Target"]) 
          
          OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[In_test_cnt,], collapse=", "))
          OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", ")) 
       
          OunitchkIn_ref <- append(OunitchkIn_ref,' ')
          OunitchkUNITIN_ref <- append(OunitchkUNITIN_ref,' ')
          
        } 
        
      }
    }
    
    
    # update parent 
    eval.parent(substitute( Orules_list <- OrulesIn_list ))
    eval.parent(substitute( Odata_list <- OdataIn_list ))
    eval.parent(substitute( Oref_list <- OrefIn_list ))
    eval.parent(substitute( Odesc_ref <- OdescIn_ref ))
    # eval.parent(substitute( Oclass_ref <- OclassIn_ref ))
    eval.parent(substitute( Odomain_ref <- OdomainIn_ref ))
    eval.parent(substitute( Orstestcd_ref <- OrstestcdIn_ref ))
    eval.parent(substitute( Otarget_ref <- OtargetIn_ref ))
    eval.parent(substitute( Ounitchk_ref <- OunitchkIn_ref ))
    eval.parent(substitute( OunitchkUNIT_ref <- OunitchkUNITIN_ref ))
    
    eval.parent(substitute( Orules_ref <- OrulesIn_ref  ))
    eval.parent(substitute( Odata_ref <- OdataIn_ref ))
    eval.parent(substitute( oth_NotFound <- othIn_NotFound ))  
    
    
    
  } #############################################
  run_checking_BLK <- function(TDomainBLK, test_domain, target_domain, test_cnt, op,
                              OrulesIn_list, OdataIn_list, OrefIn_list,
                              OdescIn_ref, # OclassIn_ref,
                              OdomainIn_ref, OrstestcdIn_ref, OtargetIn_ref, OunitchkIn_ref, OunitchkUNITIN_ref,
                              OrulesIn_ref, OdataIn_ref, othIn_NotFound)
  { 
    if (rules_P21[test_cnt,"operator"] == 'unitchk')
    {
      return(TRUE)
    }else{   
        for (t in TDomainBLK)
        {  
          eval(parse(text = paste0("target <- ",test_domain, "_data_P21[t,\"",rules_P21[test_cnt,"Target"],"\"]") ))
          
          eval(parse(text = paste0("TDomainBLK_in<- which(",target_domain, "_data_P21[,\"USUBJID\"]==ref_UID)") )) 
          
          for (t_in in TDomainBLK_in)
          {
             if (op %in% c(">=","<=","=","<","!=",">") ){
               tmin <- rules_P21[test_cnt,"Min"]
               tmax <- rules_P21[test_cnt,"Max"]
               
             }else{
              eval(parse(text = paste0("tmin<- ",target_domain, "_data_P21[t_in,\"",rules_P21[test_cnt,"Min"],"\"]") ))
              eval(parse(text = paste0("tmax<- ",target_domain, "_data_P21[t_in,\"",rules_P21[test_cnt,"Max"],"\"]") ))
               
             }
             
            message('testing..........in ')
            
            if (rule_checking_CD(d_line_no, target, rules_P21[test_cnt,"operator"], tmin,tmax) == F)  
              # for non unitchk op
            {  
              OrulesIn_list <- append(OrulesIn_list, rules_P21[test_cnt,"No"])
              OdataIn_list <- append(OdataIn_list, d_line_no)
              OrefIn_list <- append(OrefIn_list, rules_P21[test_cnt,"REF"])
              OdescIn_ref <- append(OdescIn_ref, rules_P21[test_cnt,"Desc"])
              # OclassIn_ref <- append(OclassIn_ref, rules_P21[test_cnt,"Class"]) 
              OdomainIn_ref <- append(OdomainIn_ref,  rules_P21[test_cnt,"DOMAIN"]) # paste( rules_P21[test_cnt,"DOMAIN"], ' ccc'))# 
              OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[test_cnt,cur_testcd])
              OtargetIn_ref <- append(OtargetIn_ref, rules_P21[test_cnt,"Target"]) 
              OunitchkIn_ref<-append(OunitchkIn_ref, ' ')
              OunitchkUNITIN_ref<-append(OunitchkUNITIN_ref,' ')
              OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[test_cnt,], collapse=", "))
              OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", "))
              
            } 
          }
        }
      
      # update parent 
      eval.parent(substitute( Orules_list <- OrulesIn_list ))
      eval.parent(substitute( Odata_list <- OdataIn_list ))
      eval.parent(substitute( Oref_list <- OrefIn_list ))
      eval.parent(substitute( Odesc_ref <- OdescIn_ref ))
      # eval.parent(substitute( Oclass_ref <- OclassIn_ref ))
      eval.parent(substitute( Odomain_ref <- OdomainIn_ref ))
      eval.parent(substitute( Orstestcd_ref <- OrstestcdIn_ref ))
      eval.parent(substitute( Otarget_ref <- OtargetIn_ref ))
      eval.parent(substitute( Ounitchk_ref <- OunitchkIn_ref ))
      eval.parent(substitute( OunitchkUNIT_ref <- OunitchkUNITIN_ref ))
      
      eval.parent(substitute( Orules_ref <- OrulesIn_ref  ))
      eval.parent(substitute( Odata_ref <- OdataIn_ref ))
      eval.parent(substitute( oth_NotFound <- othIn_NotFound ))  
    }
    
  }
  
  #############################################
  run_checking_CD <- function(ref_UID, test_domain, target_domain, test_cnt, op,
                              OrulesIn_list, OdataIn_list, OrefIn_list,
                              OdescIn_ref, # OclassIn_ref,
                              OdomainIn_ref, OrstestcdIn_ref, OtargetIn_ref, OunitchkIn_ref, OunitchkUNITIN_ref,
                              OrulesIn_ref, OdataIn_ref, othIn_NotFound)
  { 
    if (rules_P21[test_cnt,"operator"] == 'unitchk')
    {
      return(TRUE)
    }else{ 
      eval(parse(text = paste0("data_P21_cur <- ",test_domain, "_data_P21_cur")))
      message(ref_UID)
      message(test_cnt)
      message('testing....qqq')
      for(d_line_no in which(data_P21_cur[,"USUBJID"]==ref_UID)) # loop for data files
      {  
          message('testing....A')
          
          eval(parse(text = paste0("target <- ",test_domain, "_data_P21_cur[d_line_no,\"",rules_P21[test_cnt,"Target"],"\"]") ))
          eval(parse(text = paste0("TDomainBLK<- which(",target_domain, "_data_P21[,\"USUBJID\"]==ref_UID)") )) 
           
          
          message(TDomainBLK)
          message(target_domain)
          message('testing....B')
          for (t in TDomainBLK)
          { 
            if (op %in% c(">=","<=","=","<","!=",">") ){
               tmin <- rules_P21[test_cnt,"Min"]
               tmax <- rules_P21[test_cnt,"Max"]
              
            }else{
              
                eval(parse(text = paste0("tmin<- ",target_domain, "_data_P21[t,\"",rules_P21[test_cnt,"Min"],"\"]") ))
                eval(parse(text = paste0("tmax<- ",target_domain, "_data_P21[t,\"",rules_P21[test_cnt,"Max"],"\"]") ))
            }
            # message(test_domain)
            # message(eval(parse(text = paste0(test_domain, "_data_P21_cur[1,\"",rules_P21[test_cnt,"Target"], "\"]")))) 
            # message(difftime(as.Date(target), as.Date(tmin), units = "days"))
            
                    
            if (rule_checking_CD(d_line_no, target, rules_P21[test_cnt,"operator"], tmin,tmax) == F)  
             # for non unitchk op
            {  
              err_desc <- ''
              if (op %in% c("d>=","d<=","d=","d<","d!=","d>") )
              {
                err_desc <- paste0(ref_UID,':',rules_P21[test_cnt,"Target"],' -> ',target,'  ',rules_P21[test_cnt,"Min"],' -> ',tmin)
              }
              
              message(paste0(ref_UID,' ->   Error found............'))
              OrulesIn_list <- append(OrulesIn_list, rules_P21[test_cnt,"No"])
              OdataIn_list <- append(OdataIn_list, d_line_no)
              OrefIn_list <- append(OrefIn_list, rules_P21[test_cnt,"REF"])
              OdescIn_ref <- append(OdescIn_ref, paste0( rules_P21[test_cnt,"Desc"],'|',err_desc))
              # OclassIn_ref <- append(OclassIn_ref, rules_P21[test_cnt,"Class"]) 
              OdomainIn_ref <- append(OdomainIn_ref,  rules_P21[test_cnt,"DOMAIN"]) # paste( rules_P21[test_cnt,"DOMAIN"], ' ccc'))# 
              OrstestcdIn_ref <- append(OrstestcdIn_ref, ' ')
              OtargetIn_ref <- append(OtargetIn_ref, rules_P21[test_cnt,"Target"]) 
              OunitchkIn_ref<-append(OunitchkIn_ref, ' ')
              OunitchkUNITIN_ref<-append(OunitchkUNITIN_ref,' ')
              OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[test_cnt,], collapse=", "))
              OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", "))
              
           } 
          }
      }
      
      
      
      
      # update parent 
      eval.parent(substitute( Orules_list <- OrulesIn_list ))
      eval.parent(substitute( Odata_list <- OdataIn_list ))
      eval.parent(substitute( Oref_list <- OrefIn_list ))
      eval.parent(substitute( Odesc_ref <- OdescIn_ref ))
      # eval.parent(substitute( Oclass_ref <- OclassIn_ref ))
      eval.parent(substitute( Odomain_ref <- OdomainIn_ref ))
      eval.parent(substitute( Orstestcd_ref <- OrstestcdIn_ref ))
      eval.parent(substitute( Otarget_ref <- OtargetIn_ref ))
      eval.parent(substitute( Ounitchk_ref <- OunitchkIn_ref ))
      eval.parent(substitute( OunitchkUNIT_ref <- OunitchkUNITIN_ref ))
      
      eval.parent(substitute( Orules_ref <- OrulesIn_ref  ))
      eval.parent(substitute( Odata_ref <- OdataIn_ref ))
      eval.parent(substitute( oth_NotFound <- othIn_NotFound ))  
      }
    
  }
  
  #############################################
  run_checking <- function(d_line_no, test_cnt, op,
                           OrulesIn_list, OdataIn_list, OrefIn_list,
                           OdescIn_ref, # OclassIn_ref,
                           OdomainIn_ref, OrstestcdIn_ref, OtargetIn_ref, OunitchkIn_ref, OunitchkUNITIN_ref,
                           OrulesIn_ref, OdataIn_ref, othIn_NotFound)
  { 
    if (rules_P21[test_cnt,"operator"] == 'unitchk')
    {
       return(TRUE)
    }
    else if ((rule_checking(d_line_no, 
                           rules_P21[test_cnt,"Target"], 
                           rules_P21[test_cnt,"operator"], 
                           rules_P21[test_cnt,"Min"], 
                           rules_P21[test_cnt,"Max"]) == F)  
             ) # for non unitchk op
    {
      
       
      
      OrulesIn_list <- append(OrulesIn_list, rules_P21[test_cnt,"No"])
      OdataIn_list <- append(OdataIn_list, d_line_no)
      OrefIn_list <- append(OrefIn_list, rules_P21[test_cnt,"REF"])
      OdescIn_ref <- append(OdescIn_ref, rules_P21[test_cnt,"Desc"])
      # OclassIn_ref <- append(OclassIn_ref, rules_P21[test_cnt,"Class"]) 
      OdomainIn_ref <- append(OdomainIn_ref,  rules_P21[test_cnt,"DOMAIN"]) # paste( rules_P21[test_cnt,"DOMAIN"], ' ccc'))# 
      OrstestcdIn_ref <- append(OrstestcdIn_ref, rules_P21[test_cnt,cur_testcd])
      OtargetIn_ref <- append(OtargetIn_ref, rules_P21[test_cnt,"Target"]) 
      OunitchkIn_ref<-append(OunitchkIn_ref, ' ')
      OunitchkUNITIN_ref<-append(OunitchkUNITIN_ref,' ')
      OrulesIn_ref <- append(OrulesIn_ref, paste(rules_P21[test_cnt,], collapse=", "))
      OdataIn_ref <- append(OdataIn_ref, paste(data_P21_cur[d_line_no,], collapse=", "))
      
    }  
    
    # update parent 
    eval.parent(substitute( Orules_list <- OrulesIn_list ))
    eval.parent(substitute( Odata_list <- OdataIn_list ))
    eval.parent(substitute( Oref_list <- OrefIn_list ))
    eval.parent(substitute( Odesc_ref <- OdescIn_ref ))
    # eval.parent(substitute( Oclass_ref <- OclassIn_ref ))
    eval.parent(substitute( Odomain_ref <- OdomainIn_ref ))
    eval.parent(substitute( Orstestcd_ref <- OrstestcdIn_ref ))
    eval.parent(substitute( Otarget_ref <- OtargetIn_ref ))
    eval.parent(substitute( Ounitchk_ref <- OunitchkIn_ref ))
    eval.parent(substitute( OunitchkUNIT_ref <- OunitchkUNITIN_ref ))
    
    eval.parent(substitute( Orules_ref <- OrulesIn_ref  ))
    eval.parent(substitute( Odata_ref <- OdataIn_ref ))
    eval.parent(substitute( oth_NotFound <- othIn_NotFound ))  
    
    
    
  }
  
  rule_checking_CD <- function(line, tar, op, min, max)
  {
    
    message(line," ", tar," ", op," ", min," ", max)
    # message(op %in% c("d>=","d<=","d=","d<","d!=","d>") )
    # message(data_P21_cur[line,tar]," <-> ",data_P21_cur[line,min])
    # message(check_val((data_P21_cur[line,sub(" ","",tar)]),min)," <->  ",min)
    
    # varchk for all target
    
    # if ((sub(" ","",tar) %in% colnames(data_P21_cur)) == F){  
    #   return(F)}
    # 
    if (op== "null"){ return( check_Null(tar) ==0)}
    else if (op== "notnull"){ return( check_Null(tar) ==1)}
    
    # else if (op== "varchk") #Domain var names checking
    # {
    #   
    #   v <- as.list(el(strsplit(min, ","))) 
    #   for( i in v)
    #   {
    #     if ((sub(" ","",i) %in% colnames(data_P21)) == F){ 
    #       # message('err --> ',v)
    #       return(F)}
    #   }
    #   return(T)
    # } # Remarked by Dick@26042022 perform only in summary list, suggested by Sadie
    else if (op== "vlistb")
    {
      v <- as.list(el(strsplit(min, ","))) 
      return( any( tar ==v) | tar  == "")
      
    }
    else if (op %in% c("d>=","d<=","d=","d<","d!=","d>") ){
       
      df <- as.numeric(difftime(as.Date(format(as.Date(tar, format = "%d/%m/%Y"), "%Y-%m-%d")), 
                                as.Date(format(as.Date(min, format = "%d/%m/%Y"), "%Y-%m-%d")), unit="days"))
      tmpOp <- sub("d","",op) 
      eval(parse(text = paste0('rtn <- ',df, tmpOp,'0')))
      
      return(rtn)
    }
    else if (op %in% c("classb","class")){ # class for blank accepted
      
      
      if(check_Null(tar) ==0 & op == "classb"){ return(T)}
      if (min %in% class_type_chk == F){ return(F)}
      
      
      if ( grepl(",", min, fixed=TRUE))
      { rtn <- ""
      st_all <- str_split(min, ",")
      for (st in unlist(st_all))
      {
        # message(st,' --> ')
        # message(sub(" ","",st) == "integer")
        if (sub(" ","",st) == "integer")
        {
          if(gsub('[[:digit:]]+', '', tar) != "")
          { 
            rtn = paste(rtn, "F")
          }
          else
          { 
            rtn =  paste(rtn, "T")
          }
        }   
        else if (sub(" ","", class(tar) != sub(" ","",st)))
        { 
          rtn =  paste(rtn , "F")
        }
        else
        { 
          rtn =  paste(rtn, "T")
        }
        
      } 
      if (grepl("T", rtn, fixed=TRUE))
      {
        return(TRUE)
        
      }
      return(FALSE)
      
      }
      else if (min == "integer")
      {
        return(gsub('[[:digit:]]+', '', tar) == "")
      }   
      else if (min == "double")
      {
        return(gsub('[[:digit:]]+', '', tar) %in% c(".",""))
      }   
      else if (min == "string") #string checking A-Z, a-z, ',', '-' , ' '
      {
        return(grepl('^[A-Za-z ]+$', tar)  | tar == "," | tar  == "-")
      }   
      return( class(tar) == min)
      
      
    } 
    else if ( check_Null(tar) ==0) {return(F)}
    # check null for return F for the rest of the test Amended by Dick 19032022
    
    
    else if (op== ">"){ return( check_val(tar,min) ==0)}
    else if (op== "<"){ return( check_val(tar,min) ==1)}
    else if (op== "="){ return( check_val(tar,min) ==2)}
    else if (op== "!="){ 
      
      if (grepl("[[:digit:]]", tar)){ 
         return( check_val(tar,min) ==3)
      
      }else{
        return(tar!=min )
      }
    }
    # else if (op== "v>"){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==0)}
    # else if (op== "v<"){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==1)}
    # else if (op== "v="){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==2)}
    # else if (op== "v!="){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==3)}
    # 
    
    else if (op== "positive"){ return( check_Sign(tar) ==0)}
    else if (op== "negative"){ return( check_Sign(tar) ==1)} 
    
    else if (op== "range") 
    {
      return(tar  >= min & tar <= max)
    }
    # 
    # else if (op== "unitchk")
    # {
    #   if (data_P21_cur[line,tar] == '' |
    #       min == '' |
    #       ( max %in% colnames(data_P21)) == F ){return(F)}
    #   
    #   return(TRUE)
    #   
    #   
    # #  return(data_P21_cur[line,tar] == min)
    # } remark b6 Dick@16052022 checking conduct in separate function
    
    else if (op== "vlist")
    {
      v <- as.list(el(strsplit(min, ","))) 
      return( any( tar == v))
      
    }
    
    
    
    else{
      return(999)
    }  
  } 
  
  
  ###############################################
  # Main functions for rule checking
  rule_checking <- function(line, tar, op, min, max)
  {

    message(line," ", tar," ", op," ", min," ", max)
    # message(op %in% c("d>=","d<=","d=","d<","d!=","d>") )
    # message(data_P21_cur[line,tar]," <-> ",data_P21_cur[line,min])
    # message(check_val((data_P21_cur[line,sub(" ","",tar)]),min)," <->  ",min)

    # varchk for all target
    
    if ((sub(" ","",tar) %in% colnames(data_P21_cur)) == F){  
      return(F)}
    
    if (op== "null"){ return( check_Null(data_P21_cur[line,tar]) ==0)}
    else if (op== "notnull"){ return( check_Null(data_P21_cur[line,tar]) ==1)}
    
    # else if (op== "varchk") #Domain var names checking
    # {
    #   
    #   v <- as.list(el(strsplit(min, ","))) 
    #   for( i in v)
    #   {
    #     if ((sub(" ","",i) %in% colnames(data_P21)) == F){ 
    #       # message('err --> ',v)
    #       return(F)}
    #   }
    #   return(T)
    # } # Remarked by Dick@26042022 perform only in summary list, suggested by Sadie
    else if (op== "vlistb")
    {
      v <- as.list(el(strsplit(min, ","))) 
      return( any( data_P21_cur[line,tar]==v) | data_P21_cur[line,tar] == "")
      
    }
    else if (op %in% c("d>=","d<=","d=","d<","d!=","d>") ){
       
      df <- as.numeric(difftime(as.Date(format(as.Date(data_P21_cur[line,tar], format = "%d/%m/%Y"), "%Y-%m-%d")), 
                                as.Date(format(as.Date(data_P21_cur[line,min], format = "%d/%m/%Y"), "%Y-%m-%d")), unit="days"))
      tmpOp <- sub("d","",op) 
      eval(parse(text = paste0('rtn <- ',df, tmpOp,'0')))
      return(rtn)
    }
    else if (op %in% c("classb","class")){ # class for blank accepted
      
      
      if(check_Null(data_P21_cur[line,tar]) ==0 & op == "classb"){ return(T)}
      if (min %in% class_type_chk == F){ return(F)}
      
      
      if ( grepl(",", min, fixed=TRUE))
      { rtn <- ""
      st_all <- str_split(min, ",")
      for (st in unlist(st_all))
      {
        # message(st,' --> ')
        # message(sub(" ","",st) == "integer")
        if (sub(" ","",st) == "integer")
        {
          if(gsub('[[:digit:]]+', '', data_P21_cur[line,tar]) != "")
          { 
            rtn = paste(rtn, "F")
          }
          else
          { 
            rtn =  paste(rtn, "T")
          }
        }   
        else if (sub(" ","", class(data_P21_cur[line,tar]) != sub(" ","",st)))
        { 
          rtn =  paste(rtn , "F")
        }
        else
        { 
          rtn =  paste(rtn, "T")
        }
        
      } 
      if (grepl("T", rtn, fixed=TRUE))
      {
        return(TRUE)
        
      }
      return(FALSE)
      
      }
      else if (min == "integer")
      {
        return(gsub('[[:digit:]]+', '', data_P21_cur[line,tar]) == "")
      }   
      else if (min == "double")
      {
        return(gsub('[[:digit:]]+', '', data_P21_cur[line,tar]) %in% c(".",""))
      }   
      else if (min == "string") #string checking A-Z, a-z, ',', '-' , ' '
      {
        return(grepl('^[A-Za-z ]+$', data_P21_cur[line,tar])  | data_P21_cur[line,tar] == "," | data_P21_cur[line,tar] == "-")
      }   
      return( class(data_P21_cur[line,tar]) == min)
      
      
    } 
    else if ( check_Null(data_P21_cur[line,tar]) ==0) {return(F)}
    # check null for return F for the rest of the test Amended by Dick 19032022
    
 
    else if (op== ">"){ return( check_val(data_P21_cur[line,tar],min) ==0)}
    else if (op== "<"){ return( check_val(data_P21_cur[line,tar],min) ==1)}
    else if (op== "="){ return( check_val(data_P21_cur[line,tar],min) ==2)}
    else if (op== "!="){ return( check_val(data_P21_cur[line,tar],min) ==3)}
    
    else if (op== "v>"){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==0)}
    else if (op== "v<"){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==1)}
    else if (op== "v="){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==2)}
    else if (op== "v!="){ return( check_val(data_P21_cur[line,tar],data_P21_cur[line,min]) ==3)}
    
    
    else if (op== "positive"){ return( check_Sign(data_P21_cur[line,tar]) ==0)}
    else if (op== "negative"){ return( check_Sign(data_P21_cur[line,tar]) ==1)} 
    
    else if (op== "range") 
    {
      return(data_P21_cur[line,tar] >= min & data_P21_cur[line,tar] <= max)
    }
    # 
    # else if (op== "unitchk")
    # {
    #   if (data_P21_cur[line,tar] == '' |
    #       min == '' |
    #       ( max %in% colnames(data_P21)) == F ){return(F)}
    #   
    #   return(TRUE)
    #   
    #   
    # #  return(data_P21_cur[line,tar] == min)
    # } remark b6 Dick@16052022 checking conduct in separate function
    
    else if (op== "vlist")
    {
      v <- as.list(el(strsplit(min, ","))) 
      return( any( data_P21_cur[line,tar]==v))
      
    }
    
    
    
    else{
      return(999)
    }  
  } 
  
  ###############################################
  #  main program start
  
  # read para
  # 
  args = commandArgs(trailingOnly=TRUE)
  para_file <-  args[1] #  'd:/R/V6_debug/para_CD.csv'   # 
  para_P21 <- read.csv(file = para_file,  header=TRUE, as.is=T,  stringsAsFactors=FALSE)
  data_file <-  para_P21[1,'data_file']
  rule_file  <- para_P21[1,'rule_file']
  domain_file  <- para_P21[1,'domain_file']
  unit_table_file  <- para_P21[1,'unit_table_file']
  vs_convert_file  <- para_P21[1,'vs_convert_file']
  start_chunk <- para_P21[1,'start_chunk']
  chunk_size <- para_P21[1,'chunk_size']
  rpt_dir <- para_P21[1,'rpt_dir']
  MB_conv_file <- para_P21[1,'MB_conv_file']
  DOMAIN_P21 <- list()
  DOMAIN_t <- "" 
  data_file = unlist(strsplit(data_file, ","))
  data_file_no <- length(data_file) #No of data files 
  
  for (i in data_file)
  {
    DOMAIN_t <- substr(gsub("\\..*", "", gsub(".*/.*/", "", i)), 1, 2)
    if ((DOMAIN_t %in% DOMAIN_P21) == F)
    {
      DOMAIN_P21 <- append(DOMAIN_P21, DOMAIN_t)
      assign( paste0( DOMAIN_t, "_data_P21") , read.csv(file = sub(' ', '',i),  header=TRUE, as.is=T,  stringsAsFactors=FALSE))
    } else{
      
      f <- read.csv(file = i,  header=TRUE, as.is=T,  stringsAsFactors=FALSE, )
      eval(parse(text = paste0( DOMAIN_t, "_data_P21 <- rbindlist(list( ",DOMAIN_t, "_data_P21,f), use.names=FALSE)") ))
    }
  } 

  rules_P21 <- read.csv(file = rule_file,  header=TRUE, as.is=T, stringsAsFactors=FALSE) 
  unit_table_P21 <- read.csv(file = unit_table_file,  header=TRUE, as.is=T, stringsAsFactors=FALSE)
  MB_conv_P21 <- read.csv(file = MB_conv_file,  header=TRUE, as.is=T, stringsAsFactors=FALSE)  
  domain_TESTCD_P21 <- read.csv(file = domain_file,  header=TRUE, as.is=T, stringsAsFactors=FALSE)
  if (data_file_no > 1)
  {
    
    for (i in DOMAIN_P21)
    {
        DF <- paste( i, "data_P21", sep = "_")
        # data_P21[is.na(data_P21)] <- ""
        # data_P21 <- data.frame(lapply(data_P21, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F)
        # glimpse(data_P21)
        eval(parse(text = paste0(DF,'[is.na(',DF,')] <- ""'))) 
        eval(parse(text = paste0(DF,' <- data.frame(lapply(',DF,', function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F)')))
        
        eval(parse(text = paste0(DF,' <- ',DF,'[which(str_length(',DF,'[,\"USUBJID\"])>0),]'))) # delete all null USUBJID     
        eval(parse(text = paste0('glimpse(',DF,')')))
        # message(DF)
      
    }
  }else{
    
    data_P21 <- read.csv(file = data_file,  header=TRUE, as.is=T,  stringsAsFactors=FALSE)
    rules_P21 <- read.csv(file = rule_file,  header=TRUE, as.is=T, stringsAsFactors=FALSE) 
    unit_table_P21 <- read.csv(file = unit_table_file,  header=TRUE, as.is=T, stringsAsFactors=FALSE) 
    domain_TESTCD_P21 <- read.csv(file = domain_file,  header=TRUE, as.is=T, stringsAsFactors=FALSE)
    data_P21[is.na(data_P21)] <- ""
    rules_P21[is.na(rules_P21)] <- "" 
    
    data_P21 <- data.frame(lapply(data_P21, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F)
    glimpse(data_P21)
    
  }
  
  rules_P21[is.na(rules_P21)] <- ""
  # if (nrow(data_P21)>1){data_P21<-as.data.frame(sapply(data_P21,trimws),stringsAsFactors = FALSE)}
  # if (nrow(rules_P21)>1){rules_P21<-as.data.frame(sapply(rules_P21,trimws),stringsAsFactors = FALSE)}
  # data_P21 <- data.frame(lapply(data_P21, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F)
  rules_P21 <- data.frame(lapply(rules_P21, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F)
  
  glimpse(rules_P21)
  
  
  
  NotFound_list <- list()
  oth_NotFound  <- list()
  
  
  error_msg <- list()
  
  for (tar in rule_col_chk) #chk rule file col
  {
    
    if (tar %in% colnames(rules_P21) == F )
      
    {
      oth_NotFound <- append(oth_NotFound,tar)
      
    }
    
  }
  
  if (length(oth_NotFound) > 0)
  {
    for (i in oth_NotFound)
    {
      error_msg <- append(error_msg, paste(i, ' column not found in rule file'))
    }
    
  }
  if (data_file_no == 1)
  {
    
    oth_NotFound <- list()
    for (tar in data_col_chk) #chk data file col
    {
      
      if (tar %in% colnames(data_P21) == F )
        
      {
        oth_NotFound <- append(oth_NotFound,tar)
        
      }
      
    }
  }else{
    
    oth_NotFound <- list()
    for (tar in data_col_chk) #chk data file col
    {
      
      for (i in DOMAIN_P21)
      {
        DF <- paste( i, "data_P21", sep = "_")
        # data_P21[is.na(data_P21)] <- ""
        # data_P21 <- data.frame(lapply(data_P21, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F)
        # glimpse(data_P21)
        eval(parse(text = paste0('tmp <- tar %in% colnames(',DF,')'))) 
        
        if (tmp == F )
          
        {
          oth_NotFound <- append(oth_NotFound,tar)
          
        }
      }
      
    }
  }
  
  if (length(oth_NotFound) > 0)
  {
    for (i in oth_NotFound)
    {
      error_msg <- append(error_msg, paste(i, ' column not found in data file'))
    }
  }
  
  
  if (length(error_msg)>0)
  { 
    error_exit( paste(error_msg, collapse="| "))  
      
  }
  
  
  # # for unitchk chk rule unit correctness
  # for(tar in which(rules_P21[,"operator"] == 'unitchk')) 
  # {
  #   if (sub(" ","",rules_P21[tar,"Min"]) %in% conv_unit_options$mass == F )
  #     
  #   {
  #     oth_NotFound <- append(oth_NotFound, 
  #                            paste('For rule line',rules_P21[test_cnt,"No"],' (',rules_P21[test_cnt,"Min"],
  #                                  ') is not found in mass unit list: ' ,
  #                                  massList))
  #     
  #   }
  # } 
  
  
  for ( tar in which(rules_P21[,"operator"] == "varchk"))
  { 
    v <- as.list(el(strsplit(rules_P21[tar,"Min"], ","))) 
    for( i in v)
    {
      if ((sub(" ","",i) %in% colnames(data_P21)) == F){ 
        # message('err --> ',v)
        
        oth_NotFound <- append(NotFound_list, paste('Variable in not found: ',v))
      }
    }
    
  }
  
  
  for ( tar in which(rules_P21[,"operator"] %in% c("classb","class")))
  { 
    
    if (rules_P21[tar,"Min"] %in% class_type_chk == F)
    {
      NotFound_list <- append(NotFound_list, paste('Class in not found AAAAA : ',rules_P21[tar,"Min"]))
    } 
    
  }
  if (data_file_no > 1)
  {
      
      #######################################
      ##  Cross Domain check
    
      
      
      # Main loop
      
      
      test_cnt <- 0
      Output_list <- list()
      Orules_list <- list()
      Odata_list <- list()
      Oref_list <- list()
      Orules_ref <- list()
      Odata_ref <- list()
      Odesc_ref <- list()
      # Oclass_ref <- list()
      Odomain_ref <- list()
      Orstestcd_ref <- list()
      Otarget_ref <- list()
      Ounitchk_ref <- list()
      OunitchkUNIT_ref <- list()
      
      glimpse(Output_list)
      glimpse(Orules_list)
      glimpse(Odata_list)
      glimpse(Oref_list)
      glimpse(Orules_ref)
      glimpse(Odata_ref)
      glimpse(Odesc_ref)
      # glimpse(Oclass_ref)
      glimpse(Odomain_ref)
      glimpse(Orstestcd_ref)
      glimpse(Otarget_ref)
      glimpse(Ounitchk_ref)
      glimpse(OunitchkUNIT_ref)
       
      
      dtl_err_op_out <- append( dtl_err_op_out, 'unitchk')
      chk_rule <- which(rules_P21[,"operator"] %in% c('unitchk','convchk'))
      if(length(chk_rule>0)){
        
            oth_NotFound <- append(oth_NotFound, paste0('For the unitchk/ convchk not yet implemneted for rules ',
                                                        paste(chk_rule, collapse=', ' )))
         
      }
      
      for (test_cnt in 1:nrow(rules_P21))
      {
        
        test_domain <- rules_P21[test_cnt,"DOMAIN"] 
        target_domain<- rules_P21[test_cnt,"TDOMAIN"] 
        
           
        eval(parse(text = paste0("chk_col <- rules_P21[test_cnt,\"Target\"] %in% colnames(",test_domain, "_data_P21)")))
        if  (chk_col  == F ) 
        {  
          err_msg <- paste(rules_P21[test_cnt,"Target"],' column not found for rules ',test_cnt,', please check for the source files!!')
          
          error_exit(err_msg) 
        }
         
        if (!(rules_P21[test_cnt,"operator"] %in% c(">=","<=","=","<","!=",">"))){
            
          eval(parse(text = paste0("chk_col <- rules_P21[test_cnt,\"Min\"] %in% colnames(",target_domain, "_data_P21)")))
          if  (chk_col  == F ) 
          {  
            err_msg <- paste(rules_P21[test_cnt,"Min"],' column not found in rules ',test_cnt,',file, please check for the source files!!')
            
            error_exit(err_msg) 
          }
        }
         
        
        if ((!(rules_P21[test_cnt,"operator"] %in% c(">=","<=","=","<","!=",">"))) &
            (rules_P21[test_cnt,"Max"] != '')){

          eval(parse(text = paste0("chk_col <- rules_P21[test_cnt,\"Max\"] %in% colnames(",target_domain, "_data_P21)")))
          if  (chk_col  == F )
          {
            err_msg <- paste(rules_P21[test_cnt,"Max"],' column not found in rules ',test_cnt,', file, please check for the source files!!')

            error_exit(err_msg)
          }
        }
      }
      
      # error_exit('aaa')
      # for(tar in which(rules_P21[,"operator"] == 'unitchk')) 
      # { 
      #   
      #   run_unitchk(cur_domain, rules_P21[tar,cur_testcd], tar, Orules_list, Odata_list, Oref_list,
      #               Odesc_ref, #Oclass_ref, 
      #               Odomain_ref, Orstestcd_ref, Otarget_ref, Ounitchk_ref, OunitchkUNIT_ref,
      #               Orules_ref, Odata_ref, oth_NotFound)
      # } 
      # 
      # # for convchk chk rule unit convertion
      # for(tar in which(rules_P21[,"operator"] == 'convchk')) 
      # { 
      #   
      #   run_convchk(cur_domain, rules_P21[tar,cur_testcd], tar, Orules_list, Odata_list, Oref_list,
      #               Odesc_ref, #Oclass_ref, 
      #               Odomain_ref, Orstestcd_ref, Otarget_ref, Ounitchk_ref, OunitchkUNIT_ref,
      #               Orules_ref, Odata_ref, oth_NotFound)
      # } 
      # 
      
      for (i in DOMAIN_P21)
      {  
        
        eval(parse(text = paste0(i,'_UID_P21 <- unique(',i,'_data_P21[,"USUBJID"])')))
        
        eval(parse(text = paste0('UID_cnt <- length(unique(',i,'_data_P21[,"USUBJID"]))')))
        
        max_batch <- ceiling(UID_cnt/chunk_size) 
        seq_batch <- seq(1, UID_cnt, by = chunk_size)
        
        eval(parse(text = paste0(i,'_chunk_csv <- list()')))
        eval(parse(text = paste0('glimpse(',i,'_chunk_csv)')))
        
        NotFound_list <- list() 
        tmp_chunk_csv  <- list()
        
        if (length(seq_batch) <= 1) {
          tmp_chunk_csv <- append(tmp_chunk_csv, list(list(1,UID_cnt)))
        } else{
          for (i in 1:length(seq_batch)){
            
            if (i < length(seq_batch)){
              c <- list(seq_batch[i],seq_batch[i+1]-1)
            } else {
              c <- list(seq_batch[i],UID_cnt)
            }
            tmp_chunk_csv <- append(tmp_chunk_csv, list(c))
          }
        }
        eval(parse(text = paste0(i,'_chunk_csv <- tmp_chunk_csv')))

      # eval(parse(text = paste0(i,'_data_P21_cur <- ',i,'_data_P21[which(',i,'_data_P21[,"USUBJID"] %in% UID_P21_cur),]')))
        
        # eval(parse(text = paste0(i,'_data_P21_cur <- ',i,'_data_P21')))
        # cur_testcd <- domain_TESTCD_P21[which(domain_TESTCD_P21[,'DOMAIN']== i),'TESTCD']
        
        
        # eval(parse(text = paste0('tmp <- cur_testcd %in% colnames(',i,'_data_P21_cur)')))
        
        # message(i)
        # message(tmp)
        # message(cur_testcd)
        # message('test......')
        
        # if ((tmp  == F  &
        #      cur_testcd != 'TESTCD')|
        #     cur_testcd %in% colnames(rules_P21)  == F) 
        # {  
        #   err_msg <- paste(cur_testcd,' column not found in data/ rules file, please check for the source files!!')
        #   
        #   error_exit(err_msg)  
        # } 
        #  
        
        # 
        # if (length(cur_testcd) == 0)
        # {  
        #   error_exit('Test name not found!!')
        # }
        # 
        # if (cur_testcd == 'TESTCD')
        # {
        #   eval(parse(text = paste0(i,'_data_P21_cur$TESTCD <- character(nrow(',i,'_data_P21_cur))')))
        #   
        # }
      }
      
      
      
      
        csv_file_r_name <- paste0(rpt_dir,"IDDO_Res_P21CrossCheck_",rules_P21[test_cnt,"DOMAIN"],"_",
                                  gsub(", ", "_",toString( DOMAIN_P21)),"_",
                                  substr(gsub("\\..*", "", gsub(".*/.*/", "", rule_file)), 1, 3),"_",
                                  #"_batch_",as.numeric(cur_chunk),"_",
                                  #chunk_csv[[cur_chunk]][1], "_to_", chunk_csv[[cur_chunk]][2],
                                  "_",format(Sys.time(),"%d%m%Y_%H%M%S"),".xlsx")
        
        csv_file_l_name <- paste0(rpt_dir,"IDDO_Log_P21CrossCheck","_",
                                  gsub(", ", "_",toString( DOMAIN_P21)),"_",
                                  substr(gsub("\\..*", "", gsub(".*/.*/", "", rule_file)), 1, 3),"_",
                                  format(Sys.time(),"%d%m%Y_%H%M%S"),".txt")
        UID_log_cnt <- 0
         # Single condition testing
        for(test_cnt in which((rules_P21[,"REF"]=="" &
                                # grepl("T_",rules_P21[,"REF"])) & 
                              # rules_P21[,"TDOMAIN"] != rules_P21[,"DOMAIN"] &  
                              rules_P21[,"operator"] %in% dtl_err_op_out == F )))# loop for data files
        { 
          
            test_domain <- rules_P21[test_cnt,"DOMAIN"] 
            target_domain<- rules_P21[test_cnt,"TDOMAIN"] 
          
            eval(parse(text = paste0('chunk_csv <- ',test_domain,'_chunk_csv ')))
                 
            for (cur_chunk in start_chunk:length(chunk_csv)) {
                
              eval(parse(text = paste0('UID_P21_cur <- ',test_domain,'_UID_P21[as.numeric(chunk_csv[[cur_chunk]][1]):as.numeric(chunk_csv[[cur_chunk]][2])]')))
              message(UID_P21_cur)
              
              # message(data_P21_cur)
              # message('test......')
              # 
              # cur_testcd <- domain_TESTCD_P21[which(domain_TESTCD_P21[,'DOMAIN']== test_domain),'TESTCD']
              
              
              ###########################################
              
              
               for (ref_UID in UID_P21_cur) # loop for data files
               { 
                # d_line_no <- 3
                # perform checking
                # message(rules_P21[r_line_no,])
                # message(paste(data_P21_cur[d_line_no,], collapse=", "))
                # message('Working on loop 1.............',data_P21_cur[r_line_no,"USUBJID"],
                #         ', ',data_P21_cur[r_line_no,"RSSEQ"])
                
                message(ref_UID)
                 
                message('pre-ref...AAA') 
                
                eval(parse(text = paste0(test_domain,'_data_P21_cur <- ',test_domain,'_data_P21[which(',test_domain,'_data_P21[,"USUBJID"] == ref_UID),]')))
                eval(parse(text = paste0(target_domain,'_data_P21_cur <- ',target_domain,'_data_P21[which(',target_domain,'_data_P21[,"USUBJID"] == ref_UID),]')))
                
                run_checking_CD(ref_UID, test_domain,target_domain, test_cnt, rules_P21[test_cnt,"operator"],
                                    Orules_list, Odata_list, Oref_list,
                                    Odesc_ref, #Oclass_ref,
                                    Odomain_ref,Orstestcd_ref, Otarget_ref, Ounitchk_ref,OunitchkUNIT_ref,
                                    Orules_ref, Odata_ref, oth_NotFound)
                
               
                # save result in list
            } 
            
          } 
         }
        
        # Multiple condition testing######################
        for(ref_cnt in unique(rules_P21[ which(( grepl("T_",rules_P21[,"REF"])) & 
                               # rules_P21[,"TDOMAIN"] != rules_P21[,"DOMAIN"] &  
                               rules_P21[,"operator"] %in% dtl_err_op_out == F ),"REF"]))# loop for data files
        {
          
          
          
           
          ref_data_list <- list()
          collect_data_list <- list() 
          
          test_domain <-  unique(rules_P21[which(rules_P21[,'REF']==ref_cnt),"DOMAIN"] )
          
          if (length(test_domain)>1)
          { 
            error_exit( paste0('More that one domain found for the same Ref code', ref_cnt))  
            
          }
          eval(parse(text = paste0('chunk_csv <- ',test_domain,'_chunk_csv ')))
          
          for (cur_chunk in start_chunk:length(chunk_csv)) 
          {
            
            eval(parse(text = paste0('UID_P21_cur <- ',test_domain,'_UID_P21[as.numeric(chunk_csv[[cur_chunk]][1]):as.numeric(chunk_csv[[cur_chunk]][2])]')))
            
            message(UID_P21_cur)
            message('..........')
            
            for (ref_UID in UID_P21_cur) # loop for data files
            {
            # UID_P21_cur <- UID_P21_cur[which(str_length(UID_P21_cur)>0)]
            
            
            # collect_data_list <-UID_P21_cur
            # eval(parse(text = paste0('collect_data_list <- ',test_domain,'_data_P21[which(',test_domain,'_data_P21[,"USUBJID"] %in% UID_P21_cur),]')))
            # eval(parse(text = paste0('collect_data_list <- ',test_domain,'_data_P21_cur[which(',test_domain,'_data_P21[,"USUBJID"] %in% UID_P21_cur),"USUBJID"]')))
            
            # eval(parse(text = paste0("collect_data_list<- unique(",target_domain, "_data_P21_cur[,\"USUBJID\"])") ))
            
            # eval(parse(text = paste0("collect_data_list<- which(",test_domain, "_data_P21_cur[,\"USUBJID\"]==UID_P21_cur)") ))
             
            collect_data_list <- list()
            eval(parse(text = paste0('collect_data_list <-  which(',test_domain,'_data_P21[,"USUBJID"] == ref_UID)')))
            # UID_P21_cur [,]
            # eval(parse(text = paste0('collect_data_list <-  which(',test_domain,'_data_P21_cur )')))
            # df[!(is.na(df$USUBJID) | df$start_pc==""), ]
             # SA_data_P21_cur[which(is.na(SA_data_P21_cur$USUBJID)|SA_data_P21_cur$USUBJID ==''),] 
            ref_data_list <- list()  
            
            # SA_data_P21_cur[which(str_length(SA_data_P21_cur$USUBJID)==0),] <- ''
            # for (i in UID_P21_cur){
            #   message(i)
            #   # message(which(str_length(SA_data_P21_cur$USUBJID)>0))
            #   message('..........')
            # }
            for(test_cnt in which(rules_P21[,"REF"]==ref_cnt)) # loop for data files
            {   
                test_domain <- rules_P21[test_cnt,"DOMAIN"] 
                target_domain<- rules_P21[test_cnt,"TDOMAIN"] 
                
                    
                #  collect_data_list <- which(SA_data_P21_cur[,"USUBJID"] %in% UID_P21_cur)
                for(d_line_no in collect_data_list ) # loop for data files
                {   
                  eval(parse(text = paste0(test_domain,'_data_P21_cur <- ',test_domain,'_data_P21[which(',test_domain,'_data_P21[,"USUBJID"] == ref_UID),]')))
                  eval(parse(text = paste0(target_domain,'_data_P21_cur <- ',target_domain,'_data_P21[which(',target_domain,'_data_P21[,"USUBJID"] == ref_UID),]')))
                  
                  # eval(parse(text = paste0("ref_UID<- ",test_domain, "_data_P21_cur[d_line_no,\"USUBJID\"]") ))
                  eval(parse(text = paste0("target <- ",test_domain, "_data_P21[d_line_no,\"",rules_P21[test_cnt,"Target"],"\"]") ))
                  
                  # message(ref_UID)
                  if (rules_P21[test_cnt,"operator"] %in% c(">=","<=","=","<","!=",">") ){
                    tmin <- rules_P21[test_cnt,"Min"]
                    tmax <- rules_P21[test_cnt,"Max"]
                    
                  }else{
                    
                    eval(parse(text = paste0("tmin<- ",target_domain, "_data_P21[d_line_no,\"",rules_P21[test_cnt,"Min"],"\"]") ))
                    eval(parse(text = paste0("tmax<- ",target_domain, "_data_P21[d_line_no,\"",rules_P21[test_cnt,"Max"],"\"]") ))
                    
                  } 
                  if (rule_checking_CD(d_line_no, target, rules_P21[test_cnt,"operator"], tmin,tmax))
                  { 
                    
                     ref_data_list <- append(ref_data_list, d_line_no)
                    
                  } 
                }
                
                collect_data_list <- ref_data_list
                ref_data_list <- list()
            }
            
            
            for(test_cnt in which(rules_P21[,"REF"]== sub("T_", "C_", ref_cnt))) # loop for data files
            {   
              
              test_domain <- rules_P21[test_cnt,"DOMAIN"] 
              target_domain<- rules_P21[test_cnt,"TDOMAIN"] 
              message('In - ref ..............')
              message(length(collect_data_list))
              # message('Working on loop 2.............',data_P21_cur[r_line_no,"USUBJID"],
              #         ', ',data_P21_cur[r_line_no,"RSSEQ"])
              # perform checking 
              message(collect_data_list)
              if (length(collect_data_list)>0){ 
                run_checking_CD(ref_UID, test_domain,target_domain, test_cnt, rules_P21[test_cnt,"operator"],
                               Orules_list, Odata_list, Oref_list,
                               Odesc_ref, #Oclass_ref,
                               Odomain_ref,Orstestcd_ref, Otarget_ref, Ounitchk_ref,OunitchkUNIT_ref,
                               Orules_ref, Odata_ref, oth_NotFound)
              # save result in list
              }
            }
            }
          }
        }
      
        ###############################################################
      # ref_target <- "T_1"
       # cur_chunk
    
      #######################################
  }else{
    
    #####################
    
    max_batch <- ceiling(nrow(data_P21)/chunk_size) 
    seq_batch <- seq(1, nrow(data_P21), by = chunk_size)
    
    chunk_csv <- list()
    glimpse(chunk_csv)
    
    NotFound_list <- list() 
    
    if (length(seq_batch) <= 1) {
      chunk_csv <- append(chunk_csv, list(list(1,nrow(data_P21))))
    } else{
      for (i in 1:length(seq_batch)){
        
        if (i < length(seq_batch)){
          c <- list(seq_batch[i],seq_batch[i+1]-1)
        } else {
          c <- list(seq_batch[i],nrow(data_P21))
        }
        chunk_csv <- append(chunk_csv, list(c))
      }
    }
    
    ########################################
    
    # main loop for chunk
    
    csv_file_l_name <- paste0(rpt_dir,"IDDO_Log_P21C","_",
                              substr(gsub("\\..*", "", gsub(".*/.*/", "", data_file)), 1, 3),"_",
                              substr(gsub("\\..*", "", gsub(".*/.*/", "", rule_file)), 1, 3),"_",
                              format(Sys.time(),"%d%m%Y_%H%M%S"),".txt")
    
    
    # varchk for all target
    for(tar in unique(rules_P21[,"Target"])){
      
      tar <- sub(" ","",tar)
      if (( tar %in% colnames(data_P21)) == F)
      { 
        if (tar != "")
        {
          oth_NotFound <- append(oth_NotFound, paste('For the input rules file ',tar,' variable not found in data file'))
        }
      } 
    }
    
    for (cur_chunk in start_chunk:length(chunk_csv)) {
    
    csv_file_r_name <- paste0(rpt_dir,"IDDO_Res_P21C_",
                              substr(gsub("\\..*", "", gsub(".*/.*/", "", data_file)), 1, 3),"_",
                              substr(gsub("\\..*", "", gsub(".*/.*/", "", rule_file)), 1, 3),"_",
                              "_batch_",as.numeric(cur_chunk),"_",
                              chunk_csv[[cur_chunk]][1], "_to_", chunk_csv[[cur_chunk]][2],
                              "_",format(Sys.time(),"%d%m%Y_%H%M%S"),".xlsx")
     
    
    data_P21_cur <- data_P21[as.numeric(chunk_csv[[cur_chunk]][1]):as.numeric(chunk_csv[[cur_chunk]][2]),]
    
    
    logFile = file(csv_file_l_name, open = "a")
    write(paste0("Processing batch: ",as.numeric(cur_chunk), " record #: ",
                 chunk_csv[[cur_chunk]][1], " to ", chunk_csv[[cur_chunk]][2],
                 " at: ", format(Sys.time(),"%d-%m-%Y %H:%M:%S")), file=logFile, append=TRUE)
    close(logFile)
   
    
    
    # Chk Domain existed
    for ( test_domain in unique(data_P21_cur[,"DOMAIN"])){
      
      if(length(which(data_P21_cur[,"DOMAIN"]==test_domain))==0){
         
        NotFound_list <- append(NotFound_list, paste('Domain not found: ',test_domain))
      } 
    }
     
    # Main loop
    
    
    test_cnt <- 0
    Output_list <- list()
    Orules_list <- list()
    Odata_list <- list()
    Oref_list <- list()
    Orules_ref <- list()
    Odata_ref <- list()
    Odesc_ref <- list()
    # Oclass_ref <- list()
    Odomain_ref <- list()
    Orstestcd_ref <- list()
    Otarget_ref <- list()
    Ounitchk_ref <- list()
    OunitchkUNIT_ref <- list()
    
    glimpse(Output_list)
    glimpse(Orules_list)
    glimpse(Odata_list)
    glimpse(Oref_list)
    glimpse(Orules_ref)
    glimpse(Odata_ref)
    glimpse(Odesc_ref)
    # glimpse(Oclass_ref)
    glimpse(Odomain_ref)
    glimpse(Orstestcd_ref)
    glimpse(Otarget_ref)
    glimpse(Ounitchk_ref)
    glimpse(OunitchkUNIT_ref)
    
    
    
    
    NotFound_list <- list() 
    
    # Single condition testing with NULL test name
    # Multiple condition testing
    for(cur_domain in unique(rules_P21[,"DOMAIN"])){
    # ref_domain <- "T_1"
        
       
       cur_testcd <- domain_TESTCD_P21[which(domain_TESTCD_P21[,'DOMAIN']== cur_domain),'TESTCD']
       
       if (  (cur_testcd %in% colnames(data_P21)  == F  &
              cur_testcd != 'TESTCD')|
              cur_testcd %in% colnames(rules_P21)  == F) 
       {  
         err_msg <- paste(cur_testcd,' column not found in data/ rules file, please check for the source files!!')
          
         error_exit(err_msg)  
       } 
       
       if (length(cur_testcd) == 0)
       {  
         error_exit('Test name not found!!')
       }
      
      if (cur_testcd == 'TESTCD')
      {
        data_P21_cur$TESTCD <- character(nrow(data_P21_cur))
        
      }
       
       
       # for unitchk chk rule unit correctness
       for(tar in which(rules_P21[,"operator"] == 'unitchk')) 
       { 
         
         run_unitchk(cur_domain, rules_P21[tar,cur_testcd], tar, Orules_list, Odata_list, Oref_list,
                     Odesc_ref, #Oclass_ref, 
                     Odomain_ref, Orstestcd_ref, Otarget_ref, Ounitchk_ref, OunitchkUNIT_ref,
                     Orules_ref, Odata_ref, oth_NotFound)
       } 
       
       # for convchk chk rule unit convertion
       for(tar in which(rules_P21[,"operator"] == 'convchk')) 
       { 
         
         run_convchk(cur_domain, rules_P21[tar,cur_testcd], tar, Orules_list, Odata_list, Oref_list,
                     Odesc_ref, #Oclass_ref, 
                     Odomain_ref, Orstestcd_ref, Otarget_ref, Ounitchk_ref, OunitchkUNIT_ref,
                     Orules_ref, Odata_ref, oth_NotFound)
       } 
       
       
       
       
      for(ref_domain in unique(rules_P21[which(rules_P21[,cur_testcd] == "" &
                                               rules_P21[,"DOMAIN"] == cur_domain),"DOMAIN"])) # loop for REF Target files
      {  
        # Collect data for the same domain
        ref_data_list <- list()
         
        for(d_line_no in which(data_P21_cur[,"DOMAIN"]==ref_domain)) # loop for data files
        {   
              ref_data_list <- append(ref_data_list, d_line_no) 
        } 
        # Perform single checking  
        for(test_cnt in which(rules_P21[,"DOMAIN"]==ref_domain  &  
                              rules_P21[,"REF"]=="" & 
                              rules_P21[,"operator"] %in% dtl_err_op_out == F &
                              rules_P21[,cur_testcd] == "")) # loop for data files
        {  
          for(d_line_no in ref_data_list ) # loop for data files
          {
            
            # message('Working on loop 2.............',data_P21_cur[r_line_no,"USUBJID"],
                    # ', ',data_P21_cur[r_line_no,"RSSEQ"])
            # perform checking
            
            run_checking(d_line_no, test_cnt, rules_P21[test_cnt,"operator"],
                         Orules_list, Odata_list, Oref_list,
                         Odesc_ref, #Oclass_ref,
                         Odomain_ref,Orstestcd_ref, Otarget_ref, Ounitchk_ref,OunitchkUNIT_ref,
                         Orules_ref, Odata_ref, oth_NotFound)
            
            
            # save result in list
          } 
        }
        
        
        # Perform multiple checking 
        # ref_target <- "T_1"
        for(ref_target in unique(rules_P21[which(sub("T_","",rules_P21[,"REF"]) != "" & 
                                                 rules_P21[,"DOMAIN"]==ref_domain  &  
                                                 grepl("T_", rules_P21[,"REF"], fixed=TRUE) &
                                                 rules_P21[,cur_testcd] == ""),"REF" ])) # loop for REF Target files
        {  
          # Collect ref data
          ref_data_list <- list()
          collect_data_list <- which(data_P21_cur[,"DOMAIN"]==ref_domain)
          for(test_cnt in which(rules_P21[,"REF"]==ref_target & 
                                rules_P21[,"DOMAIN"]==ref_domain &  
                                grepl("T_", rules_P21[,"REF"], fixed=TRUE) &
                                rules_P21[,cur_testcd] == "")) # loop for data files
          { 
            for(d_line_no in collect_data_list) # loop for data files
            { 
              # message(rules_P21[test_cnt,])
              # message(data_P21_cur[d_line_no,])
              # message('Line: ', d_line_no+1, ' -> ',tobool(rule_checking(d_line_no, 
              #                                                            rules_P21[test_cnt,"Target"], 
              #                                                            rules_P21[test_cnt,"operator"], 
              #                                                            rules_P21[test_cnt,"Min"], 
              #                                                            rules_P21[test_cnt,"Max"])))
              # 
              
              if (rule_checking(d_line_no, 
                                rules_P21[test_cnt,"Target"], 
                                rules_P21[test_cnt,"operator"], 
                                rules_P21[test_cnt,"Min"], 
                                rules_P21[test_cnt,"Max"])){ 
                
                ref_data_list <- append(ref_data_list, d_line_no)
              }
              
              collect_data_list <- ref_data_list
            }
          }
          
          # Perform checking 
          
          for(test_cnt in which(rules_P21[,"REF"]==sub("T_", "C_", ref_target) & 
                                rules_P21[,"DOMAIN"]==ref_domain &
                                rules_P21[,"operator"] %in% dtl_err_op_out == F &
                                grepl("C_", rules_P21[,"REF"], fixed=TRUE) &
                                rules_P21[,cur_testcd] == "")) # loop for data files
          {  
            for(d_line_no in ref_data_list ) # loop for data files
            {
              
              # message('Working on loop 2.............',data_P21_cur[r_line_no,"USUBJID"],
              #         ', ',data_P21_cur[r_line_no,"RSSEQ"])
              # perform checking
              
              run_checking(d_line_no, test_cnt, rules_P21[test_cnt,"operator"],
                           Orules_list, Odata_list, Oref_list,
                           Odesc_ref, #Oclass_ref,
                           Odomain_ref,Orstestcd_ref, Otarget_ref, Ounitchk_ref,OunitchkUNIT_ref,
                           Orules_ref, Odata_ref, oth_NotFound)
              # save result in list
            } 
          }
        } 
        
      }
       
      for ( test_name in unique(data_P21_cur[which(data_P21_cur[,"DOMAIN"] == cur_domain),cur_testcd])) # loop for rules files
      { 
        # Chk Test code existed
        
        if(length(which(data_P21_cur[,cur_testcd]==test_name))==0){ 
          NotFound_list <- append(NotFound_list, paste('Test Name not found: ',test_name))
        } 
        # Single condition testing
        for(test_cnt in which(rules_P21[,cur_testcd]==test_name & 
                              rules_P21[,"REF"]==""  &
                              rules_P21[,"operator"] %in% dtl_err_op_out == F &
                              rules_P21[,cur_testcd]!="")) # loop for data files
        { 
          for(d_line_no in which(data_P21_cur[,cur_testcd]==test_name )) # loop for data files
          { 
            # d_line_no <- 3
            # perform checking
            # message(rules_P21[r_line_no,])
            # message(paste(data_P21_cur[d_line_no,], collapse=", "))
            # message('Working on loop 1.............',data_P21_cur[r_line_no,"USUBJID"],
            #         ', ',data_P21_cur[r_line_no,"RSSEQ"])
            
            run_checking(d_line_no, test_cnt, rules_P21[test_cnt,"operator"],
                         Orules_list, Odata_list, Oref_list,
                         Odesc_ref, #Oclass_ref,
                         Odomain_ref,Orstestcd_ref, Otarget_ref, Ounitchk_ref,OunitchkUNIT_ref,
                         Orules_ref, Odata_ref, oth_NotFound)
            # save result in list
          } 
        } 
        # Multiple condition testing
        
        # ref_target <- "T_1"
        for(ref_target in unique(rules_P21[which(sub("T_","",rules_P21[,"REF"]) != "" &  
                                 rules_P21[,cur_testcd]!=""),"REF"])) # loop for REF Target files
        {  
          # Collect ref data 
          ref_data_list <- list()
          collect_data_list <- which(data_P21_cur[,cur_testcd]==test_name)
          for(test_cnt in which(rules_P21[,"REF"]==ref_target 
                                )) # loop for data files
          { 
            for(d_line_no in collect_data_list) # loop for data files
            { 
              # message(rules_P21[test_cnt,])
              # message(data_P21_cur[d_line_no,])
              # message('Line: ', d_line_no+1, ' -> ',tobool(rule_checking(d_line_no, 
              #                                                            rules_P21[test_cnt,"Target"], 
              #                                                            rules_P21[test_cnt,"operator"], 
              #                                                            rules_P21[test_cnt,"Min"], 
              #                                                            rules_P21[test_cnt,"Max"])))
              # 
              
              if (rule_checking(d_line_no,  
                                rules_P21[test_cnt,"Target"], 
                                rules_P21[test_cnt,"operator"], 
                                rules_P21[test_cnt,"Min"], 
                                rules_P21[test_cnt,"Max"])){ 
                 
                ref_data_list <- append(ref_data_list, d_line_no)
              }
              
              collect_data_list <- ref_data_list
            }
          }
          
          # Perform checking  
          for(test_cnt in which(rules_P21[,cur_testcd]==test_name & 
                                rules_P21[,cur_testcd]!="" & 
                                rules_P21[,"operator"] %in% dtl_err_op_out == F &
                                rules_P21[,"REF"]==sub("T_", "C_", ref_target))) # loop for data files
          {  
            for(d_line_no in ref_data_list ) # loop for data files
            {
              
              # message('Working on loop 2.............',data_P21_cur[r_line_no,"USUBJID"],
              #         ', ',data_P21_cur[r_line_no,"RSSEQ"])
              # perform checking
              
              run_checking(d_line_no, test_cnt, rules_P21[test_cnt,"operator"],
                           Orules_list, Odata_list, Oref_list,
                           Odesc_ref, #Oclass_ref,
                           Odomain_ref,Orstestcd_ref, Otarget_ref, Ounitchk_ref,OunitchkUNIT_ref,
                           Orules_ref, Odata_ref, oth_NotFound)
              # save result in list
            } 
          }
        }
      }
     
    }
    
    }} # end cur_chunk
    
    # OsheetName = list('Validation Summary', 'Dataset Summary','Issue Summary', 'Details', 'Rules')
    RptHeader <- "ODDI P21 Validation Report "
    Summary <- list()
     
    Summary <- append(Summary, RptHeader)
    Summary <- append(Summary, paste('Rules file: ',rule_file))
    Summary <- append(Summary, paste('Data file: ',data_file))
    Summary <- append(Summary, paste('Generate Date: ',format(Sys.time(), "%d-%m-%Y")))
    Summary <- append(Summary, paste('Software Version: ', pg_version))
    if (data_file_no > 1)
    {
      
      for (i in DOMAIN_P21)
      {  
        
        eval(parse(text = paste0("Summary <- append(Summary, paste('Total data size for ",i," Domain: ',nrow(",i,"_data_P21)))")))
        eval(parse(text = paste0("Summary <- append(Summary, paste('Total size in batch ",i," Domain: ',length(UID_P21_cur)))")))
        
      }
      
    }else{
      
      Summary <- append(Summary, paste('Total data size: ',nrow(data_P21)))
      Summary <- append(Summary, paste('Total data size in batch: ',nrow(data_P21_cur)))
    }
    if (length(Odata_list)==1 & length(oth_NotFound)==0)
      {
        Summary <- append(Summary, paste('No Errors found'))
    }
     
    Summary <- append(Summary, paste(''))
    Summary <- append(Summary, paste(''))
    Summary <- append(Summary, NotFound_list)  
    Summary <- append(Summary, oth_NotFound)  
    
      
    Out_Summary        <- data.frame(No=1:length(Summary))
    Out_Summary$Details <- Summary 
    
   
    df       <- data.frame(No=1:length(Orules_list)) 
     
    if (length(Odata_list)>0){
      df$Domain <-  Odomain_ref
      df$RSTESTCD <- Orstestcd_ref 
      df$Target <- Otarget_ref 
      df$RuleNo<- Orules_list 
      df$DataLine<- Odata_list 
      df$RefNo<- Oref_list 
      df$Desc<- Odesc_ref 
      # df$Class<- Oclass_ref 
      df$STD_Value<- Ounitchk_ref
      df$STD_Unit<- OunitchkUNIT_ref
      if (length(Orules_ref))
      {
        df$RuleRef<- Orules_ref 
        df$DataRef<- Odata_ref 
      }  
      
      df[is.na(df)] <- ' '
      
      details_st <- aggregate(df, by=list((df$Domain %>% unlist()),
                                          (df$RuleNo %>% unlist()),
                                          (df$RSTESTCD %>% unlist())), FUN=length)
      details_st <- details_st[,0:4]
      names(details_st)[1] <- "DOMAIN"
      names(details_st)[2] <- "RuleNo"
      if (data_file_no == 1)
      {
        names(details_st)[3] <- cur_testcd
      }
      # names(details_st)[4] <- "Class"
      names(details_st)[4] <- "Rejected Records"
      
      details_st[length(details_st)+4, ]  <- list("total:","","", sum(details_st[,"Rejected Records" ])) 
      
    
    
    ## Create Sheets 
    
    wb <- createWorkbook() 
    glimpse(wb) 
    addWorksheet(wb, "Validation Summary") 
    writeData(wb, sheet = "Validation Summary", x = Out_Summary, startCol = 1) 
    if (length(Odata_list) >0 ){ 
      addWorksheet(wb, "Dataset Summary") 
      writeData(wb, sheet = "Dataset Summary", x = details_st, startCol = 1) 
      addWorksheet(wb, "Result") 
      writeData(wb, sheet = "Result", x = df, startCol = 1)          
      # addWorksheet(wb, "Details") # remark request by user 22032022
      # writeData(wb, sheet = "Details", x = data_P21, startCol = 1) 
      addWorksheet(wb, "Rules") 
      writeData(wb, sheet = "Rules", x = rules_P21, startCol = 1) 
      
    } 
    
  # 
  #   cl <- makeCluster(10)
  #   dummy <- lapply(1:length(Orules_list), FUN = function(i) writeFormula(wb, "Result",
  #                                                                           x =  makeHyperlinkString(
  #                                                                             sheet = "Rules", row = as.numeric(Orules_list[i])+1, col = 1,
  #                                                                             text = as.numeric(Orules_list[i])
  #                                                                           ), startCol = 5, startRow = i+1))
  # 
  #   dummy <- lapply(1:length(Orules_list), FUN = function(i) writeFormula(wb, "Result",
  #                                                                           x =  makeHyperlinkString(
  #                                                                             sheet = "Details", row = as.numeric(Odata_list[i])+1, col = 1,
  #                                                                             text = as.numeric(Odata_list[i])
  #                                                                           ), startCol = 6, startRow = i+1))
  # 
  # 
  # 
  # 
  #   stopCluster(cl) 
    
    EndWBTime <-  format(Sys.time(), "%d-%m-%Y %H:%M:%S")
    saveWorkbook(wb, csv_file_r_name  , overwrite = TRUE) 
    
    }
    
  message('Start time: ', StartTime) 
  message('End wb time: ', EndWBTime)
  message('End time: ', format(Sys.time(), "%d-%m-%Y %H:%M:%S")) 
