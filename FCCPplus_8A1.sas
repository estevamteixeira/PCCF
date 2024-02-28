/********************************************************************************************************/
/*  FCCP+ Version 8A			                                                                        */
/*																										*/
/*  Routine SAS pour le codage géographique automatisé à partir de codes postaux au moyen du Fichier de */
/*  conversion des codes postaux (FCCP) et du Fichier de conversion pondéré (FCP).                      */
/*                                                                                                      */
/*  Lecture des codes postaux entrants en provenance d’un fichier d’entrée précisé par l’utilisateur et */
/*  création d’un fichier de sortie géocodé ainsi que d’un fichier de « problèmes » comprenant les      */
/*  enregistrements pouvant constituer un problème. Les diagnostics sont également inclus en tant que   */
/*  fichier PDF de sortie avec un résumé des résultats de géocodage et plusieurs autres variables de    */
/*  diagnostic pour faciliter l’évaluation de l’exactitude du géocodage.                                */
/*                                                                                                      */
/*  La version 8 du FCCP+ comprend des options de géocodage des codes postaux résidentiels et           */
/*  institutionnels. Les codes postaux résidentiels sont géocodés à l’aide d’un fichier de conversion   */
/*  pondéré attribuant les codes postaux en double en fonction de la répartition sous-jacente de la     */
/*  population. Le géocodage institutionnel sert à coder les secteurs institutionnel et de bureaux      */
/*  (hôpitaux, entreprises, etc.) et à apparier les codes postaux ruraux à l’emplacement du bureau de   */
/*  poste rural au lieu d’utiliser une attribution pondérée selon la population.                      */
/*                                                                                                      */
/*  Le FCCP+ a été préparé et rédigé par la Division de l’analyse de la santé                           */
/*  de Statistique Canada, R.-H.-C-24A, 100, promenade Tunney’s Pasture, Ottawa (Ontario) K1A 0T6       */
/*  Pour obtenir de l’aide, envoyer un courriel à l’adresse fe-hirddirs@statcan.gc.ca                                                 */
/*                                                                                                      */
/********************************************************************************************************/


/********************************************************************************************************/
/* 1. Établir le chemin d’accès au dossier d’installation                                               */
/*      - Le chemin d’accès au dossier ne doit PAS contenir d’espaces.                   										*/
%let installDir = \\hird1\Saslibe$\PCCFplus_FCCPplus\Processing\Programs\PCCF8A1;


/********************************************************************************************************/
/* 2. Préciser la bibliothèque de données d’entrée et le fichier d’entrée.                              */
/*      - Par défaut, le fichier d’entrée est un fichier .sas7bdat (fichier SAS ordinaire).             */
/*      - Le fichier DOIT contenir les champs suivants :                                                */
/*          ID      - Identificateur unique, champ de caractères, 15 caractères par défaut              */
/*          PCODE   - Code postal,       champ de caractères, 6 caractères, pas d’espaces               */

/* Bibliothèque de données (chemin d’accès au dossier où se trouve l’ensemble de données                 */
libname inData "\\hird1\Saslibe$\PCCFplus_FCCPplus\Processing\Programs\PCCF8A1\sample";

/* Nom du fichier de données (aucune extension de fichier)                                               */
%let inFile = sampledat;


/* Code pour lire les fichiers texte en SAS - enlever les étoiles au commencement de chaque ligne pour exécuter ce code */
*filename sampdat   "&installDir.\Sample\sampledat.txt";  /* les données d’entrée en format texte */
*data inData.&infile.;
*    infile sampdat lrecl=256 stopover pad;
*    input
        @   1       PCODE           $6.             /* Postal Code                                              */
        @   8       ID				$15.;
*run;


/********************************************************************************************************/
/* 3. Préciser la bibliothèque de données de sortie et nom du fichier de sortie 
		(le fichier posant problème est indiqué à l’aide du suffixe « _problem »)        */

/* La  bibliothèque de données de sortie (chemin d’accès au dossier où se trouve les fichiers de sortie)        */
%let outData = \\hird1\Saslibe$\PCCFplus_FCCPplus\Processing\Programs\PCCF8A1\sample;

/*    Ce fichier de sortie remplace les anciens fichiers HLTHOUT et GEOPROB                             */
%let outName = sampledat_out;


/********************************************************************************************************/
/* 4. Préciser l'adresse de sortie et le nom du fichier pour le résumé de sortie (format PDF)           */
/*    Ce fichier de sortie comprend les anciens fichiers HLTHOUT et GEOPROB.                                   */
%let pdfOutput = "\\hird1\Saslibe$\PCCFplus_FCCPplus\Processing\Programs\PCCF8A1\sample\sample_01.pdf";


/********************************************************************************************************/
/* 5. Version pour l’exécution d’un codage résidentiel ou institutionnel (codage résidentiel par défaut)*/
/*      - Indiquer 0 pour un codage résidentiel                                                         */
/*      - Indiquer 1 pour un codage institutionnel                                                                   */
%let codeVersion = 0;



/********************************************************************************************************/
/********************************************************************************************************/
/*                                                                                                      */
/* NORMALEMENT, AUCUN CHANGEMENT N’EST NÉCESSAIRE APRÈS CE POINT                                        */
/* Quelques options spécifiées ci-dessous peuvent être modifiées                                        */
/*      - La valeur du germe aléatoire                                                                  */
/*      - La longueur du champ identificateur unique                                                         */
/*                                                                                                      */
/********************************************************************************************************/
/********************************************************************************************************/











/********************************************************************************************************/
/* Read in program SOURCE files from the installation directory                                         */
libname pccf_in "&installDir.\DATA_SAS\pccf_in"; /* Postal Code and weighting                */
libname geo_in "&installDir.\DATA_SAS\georef_in"; /* Geographic codes & lookup                */

proc datasets library=pccf_in;
	copy out=work;
	copy in=geo_in out=work;
run;


/********************************************************************************************************/
/* Random Seed Value                                                                                    */
/* If the seed value is 0 (default) then computer time is used                                          */
/* Change this value as desired to use the same seed between PCCF+ trials                               */
%let seedVal=0;



/********************************************************************************************************/
/* PCCF+ Release Version                                                                                */
%let Version='8A1';

options VARLENCHK = NOWARN;

/********************************************************************************************************/
data input_data;                                /* Read in data file with postal codes to be geocoded   */
    * Unique identifier variable (ID) can be changed here;
    format ID $15. PCODE $6. FSA $3. LDU $3.;
    set inData.&inFile. (rename=(PCODE=inPCode));
    PCODE = compress(inPCode);
    FSA   = upcase(substr(inPCode,1,3));
    LDU   = upcase(substr(inPCode,4,3));
    if PCODE='' then delete;

    RAND2=RANUNI(&seedVal.);                    /* Generate random number for probabilistic assignment  */

    drop inPCode;
run;
proc sort data=input_data nodupkey;             /* Remove duplicate identifiers                         */
    *by PCODE RAND2;                             /* Sort duplicate postal codes by random seed           */
	by PCODE ID;                             	/* Sort duplicate postal codes by unique ID 			*/
run;
data tmp_pccf_wcf;
    merge   pccf_wc6dups    (in=inDup)
            pccf_wc6point   (in=inPnt rename=(TWT=SumWTS));
    by PCODE;
    if inDup and inPnt;

    length DB $4 CMA $3;
    CMA=SAC;
    if CMA in('996' '997' '998' '999') then CMA='000';  /* Change the SAC MIZ codes to CMA              */

    if DMT='Z' and substr(PCODE,2,1)='0' then DMTDIFF='W';  /* Rural postal codes                       */

    RPF='5';
    DB='';
    
    * Calculate weights;
    if PC6DAWT gt 0  then WT =PC6DAWT/SumWTS;
        else WT=0;
    if first.PCODE=1 then TWT=WT;          
        else TWT+WT;
    if last.PCODE=1  then TWT=1;
run;

data tmp_pccf_wcfhit tmp_pccf_wcfmiss; 
    merge   input_data        (in=inHlth) 
            pccf_wc6point   (in=inPnt /*where=(SUBSTR(PCODE,2,1) ne '0')*/);
    by PCODE;
    if inHlth=1 and inPnt=1         then output tmp_pccf_wcfhit;    /* Found on duplicates file         */
    else if inHlth=1 and inPnt=0    then output tmp_pccf_wcfmiss;   /* Not found on duplicates file     */
run;
data tmp_pccf_hlthdat;      /* Temporary file with pcodes not found on duplicates                       */
    set tmp_pccf_wcfmiss;
    keep ID PCODE;
run;
data tmp_pccf_wcfhit;
    set tmp_pccf_wcfhit;

    length Link_Source $1;
    i = firstobs;
    loop: set tmp_pccf_wcf point = i;
        if RAND2 le TWT then Link_Source='C';
            else do; i+1;
        goto loop;
    end;
    keep ID PCODE PR CD DA CSD SAC CTname Tracted LAT LONG DMT H_DMT
            RPF PCtype nDA nCD nCSD DMTDIFF Link_Source Rep_Pt_Type;
run;


/********************************************************************************************************/
/* Match postal codes that are unique on the PCCF                                                       */
/* Since all PCODEs on pccfuniq occur only once on the PCCF these are matched to unique                 */
/* geography at all levels                                                                              */
data tmp_pccf_pccfuniq;
    if &codeVersion. = 0 then       /* Residential geocoding    */
        set pccf_pccfuniq;
    if &codeVersion. = 1 then       /* Institutional geocoding  */
        set pccf_pccfuniq pccf_rpo; 
    
    CMA=SAC;
    if CMA in('996' '997' '998' '999') then CMA='000';  /* Change the SAC MIZ codes to CMA              */

    if DB='000' then DB='';          /* Missing the Block code (RPF=3)                                   */
    SLI='1';
    nCSD=1;
    nCD=1;
    nDA=1;
run;
proc sort data=tmp_pccf_pccfuniq;
    by PCODE PR CSD DA DB LAT LONG; 
run;                             
data tmp_pccf_hit1 tmp_pccf_miss1;
  if &codeVersion. = 0 then do;      			/* Residential geocoding    */
    merge   tmp_pccf_hlthdat  (in=inHlth)   /* Records not found on duplicate file */    
            tmp_pccf_pccfuniq (in=inPCCF);
	by PCODE;
	end;
  else if &codeVersion. = 1 then do;       		/* Institutional geocoding    */
    merge   input_data  (in=inHlth)   		/* All recs - i.e. those not found on duplicate file + rural postal codes which will be assigned to the coords of the RPO */    
            tmp_pccf_pccfuniq (in=inPCCF);
    by PCODE;
	end;
  if inHlth=1 AND inPCCF=1 and DA ne '0000'     then output tmp_pccf_hit1;            /* Matched to unique PCODE      */
  else if inHlth=1 /*AND inPCCF=0 */then output tmp_pccf_miss1;           			/* Not matched to unique        */
run;
data tmp_pccf_hit1;         /* Records found on unique postal code file, no further processing          */
    set tmp_pccf_hit1;
    length Link_Source $1;
    Link_Source='F';        /* Source of geocoding is from PCCF unique records                          */
		 if Rep_Pt_Type='1' then RPF='1';	/* Block face */
	else if Rep_Pt_Type='2' then RPF='2';	/* DB */
	else if Rep_Pt_Type='3' then RPF='3';	/* DA now - later, DB will be imputed within DA */
run;
data tmp_pccf_pccfdups;
    set pccf_pccfdups;
    
    CMA=SAC;
    if CMA in('996' '997' '998' '999') then CMA='000';  /* Change the SAC MIZ codes to CMA              */
 
    if DB='000' then DB='';      
            /* Randomize records zxy (=LAT LONG) within each PCODE so that successive coding            */
            /* sessions can match to every possibility rather than favouring the first occurences       */
            /* on file and generate random num for sort w/in PCODE                                      */
    RANDOM=put(RANUNI(&seedVal.),9.8); 
run;
proc sort data=tmp_pccf_pccfdups;
    by PCODE RANDOM;
run;
/* Assign sequence number to successive occurences of same postal code                                  */
/* Sequence number is used if the PCODE appears more than once on pccf                                  */
proc sort data=tmp_pccf_miss1; by pcode; run;
data tmp_pccf_miss1;
    set tmp_pccf_miss1;
    by PCODE;
    if first.PCODE=1 then SEQH=1;
    else SEQH+1;
run;
proc sort data=pccf_pointdup; by pcode; run;
data tmp_pccf_hit2 tmp_pccf_miss2;
    merge   tmp_pccf_miss1  (in=inMiss) 
            pccf_pointdup   (in=inPoint);
    by PCODE;

 	if inMiss=1 and inPoint=1 then do;          /* If matched to duplicate PCODE record select the      */
        OFFSET=mod(SEQH,nPCODE);                /* next record which matches starting over when         */
        POINTER=OBSDUP+OFFSET;                  /* all are used                                         */
		output tmp_pccf_hit2;               /* hit2 => records matched to dups file will be         */		
    end;                                        /* uniformly distributed among applicable DA/BLK        */
                                                /* for that PCODE.                                      */
   else  if inMiss=1 and inPoint=0 then output tmp_pccf_miss2; 
run;

data tmp_pccf_miss2;
    set tmp_pccf_miss2;
	if DMT=" " then DMT='9';                    /* DMT missing since not matched to pccf(6)             */
    keep ID PCODE DMT;                          /* no geography assigned yet to pccf_miss2              */
run;
/* For postal codes which match on pccf_pointdup file get full geog from pccf_dups file                 */
data tmp_pccf_hit2;
    set tmp_pccf_hit2;

    length Link_Source $1;
    POINTER2=POINTER;                           /* Renamed for checking                                 */
    do; 
        set tmp_pccf_pccfdups point=pointer2;   /* Merge with specified record on dups                  */
    end;

    Link_Source='D';        /* Source of geocoding is from PCCF duplicate records                       */
		 if Rep_Pt_Type='1' then RPF='1';	/* Block face */
	else if Rep_Pt_Type='2' then RPF='2';	/* DB */
	else if Rep_Pt_Type='3' then RPF='3';	/* DA now - later, DB will be imputed within DA */
run;


/********************************************************************************************************/
/* Imputes full geography from first 5 digits of postal code for pccf_miss2 not matched previously                 */
data tmp_pccf_wc5;
    set pccf_wc5dups;
    length DB $3;
    CMA=SAC;
    if CMA in('996' '997' '998' '999') then CMA='000';  /* Change the SAC MIZ codes to CMA              */
    RPF='6';
    DB='';
run;
data tmp_pccf_wc5;
    merge tmp_pccf_wc5 
		  pccf_wc5point (rename=(TWT=SUMTWT));
    by PCODE5;
    if PC5DAWT gt 0             then WT=PC5DAWT/SUMTWT;		
		else WT=0;
    if first.PCODE5=1           then TWT=WT;
        else TWT+WT;
    if last.PCODE5=1            then TWT=1;
run;

data tmp_pccf_miss2; 
    length PCODE5 $5;
    set tmp_pccf_miss2;
    PCODE5=substr(PCODE,1,5);
    RAND2=RANUNI(&seedVal.);
run;
proc sort data=tmp_pccf_miss2; 
    by PCODE5 RAND2;
run;
data tmp_pccf_wc5hit tmp_pccf_wc5miss; 
    merge   tmp_pccf_miss2  (in=inHlth) 
            pccf_wc5point        (in=inPnt);
    by PCODE5;
    if inHlth=1 and inPnt=1         then output tmp_pccf_wc5hit;
    else if inHlth=1 and inPnt=0    then output tmp_pccf_wc5miss;
run;
data tmp_pccf_wc5hit; 
    set tmp_pccf_wc5hit;
    length Link_Source $1;
    i = firstobs;
    loop: set tmp_pccf_wc5 point = i;
    if RAND2 le TWT then Link_Source='5';   /* Geocoding source is 5-character imputation               */
        else do; 
            i+1;
            goto loop;
        end;
run;
data tmp_pccf_wc5miss;
    set tmp_pccf_wc5miss;
    length FSA $3; 
    DMT='9';
    FSA=substr(PCODE,1,3);
    keep ID PCODE FSA DMT;
run;


/********************************************************************************************************/
/* Imputes full geography from first 4 digits of postal code for pccf_wc5miss not matched previously               */
data tmp_pccf_wc4;
    length PCODE4 $4 DB $3;
    set pccf_wc4dups;
    CMA=SAC;
    if CMA in('996' '997' '998' '999') then CMA='000';  /* Change the SAC MIZ codes to CMA              */
    RPF='6';
    DB ='';
run;
data tmp_pccf_wc4;
    length PCODE4 $4;
    merge tmp_pccf_wc4 
          pccf_wc4point (rename=(TWT=SUMTWT));
    by PCODE4;
    if PC4DAWT gt 0     then WT=PC4DAWT/SUMTWT;
        else WT=0;
    if first.PCODE4=1   then TWT=WT;
        else TWT+WT;
    if last.PCODE4=1    then TWT=1;
run;
data tmp_pccf_wc5miss; 
    length PCODE4 $4;
    set tmp_pccf_wc5miss;
    PCODE4 = substr(PCODE,1,5);
    RAND2  = RANUNI(&seedVal.);
run;
proc sort data=tmp_pccf_wc5miss; 
    by PCODE4 RAND2;
run;
data tmp_pccf_wc4hit tmp_pccf_wc4miss; 
    merge   tmp_pccf_wc5miss (in=inHlth) 
            pccf_wc4point         (in=inPnt);
    by PCODE4;
    if inHlth=1 and inPnt=1         then output tmp_pccf_wc4hit;
    else if inHlth=1 and inPnt=0    then output tmp_pccf_wc4miss;
run;
data tmp_pccf_wc4hit;
    set tmp_pccf_wc4hit;
    length Link_Source $1;
    i = firstobs;
    loop: set tmp_pccf_wc4 point = i;
    if RAND2 le twt then Link_Source='4';   /* Geocoding source is 4-character imputation               */
        else do; 
            i+1;
            goto loop;
        end;
run;
data tmp_pccf_wc4miss;
    set tmp_pccf_wc4miss;
    length PCODE3 $3; 
    keep ID PCODE DMT;
    DMT='9';
    PCODE3=substr(PCODE,1,3);
run;
/* Determine records to either recode (using FSA and below) or to keep (norecode)                       */
data tmp_pccf_recode0 tmp_pccf_norecode;
    length dmtdiff $7;
    set tmp_pccf_hit1 tmp_pccf_hit2 tmp_pccf_wc4miss;
    PCODE3=substr(PCODE,1,3);

    if &codeVersion. = 0 then do;	/* Residential Coding */
        if DMT='9' or DMT in('H' 'J' 'K' 'M' 'R' 'T' 'X')       /* W delivery type not included         */
            or (DMT='Z' and DMTDIFF in('H' 'J' 'K' 'M' 'R' 'T' 'X'))
        then output tmp_pccf_recode0;
        else output tmp_pccf_norecode;
    end;
    if &codeVersion. = 1 then do;	/* Institutional Coding */
        if DMT='9' or DMT in('H' 'J' 'K' 'R' 'T' 'X')           /* W and M delivery type not included         */
            or (DMT='Z' and DMTDIFF in('H' 'J' 'K' 'R' 'T' 'X'))
        then output tmp_pccf_recode0; 
        else output tmp_pccf_norecode;
    end; 
run;


/********************************************************************************************************/
/* Imputes SGC from FSA (first 3 characters) for problem records not matched previously                 */

data tmp_pccf_wc3dups;
    set pccf_wc3dups;
	length CMA $3;
    CMA=SAC;
    if CMA in('996' '997' '998' '999') then CMA='000';  /* Change the SAC MIZ codes to CMA              */
run;
data tmp_pccf_wc3dups;
    merge   tmp_pccf_wc3dups    (in=inudups) 
            		pccf_wc3point   (in=inpt rename=(TWT=SUMTWT));
    by PCODE3;
    if inudups=1;
    if PC3DAWT gt 0  then WT=PC3DAWT/SUMTWT /*WT=DBpop2011/fsapop*/;
                    else WT=0;
    if first.PCODE3=1  then TWT=WT;
                    else TWT+WT;
    if last.PCODE3=1   then TWT=1;
run;
data tmp_pccf_uimpute; 
    set tmp_pccf_recode0;
    RAND3=RANUNI(&seedVal.);        /* Generate random number for probabilistic assignment              */
run;
proc sort data=tmp_pccf_uimpute; 
    by PCODE3 RAND3;
run;
data tmp_pccf_wc3hit tmp_pccf_wc3miss; 
    length RPF $3.;
    merge    tmp_pccf_uimpute(in=inhlth) 
             pccf_wc3point   (in=infpnt);
    by pcode3;

	db="";	/* since PCCF_WCDUPS does not have the DB and we do not want to retain the old DB as it will result in a mismatch, reset DB to missing */

    if inhlth=1 and infpnt=1        then output tmp_pccf_wc3hit;       /* DMT=9                        */
    else if inhlth=1 and infpnt=0   then output tmp_pccf_wc3miss;
run;
data tmp_pccf_wc3hit;
    set tmp_pccf_wc3hit;

    length Link_Source $1.;
    i = firstobs;
    loop: set tmp_pccf_wc3dups point = i;
    if RAND3 lt TWT then do;
        Link_Source='3';            /* Imputed from FSA-DA population weights                           */
        RPF='6';                    /* DA representative point imputed within FSA                       */
    end;
    else do; 
        i+1;
        goto loop;
    end;

if &codeVersion. = 1 then do;
	if substr(PCODE,2,1)= '0' or DMT in('H' 'J' 'K' 'R' 'T' 'X')
        or (DMT='Z' and DMTDIFF in('H' 'J' 'K' 'R' 'T' 'X')) then do;	/* if rural only assign CMA, PR/CMA and CD; if urban retain all */;
            DA='9999';
            DB='';
            CSD='999';
            if Tracted='1' then CTname='9999.99';
        end;
	else do; /*Urban and DMT=ABEGM*/
    		DA='9999';
        	DB='';
            if Tracted='1' then CTname='9999.99';
    end;
end;

if &codeVersion. = 0 then do;
	if substr(PCODE,2,1)= '0' or DMT in('H' 'J' 'K' 'M' 'R' 'T' 'X')
        or (DMT='Z' and DMTDIFF in('H' 'J' 'K' 'M' 'R' 'T' 'X')) then do;	/* if rural only assign CMA, PR/CMA and CD; if urban retain all */;
            DA='9999';
            DB='';
            CSD='999';
            if Tracted='1' then CTname='9999.99';
        end;
	else do; /*Urban and DMT=ABEG*/
    		DA='9999';
        	DB='';
            if Tracted='1' then CTname='9999.99';
    end;
end;
    /* Note: the service areas of above DMTs can be outside the FSA so don't impute full geography     */
    /* Note that DB LAT LONG retained here, however, from FSA input                                     */
run;
data tmp_pccf_wc3miss;
    set tmp_pccf_wc3miss;
    keep ID PCODE PCODE3 DMT H_DMT DMTDIFF RAND3;
run;


/********************************************************************************************************/
/* Imputes SGC from FSA for problem records not matched previously (1&2 characters of FSA)              */
data tmp_pccf_wc2dups;
    set pccf_wc2dups;
	length CMA $3;
    CMA=SAC;
    if CMA in('996' '997' '998' '999') then CMA='000';  /* Change the SAC MIZ codes to CMA              */
run;
data tmp_pccf_wc2dups;
    merge   tmp_pccf_wc2dups    (in=inudups) 
            		pccf_wc2point   (in=inpt rename=(TWT=SUMTWT));
    by PCODE2;
    if inudups=1;
    if PC2DAWT gt 0  then WT=PC2DAWT/SUMTWT;
                    else WT=0;
    if first.PCODE2=1  then TWT=WT;
                    else TWT+WT;
    if last.PCODE2=1   then TWT=1;
run;
data tmp_pccf_recode;
    set tmp_pccf_wc3miss;
    PCODE2=substr(PCODE,1,2);
    keep ID PCODE PCODE2 DMT H_DMT DMTDIFF RAND3;
run;
proc sort data=tmp_pccf_recode;
    by PCODE;
run;
data tmp_pccf_wc2hit tmp_pccf_wc2miss;
    length  RPF $3.;
    merge   tmp_pccf_recode (in=inrecode) 
            pccf_wc2point/*pccf_wc3geog*/  (in=infsa);
    by PCODE2 /*FSA*/;
    if inrecode=1 and infsa=1       then output tmp_pccf_wc2hit;
    else if inrecode=1 and infsa=0  then output tmp_pccf_wc2miss;
run;

data tmp_pccf_wc2hit;
    set tmp_pccf_wc2hit;

    length Link_Source $1;
    i = firstobs;
    loop: set tmp_pccf_wc2dups point = i;
    if RAND3 lt TWT then do;
        Link_Source='2';            /* Imputed from FSA-DA population weights                           */
        RPF='8';                   /* PR and CMA imputed within first 2-characters 	*/
    end;
    else do; 
        i+1;
        goto loop;
    end;

    if &codeVersion. = 1 then do;/*Institutional*/
	if substr(PCODE,2,1)= '0' or DMT in('H' 'J' 'K' 'R' 'T' 'X')
        or (DMT='Z' and DMTDIFF in('H' 'J' 'K' 'R' 'T' 'X')) then do;	/* if rural only assign CMA, PR/CMA and CD; if urban retain all */;
            DA='9999';
            DB='';
			FED='999';
			ER='99';
            CSD='999';
            if Tracted='1' then CTname='9999.99';
        end;
	else do; /*Urban and DMT=ABEGM*/
    		DA='9999';
        	DB='';
			FED='999';
            CSD='999';
            if Tracted='1' then CTname='9999.99';
    end;
end;

if &codeVersion. = 0 then do;/*Residential*/
	if substr(PCODE,2,1)= '0' or DMT in('H' 'J' 'K' 'M' 'R' 'T' 'X')
        or (DMT='Z' and DMTDIFF in('H' 'J' 'K' 'M' 'R' 'T' 'X')) then do;	/* if rural only assign CMA, PR/CMA and CD; if urban retain all */;
            DA='9999';
            DB='';
			FED='999';
			ER='99';
            CSD='999';
            if Tracted='1' then CTname='9999.99';
        end;
	else do; /*Urban and DMT=ABEG*/
    		DA='9999';
        	DB='';
			FED='999';
            CSD='999';
            if Tracted='1' then CTname='9999.99';
    end;
end;
    
    /* Note: the service areas of above DMTs can be outside the FSA so don't impute full geography     */
    /* Note that DB LAT LONG retained here, however, from FSA input                                     */
run;

data tmp_pccf_wc2miss;
    set tmp_pccf_wc2miss;

    length PR $2 Link_Source $1;
    PCODE1=substr(PCODE,1,1);
    if      PCODE1='A' then do; PR='10'; Link_Source='1'; RPF='9';end;
    else if PCODE1='B' then do; PR='12'; Link_Source='1'; RPF='9';end;
    else if PCODE1='C' then do; PR='11'; Link_Source='1'; RPF='9';end;
    else if PCODE1='E' then do; PR='13'; Link_Source='1'; RPF='9';end;
    else if PCODE1='G' then do; PR='24'; Link_Source='1'; RPF='9';end;
    else if PCODE1='H' then do; PR='24'; Link_Source='1'; RPF='9';end;
    else if PCODE1='J' then do; PR='24'; Link_Source='1'; RPF='9';end;
    else if PCODE1='K' then do; PR='35'; Link_Source='1'; RPF='9';end;
    else if PCODE1='L' then do; PR='35'; Link_Source='1'; RPF='9';end;
    else if PCODE1='M' then do; PR='35'; Link_Source='1'; RPF='9';end;
    else if PCODE1='N' then do; PR='35'; Link_Source='1'; RPF='9';end;
    else if PCODE1='P' then do; PR='35'; Link_Source='1'; RPF='9';end;
    else if PCODE1='R' then do; PR='46'; Link_Source='1'; RPF='9';end;
    else if PCODE1='S' then do; PR='47'; Link_Source='1'; RPF='9';end;
    else if PCODE1='T' then do; PR='48'; Link_Source='1'; RPF='9';end;
    else if PCODE1='V' then do; PR='59'; Link_Source='1'; RPF='9';end;
    else if PCODE1='X' then do; PR='61'; Link_Source='1'; RPF='9';end;        /* X is always NWT          */
    else if PCODE1='Y' then do; PR='60'; Link_Source='1'; RPF='9';end;
    else do;                  PR='99'; Link_Source='0'; RPF='9';end;
    DMT='9';
	 *keep ID PCODE PCODE1 DMT H_DMT DMTDIFF;
run;

/* Merge recoded problem files back into main hlth file in 2 parts:                                     */
/*   records with block & those without block                                                           */
data tmp_pccf_hblk tmp_pccf_hxblk;
    length CSD $3. RPF $1. Tracted $1. DMT $1. H_DMT $1. DMTDIFF $1. Rep_pt_Type $1. Pctype $1.;
    retain  ID PCODE PR CD DA CSD CSDname CSDtype SAC CTname FED DB Rep_Pt_Type
            LAT LONG SLI PCtype Comm_Name DMT H_DMT DMTDIFF PO QI Source Link_Source RPF CMA 
            POP_CNTR_RA POP_CNTR_RA_type POP_CNTR_RA_SIZE_CLASS nCD nCSD nDA;
	if &codeVersion. = 0 then       /* Residential geocoding    */
    set 	tmp_pccf_wcfhit
        	tmp_pccf_wc5hit
        	tmp_pccf_wc4hit
        	tmp_pccf_norecode
        	tmp_pccf_wc3hit 
        	tmp_pccf_wc2hit 
        	tmp_pccf_wc2miss;
	if &codeVersion. = 1 then       /* Institutional geocoding - exclude rural WCF records - using RPO instead   */
    set 	tmp_pccf_wc5hit
        	tmp_pccf_wc4hit
        	tmp_pccf_norecode
        	tmp_pccf_wc3hit 
        	tmp_pccf_wc2hit 
        	tmp_pccf_wc2miss;

    if DB='' then output tmp_pccf_hxblk;
             else output tmp_pccf_hblk;
    keep    ID PCODE PR CD DA CSD CSDname CSDtype SAC CTname Tracted FED DB Rep_Pt_Type
            LAT LONG SLI PCtype Comm_Name DMT H_DMT DMTDIFF PO QI Source Link_Source RPF CMA
            POP_CNTR_RA POP_CNTR_RA_type POP_CNTR_RA_SIZE_CLASS nCD nCSD nDA nBLK;
run;


/********************************************************************************************************/
/* Impute blocks for any records with DA but no DB                                                      */
data geo_dablk;
    merge   geo_gaf21   (in=inBLK keep=PR CD DA BLK DBPop2021) 
            geo_dablkpnt(in=inPT keep=PR CD DA DA21pop);
    by PR CD DA;
    if inBLK and inPT;
    if DA21pop gt 0 then WT=DBPop2021/DA21pop;
        else WT=0;
    if first.DA=1 then TWT=WT;
        else twt+wt;
    if last.DA=1  then TWT=1;
run;
data tmp_pccf_hxblk;
    set tmp_pccf_hxblk;
    RAND4=RANUNI(&seedVal.);
run;
proc sort data=tmp_pccf_hxblk;
    by PR CD DA RAND4;
run;
data tmp_pccf_hxblkhit tmp_pccf_hxblkmiss;
    merge   tmp_pccf_hxblk  (in=inhxblk) 
            geo_dablkpnt    (in=inblkpnt);
    by PR CD DA;
    if inhxblk=1 and inblkpnt=1         then output tmp_pccf_hxblkhit;
    else if inhxblk=1 and inblkpnt=0    then output tmp_pccf_hxblkmiss;
run;
data tmp_pccf_hxblkhit;
    set tmp_pccf_hxblkhit;
    i=firstobs;
    loop: 
        set geo_dablk point=i;
        if RAND4 le TWT then DB=BLK;
        else do; i+1;
            goto loop;
        end;
run;

data tmp_pccf_hxblkhit;
    set tmp_pccf_hxblkhit;
    drop nBLK FirstObs DA21pop BLK DBPop2021 WT TWT RAND4;
run;


/********************************************************************************************************/
/* The file is now substantively geocoded. The program now completes a series of checks and codes the   */
/* remainder of the supplementary codes.                                                                */
data tmp_geocoded00;
    retain  ID PCODE PR CD DA DA DB CSD SAC CMA Tracted CTname FED Rep_Pt_Type RPF LAT LONG SLI
            PCtype Comm_Name DMT H_DMT DMTDIFF PO QI Source Link_Source nCD nCSD nDA nBLK;
    set     tmp_pccf_hxblkhit 
            tmp_pccf_hxblkmiss
            tmp_pccf_hblk;
    if PR=' ' then PR='99';
    keep    ID PCODE PR CD DA DA DB CSD SAC CMA Tracted CTname FED Rep_Pt_Type RPF LAT LONG SLI
            PCtype Comm_Name DMT H_DMT DMTDIFF PO QI Source Link_Source nCD nCSD nDA nBLK;
run;
proc sort data=tmp_geocoded00;
    by PCODE;
run;

/********************************************************************************************************/
/* Add air stage delivery variable                                                                      */
data tmp_geocoded01;
    merge   tmp_geocoded00  (in=inHlth)
            cpc_airstage    (in=inAir keep=PCODE);
    by PCODE;
    if inHlth;
    AIRLIFT='';
    if inAir and inHlth then AIRLIFT='*';
run;

/********************************************************************************************************/
/* Determine if the area is primarily institutional                                                     */
proc sort data=cpc_instflg;
    by PCODE;
run;
data tmp_geocoded02;
    merge   tmp_geocoded01  (in=inHlth)
            cpc_instflg     (keep=Pcode InstFlag);
    by PCODE;
    if inHlth;
run;

data tmp_geocoded02;
  set tmp_geocoded02;
  if link_source not in('F' 'D') then InstFlag='';
run;

/********************************************************************************************************/
/* Check if DMT E,G,M are possible residences                                                           */
proc sort data=cpc_egmres;
    by PCODE;
run;
data cpc_egmres;
    set cpc_egmres;
    by PCODE;
    if first.PCODE;
run;
data tmp_geocoded03;
	length tracted $1;
    merge   tmp_geocoded02  (in=inHlth)
            cpc_egmres      (in=inEGM);
    by PCODE;
    if inHlth;
    if inEGM=0 and DMT in('E' 'G' 'M') then resFlag='?';
    if DMT in ('A' 'B') then resFlag=' ';
	if &codeversion.=0 then do;	/* RESIDENTIAL CODING ONLY */
								/* if non-residential set geographic codes to missing, except for CMA/CA, PR */
		if resFlag='-' then do;	/* only retain values for CMA/CA, PR and Tracted (i.e., CT='0000.00' or CT='9999.99')- set all others to missing */
			CD='00';
			CSD='999';
			FED='999';
			DA='9999';
			DB='   ';
		    *BLK='   ';
			LAT=.;
			LONG=.;
			LINK='2';
			if tracted='0' then CT='0000.00';
				else CT='9999.99';
		end;
	end;
run;


/********************************************************************************************************/
/* Adds names and addresses for office & institutional coding                                           */
data tmp_geocoded04;
    merge   tmp_geocoded03  (in=inHlth)
            cpc_nadr        (in=inInst keep=PCODE numAdr);
    by PCODE;
    if inHlth;

    if numAdr ge 9 then numAdr=9;
    if substr(PCODE,2,1)='0' then numAdr=9; /* 9+ adr for rural */
    else if Link_Source='F' then numAdr=1;
    else if numAdr=. and (DMT='K' or (DMT='W' and DMTDIFF='K')) then numAdr=9;
    else if numAdr=. and (DMT in ('B' 'E' 'G' 'M') or (DMT='W' 
                   and DMTDIFF in ('B' 'E' 'G' 'M'))) then numAdr=1;

run;

proc sort data=tmp_geocoded04;
    by PR CD DA DB;
run;

/********************************************************************************************************/
/* Merge back with Geographic Attibute File to get the remainder of the codes                           */
/* - Start at the Dissemination Block level, then move up to the DA, CSD, etc...                        */
data tmp_geocoded04;
    set tmp_geocoded04;
    keep ID PCODE PR CD DA DB Rep_Pt_Type RPF LAT LONG SLI PCtype Comm_Name Airlift InstFlag ResFlag BldgName NumAdr
			DMT H_DMT DMTDIFF PO QI Source Link_Source Tracted nCD nCSD nDA nBLK ;
run;

* Merge with DB-level GAF to get additional variables unique at the DB level;
proc sort data=geo_gaf21;
    by PR CD DA DB;
run;
data tmp_geocoded05;
    merge   tmp_geocoded04 (in=inHlth)
            geo_gaf21 (keep=PR CD DA DB DB_ir2021 PRabbr PRfabbr FEDname ERuid ERdguid ERname CDname CSDname CSDtype CTname 
                CCSuid CCSdguid CCSname DPLuid DPLdguid DPLname DPLtype CMAPuid CMAname CMAtype SACcode SACtype PopCtrRAPuid
                PopCtrRAname PopCtrRAtype PopCtrRAclass CARuid CARname CSIZE CSIZEMIZ 
				/*PR CD DA - these values set to missing for recs with no DBUID because they will not be merged */
                FED CSD DB11uid DA11uid DA06uid DA01uid EA96uid EA91uid EA86uid EA81uid InuitLands DA16uid DB16uid DB16DGUID);
    by PR CD DA DB;
    if inHlth;
run;

/********************************************************************************************************/
/* Add health region and alternate health region codes                                                                */
proc sort data=geo_hrdef;
    by PR CD DA DB;
run;
data tmp_geocoded06;
    merge   tmp_geocoded05  (in=inHlth)
            geo_hrdef       (keep=PR CD DA DB HRuid HRename HRfname AHRuid AHRename AHRfname);
    by PR CD DA DB;
    if inHlth;
run;

/********************************************************************************************************/
/* Adds the income quintiles and immigrant terciles                                                     */
/********************************************************************************************************/
/* Adds the income quintiles and immigrant terciles  */

data tmp_geocoded06;
  set tmp_geocoded06;
  dauid=PR||CD||DA;
run;

proc sort data=tmp_geocoded06;
	by dauid;
run;

proc sort data=geo_SESref;
	by dauid;
run;
data tmp_geocoded07;
    merge   tmp_geocoded06  (in=inHlth)
            geo_SESref      (in=inSES);
	by dauid;
    if inHlth;

    if inSES=0 then do;
		BTIPPE='99999999';
		ATIPPE='99999999';
        QABTIPPE='999';
		QNBTIPPE='999';
        DABTIPPE='999';
		DNBTIPPE='999';
		QAATIPPE='999';
		QNATIPPE='999';
        DAATIPPE='999';
        DNATIPPE='999';
        /*ImmTer='9';*/
		IMPFLG='9';

    end;
run;
/********************************************************************************************************/
/* Set missing values for output variables                                                              */
data final_hlthout;
    set tmp_geocoded07;
	length 	DBuid	$11
			DAuid	$8
			CDuid	$4
			CSDuid	$7
			FEDuid	$5;

    * Date and time the process was run;
    format Date_Run datetime16.;
    date_run = datetime();

    * Set missing values for geographic identifiers;
    if PR='' or PR='00'                                     then PR='99';
    if PR='99'                                              then Link_Source='0';
    if CD=''                                                then CD='00';
    if DA='' or DA='0000'                                   then DA='9999';
    if DB=''                                                then DB='000';
    if CSD=''                                               then CSD='999';
    if SACtype=''                                           then SACtype='9';
    CMA=SACcode;
    if CMA in('996' '997' '998' '999')                      then CMA='000';	/* not CMA; rural MIZ */
		else if CMA in('   ')								then CMA='999';	/* missing CMA */
    Tracted='0';
    if SACtype in ('1' '2')                                 then Tracted='1';
    if SACtype = '9'                                        then Tracted='9';
    if Tracted='1' and CTname='0000.00'                     then CTname='9999.99';	/* CT unknown but applicable, i.e., in tracted CMA/CA */
    if Tracted='1' and CTname=''                            then CTname='9999.99';  /* CT unknown but applicable, i.e., in tracted CMA/CA */
    if CTname='0000000'                                     then CTname='9999.99';  /* CT unknown but applicable, i.e., in tracted CMA/CA */
    if CTname=''                                            then CTname='9999.99';	/* CT unknown but applicable, i.e., in tracted CMA/CA */
	if Tracted='0' 											then CTname='0000.00';	/* Not in Census Tracted area or CMA/CA unknown */
    if LAT=.                                then RPF='9';
    if LAT=.                                then Rep_Pt_Type='9';
    if Rep_Pt_Type=''                       then Rep_Pt_Type='9';
    if DPLuid=''                            then DPLuid=PR||'    ';
    if FED=''                               then FED='999';
    if ERuid=''                             then ERuid=PR||'99';  
	if PR in('60' '61' '62')				then CARuid=PR||'00'; else if CARuid='' then CARuid=PR||'99'; 
    if CCSuid=''                            then CCSuid='999';
    if PopCtrRAPuid=''                      then PopCtrRAPuid=PR||'9999';
    if SACtype=''                           then SACtype='9';
    if SACtype='9'                          then Tracted='9';
    if CMAtype=''                           then CMAtype='9';
    if PopCtrRAtype=''                      then PopCtrRAtype='9';
    if PopCtrRAclass=''                     then PopCtrRAclass='9';
    if QI=''                                then QI='999';
    if Source=''                            then Source='9';
    if DB_ir2021=''                         then DB_ir2021='9';
    if CSIZE=''                             then CSIZE='9';
    if CSIZEMIZ=' '                         then CSIZEMIZ='9';	
    if DMT=''                               then DMT='9';
    if DMT=''                               then H_DMT='9';
    if LINK=''                              then LINK='0';              * This is coded below;
    if Link_Source=''                       then Link_Source='0';
    if SLI=''                               then SLI='9';
    if PCtype=''                            then PCtype='9';
    if PO=''                                then PO='2';		/* as per PCCF documentation */
    if InuitLands=''                        then InuitLands='9';
	if BTIPPE=''							then BTIPPE='99999999';
	if ATIPPE=''							then ATIPPE='99999999';
	if QABTIPPE=''                          then QAIPPE='999';
    if QNBTIPPE=''                          then QNIPPE='999';
    if DABTIPPE=''                          then DAIPPE='999';
    if DNBTIPPE=''                          then DNIPPE='999';
	if QAATIPPE=''							then QAATIPPE='999';
	if QNATIPPE=''							then QNATIPPE='999';
	if DAATIPPE=''							then DAATIPPE='999';
	if DNATIPPE=''							then DNATIPPE='999';
    if IMPFLG=''							then IMPFLG='9';
	if BTIPPE='.'							then BTIPPE='99999999';
	if ATIPPE='.'							then ATIPPE='99999999';
	if QABTIPPE='.'                         then QAIPPE='999';
    if QNBTIPPE='.'                         then QNIPPE='999';
    if DABTIPPE='.'                         then DAIPPE='999';
    if DNBTIPPE='.'                         then DNIPPE='999';
	if QAATIPPE='.'							then QAATIPPE='999';
	if QNATIPPE='.'							then QNATIPPE='999';
	if DAATIPPE='.'							then DAATIPPE='999';
	if DNATIPPE='.'							then DNATIPPE='999';
    if IMPFLG='.'							then IMPFLG='9';
	/*if QAIPPE='.'                           then QAIPPE='9';
    if QNIPPE='.'                           then QNIPPE='9';
    if DAIPPE='.'                           then DAIPPE='99';
    if DNIPPE='.'                           then DNIPPE='99';*/
    /*if IMMTER=''                            then IMMTER='9';
      if IMMTER='.'                           then IMMTER='9';*/
    if EA81uid=' '                          then EA81uid='99999999';
    if EA86uid=' '                          then EA86uid='99999999';
    if EA91uid=' '                          then EA91uid='99999999';
    if EA96uid=' '                          then EA96uid='99999999';
    if DA01uid=' '                          then DA01uid='99009999';
    if DA06uid=' '                          then DA06uid='99009999';
	if DA11uid=' '                          then DA11uid='99009999';
	if DA16uid=' '                          then DA16uid='99009999';
	if DB16uid=' '                          then DB16uid='99009999';
	if DB11uid=' '                          then DB11uid='99009999';
    if PR='35' and HRuid=''                 then HRuid=PR||'99';	/* missing but applicable */
		else if HRuid=''					then HRuid=PR||'  ';	/* not applicable */
    if PR='35' and AHRuid=''                then AHRuid=PR||'99';	/* missing but applicable */
		else if AHRuid=''					then AHRuid=PR||'  ';   /* not applicable */

    /* Create DBuid, DAuid, CDuid, CSDuid and FEDuid */
	DBuid=PR||CD||DA||DB;
	DAuid=PR||CD||DA;
	CDuid=PR||CD;
	CSDuid=PR||CD||CSD;
	FEDuid=PR||FED;

    * Calculate the number of missing at each primary geographic level;
    if PR       ne '99'         then PRok=1;    else PRok=0;    /* PR  coded */
    if CD       ne '00'         then CDok=1;    else CDok=0;    /* CD  coded */
    if CMA      ne ''           then CMAok=1;   else CMAok=0;   /* CMA coded */
    if CSD      ne '999'        then CSDok=1;   else CSDok=0;   /* CSD coded */
    if CTname   ne '9999.99'    then CTok=1;    else CTok=0;    /* CT  coded */
    if DA       ne '9999'       then DAok=1;    else DAok=0;    /* DA  coded */
    if DB       ne '000'         then DBok=1;    else DBok=0;    /* DB  coded */

    CCSum=9;
    if DBok=1 and DAok=1                            then CCSum=5;
    if CSDok=1 and CMAok=1 and DAok=0               then CCSum=4;
    if CDok=1 and CMAok=1 and DAok=0                then CCSum=3;
    if PRok=1 and (CDok=1 or CMAok=1) and DAok=0    then CCSum=2;
    if PRok=1 and CDok=0 and CMAok=0 and DAok=0     then CCSum=1;
    if PRok=0                                       then CCSum=0;
    
    /* Create link type field (LINK) but first initialize variables LTx                     */
    lt0=0; lt1=0; lt2=0; lt3=0; lt4=0; lt5=0; lt6=0; lt7=0; lt9=0;
    if DMT='9'                      then do;
        LINK='0'; 
        lt0=1;
		RPF='9';
    end;
    /* Not full match--although partial matching may have been done                         */
    else if DA='9999' 
        and (DMT in ('H' 'J' 'K' 'M' 'R' 'T' 'X') 
        or (DMT='Z' and DMTDIFF in ('H' 'J' 'K' 'M' 'R' 'T' 'X'))) then do; 
            LINK='1'; 
            lt1=1; 
            RPF='8';
        end;
    else if (DMT='M' or (DMT='Z' and DMTDIFF='M'))  /* Option only for INSTITUTIONAL        */
        and &codeVersion. = 1 
        and Link_Source NE 'C'      then do;
            LINK='4';
            LT1=4;
        end;
    else if DMT='E' or (DMT='Z' and DMTDIFF='E') then do;   
        if ResFlag='-'              then do;
            LINK='2'; 
            lt2=1;        /* non-residential                                      */
        end; 
        else if ResFlag in('+' '@')              then do;                
            LINK='9'; 
            lt9=1;        /* residential-ok                                       */
        end; 
        else if ResFlag in(' ' '?')              then do;  
            LINK='3'; 
            lt3=1;        /* business building-may be non-residential             */
        end; 
    end;
    else if (DMT='G' or DMT='M') 
        or (DMT='Z' and DMTDIFF in('G' 'M')) then do;
        if ResFlag='-'              then do; /* non-residential       */
            LINK='2'; 
            lt2=1; 
        end; 
        else do;    /* commercial or institutional building  */
            LINK='4'; 
            lt4=1; 
        end;               
    end;
    else if DMT='Z' then do;	/* retired postal code with no known DMT problems */
        LINK='5';
        lt5=1;
    end;
	else if nDA gt 1 and Link_Source ='D' then do; /* multiple DA match using PCCF dups - unweighted allocation */
        LINK='6'; 
        lt6=1;
    end;
    else if Link_Source='C' then do; /* multiple match using wcf - weighted allocation */
        LINK='7'; 
        lt7=1;
    end;
    else do; /* PCCF dups where nDA=1 or PCCF Unique (link_source='F')         */
        LINK='9'; 
        lt9=1;
    end;

	if DMT in ('A' 'B' 'E' 'G') or (DMT='Z' and DMTDIFF in ('A' 'B' 'E' 'G' ' ')) then do;
        if Link_Source='F' and Rep_Pt_Type='1'          					then PREC='9'; /* 1 blkface (a-g) */
        else if Link_Source in('F' 'D') and Rep_Pt_Type='2' and nBLK=1   	then PREC='8'; /* 1 blk (a b e g) */
		else if Link_Source in('F' 'D' 'C') and nDA=1						then PREC='7'; /* 1 da (a b e g)  */
		else if Link_Source in('F' 'D' 'C') and nDA>1               		then PREC='6'; /* 2+ da's (abeg)  */
    end;
    if PREC='' then do;
            	 if Link_Source='C'         then PREC='5'; /* 1+ da (dmt h-x) */
            else if Link_Source='5' 		then PREC='4'; /* from wc5 pop wt */
            else if Link_Source='4'         then PREC='3'; /* from wc4 pop wt */
            else if Link_Source='3'         then PREC='2'; /* from wc3 pop wt */
            else if Link_Source in('2' '1') then PREC='1'; /* from from wc2 or wc1(PR) */
            else if Link_Source='0'         then PREC='0'; /* no geog coding  */
            else                            	 PREC='#'; /* logic check     */
    end;

    if Link_Source in ('F' 'D') and CSD='999'    then PREC='1'; /* non-res pcode */

	if PREC='#' then do;
		if DMT in('H' 'J' 'K' 'T' 'W' 'X') or (DMT='Z' and DMTDIFF in('H' 'J' 'K' 'T' 'W' 'X')) then PREC='5';
		else if DMT in('M') or (DMT='Z' and DMTDIFF in('M')) then PREC='2';	/* 'M' indicates delivery to a large volume receivers (PO box), however,
																				the ultimate destination of an 'M' is usually within the 
																				same FSA as the PO Box */
	end;


    if RPF in ('' '9') then do;
        if Link_Source in ('F' 'D')                         then RPF='3';
        if Link_Source = 'C' and Rep_Pt_Type = '2'          then RPF='4';
        if Link_Source = 'C' and Rep_Pt_Type = '3'          then RPF='5';
        if Link_Source in ('C' '4' '5')                     then RPF='6';
        if Link_Source in ('1' '2' '3')                     then RPF='8';
    end;

    Tot=1;
    PCCFplus_Release=&Version.;
    Random_Seed=&SeedVal.;
    
    if &codeVersion. = 1 then geoCodeType="Institutional";
    else geoCodeType="Residential";

run;



/********************************************************************************************************/
/* Create the problem file and add additional variables                                                 */
data final_problem; 
    set final_hlthout;
    if LINK in ('0' '1' '2' '3' '4');
 
    /* DEFINE MESSAGES RELATED TO LINK TYPES                        */
    /*NOTE: MOST SERIOUS PROBLEM (LOWEST LINK TYPE) TAKES PRECEDENCE*/
    length MESSAGE $80.;
    if LINK='0'         then 
         MESSAGE = '0 ERROR: NO MATCH TO PCCF - CHECK PCODE/ADDRESS OR CODE MANUALLY';
    else if LINK = '1'    then 
         MESSAGE = '1 ERROR: LINKED TO PO GEOGRAPHY - CODE MANUALLY IF ADDRESS AVAILABLE';
    else if LINK = '2'    then 
         MESSAGE = '2 WARNING: NON-RESIDENTIAL PCODE - CHECK PCODE/ADDRESS (LEGITIMATE RESIDENCE?)';
    else if LINK = '3'    then 
         MESSAGE = '3 WARNING: BUSINESS BUILDING - CHECK PCODE/ADDRESS (LEGITIMATE RESIDENCE?)';
    else if LINK = '4'    then 
         MESSAGE = '4 WARNING: COMMERCIAL/INSTITUTIONAL - CHECK PCODE/ADDRESS (LEGITIMATE RESIDENCE?)';
    else if LINK = '5'    then 
         MESSAGE = '5 NOTE: RETIRED PCODE - EXPECTED IN ADMINISTRATIVE DATA FILES';
    else if LINK = '6'    then 
         MESSAGE = '6 NOTE: MULTIPLE MATCH DISSEMINATION AREA - USING UNWEIGHTED ALLOCATION';
		 *MESSAGE = '6 NOTE: MULTIPLE MATCH TO CSD-PCCF - DISTRIBUTED AMONG APPLICABLE DB/BLOCK FACE';
    else if LINK = '7'    then 
         MESSAGE = '7 NOTE: WEIGHTED ALLOCATION USING 6-DIGIT WCF';
		 *MESSAGE = '7 NOTE: MULTIPLE MATCH TO CSD-WCF - DISTRIBUTED BY POPULATION WEIGHTS OBSERVED';
    else if LINK = '9'    then 
         MESSAGE = '. NO ERROR, WARNING OR NOTE - NO ACTION REQUIRED';
run;

proc sort data=final_problem;
    by PCODE;
run;
data pccf_lookup;
    length ADR $130;
    set cpc_bldgnam ;                       /* BUILDING NAME/ADDRESS LOOKUP FILE */
    if NADR=.               then NADR=1;    /* TEXT FILE WAS MADE UNIQUE */
    else if NADR GE 9       then NADR=9;    /* 9=> 9+ ADDRESS RANGES */
    ADR=trim(NameAdr)||' '||Comm_Name;
run;
proc sort data=pccf_lookup;
    by PCODE;
run;
data pccf_lookup;
    set pccf_lookup;
    by PCODE;
    if LAST.PCODE=1; /* FIRST RECORD CAN BE THE PO VS STREET ADR RANGES */
run;
data final_problem;
    merge   final_problem   (in=inProb)
            pccf_lookup     (in=inLook keep=PCODE ADR);
    by PCODE;
    if inProb;
run;
proc sort data=final_problem;
    by Link ResFlag PCODE PR CD DA;
run;


/********************************************************************************************************/
/* Summarise the final_hlthout and final_problem files                                                  */
proc format;
    value $fmt_link     '0'     = '0| Error: No match to PCCF'
                        '1'     = '1| Error: Link to PO Geography'
                        '2'     = '2| Warning: Non-residential'
                        '3'     = '3| Warning: Business building'
                        '4'     = '4| Warning: Commercial/Institutional'
                        '5'     = '5| Note: Retired postal code'
                        '6'     = '6| Note: Multi-DA, unweighted allocation'
                        '7'     = '7| Note: WCF, weighted allocation'
                        '9'     = '9| No error, note or warning';
    value $fmt_linksrc  'F'     = 'F| Fully coded, PCCF unique record'
                        'D'     = 'D| Fully coded, PCCF duplicate record'
                        'C'     = 'C| Fully coded, WCF record'
                        '5'     = '5| Fully coded, 5-character imputation'
                        '4'     = '4| Fully coded, 4-character imputation'
                        '3'     = '3| Partially coded, 3-character imputation'
                        '2'     = '2| Partially coded, 2-character imputation'
                        '1'     = '1| Province code from 1-character'
                        '0'     = '0| No geographic codes assigned'
                        'V'     = 'V| Fully matched, V1H or V9G (BC Old)'
						'9'     = '9| Not Applicable / Missing';
    value $fmt_source   '1'     = '1| Automated coding to 2011 Census'
                        '2'     = '2| Geocoded using 2011 Census response'
                        '3'     = '3| Converted from 2006 Census geography'
                        '4'     = '4| Manually geocoded'
                        '9'     = '9| Not Applicable / Missing';
    value $fmt_prec     '0'     = '0| No geographic coding'
                        '1'     = '1| Imputed from 2 (WC2) or 1 character (PR)'
                        '2'     = '2| Imputed from 3 characters (WC3)'
                        '3'     = '3| Imputed from 4 characters (WC4)'
                        '4'     = '4| Imputed from 5 characters (WC5)'
                        '5'     = '5| 1 or more DAs (WC6) (DMT=HtoX)'
                        '6'     = '6| 2 or more DAs (DMT=ABEG)'
                        '7'     = '7| 1 Dissemination Area (DMT=ABEG)'
                        '8'     = '8| 1 Dissemination Block (DMT=ABEG)'
                        '9'     = '9| 1 Block Face (DMT=ABEG)';
    value fmt_nCSD       1      = '1| 1 CSD'
                         2      = '2| 2 CSDs'
                         3      = '3| 3 CSDs'
                         4      = '4| 4 CSDs'
                         5      = '5| 5 CSDs'
                         6      = '6| 6 CSDs'
                         7      = '7| 7 CSDs'
                         8      = '8| 8 CSDs'
                         9      = '9| 9 or more CSDs'
                         .      = 'Missing';
    value $fmt_RepPt    '1'     = '1| Block face'
                        '2'     = '2| Dissemination block'
                        '3'     = '3| Dissemination area'
                        '4'     = '4| Census Subdivision'
                        '9'     = '9| Not applicable / Missing';
    value $fmt_RPF      '1'     = '1| Block-face point (Link-Source=F,D)'
                        '2'     = '2| DB point (Link-Source=F,D)'
                        '3'     = '3| DB point imputed within DA (Link_Source=F,D)'
                        '4'     = '4| DB point imputed within set of possible DAs (Link_Source=C)'
                        '5'     = '5| DA point imputed within set of possible DAs (Link_Source=C)'
                        '6'     = '6| DA point imputed within partial Postal Code (Link_Source=3,4,5)'
                        '8'     = '8| Centroid imputed by from first 1 or 2 characters (Link_Source=2,1)'
                        '9'     = '9| Not applicable / Missing';
    value $fmt_ResF     '+'     = '+| Possible residence'
                        '-'     = '-| Improbable residence'
                        '?'     = '?| DMT=E,G,M, residence undetermined'
                        ' '     = '(blank)| Not in DMT=E,G,M';
    value $fmt_InsF     'E'     = 'E| School or university residence'
                        'H'     = 'H| Hospital'
                        'M'     = 'M| Military bases'
                        'N'     = 'N| Nursing homes'
                        'S'     = 'S| Seniors residences'
                        'R'     = 'R| Religious'
                        'P'     = 'P| Prisons, jails'
						'T'     = 'T| Hotels, motels'
                        'U'     = 'U| Other'
                        ' '     = '(blank)| Not applicable or missing';
    value $fmt_PCtyp    '0'     = '0| Unknown'
                        '1'     = '1| Street address with letter carrier'
                        '2'     = '2| Street address with route service'
                        '3'     = '3| Post Office box'
                        '4'     = '4| Route service'
                        '5'     = '5| General delivery'
                        '9'     = '9| Not applicable / Missing';
    value $fmt_DMT      'A'     = 'A| Letter carrier to street address'
                        'B'     = 'B| Letter carrier to apartment building'
                        'E'     = 'E| Delivery to business building'
                        'G'     = 'G| Delivery to large volume receiver'
                        'H'     = 'H| Delivery via rural route'
                        'J'     = 'J| General delivery'
                        'K'     = 'K| Small PO Box (not community box)'
                        'M'     = 'M| Large volume receiver (Large PO Box)'
                        'T'     = 'T| Suburban route service'
                        'W'     = 'W| Rural postal code'
                        'X'     = 'X| Mobile route'
                        'Z'     = 'Z| Retired postal code'
                        '9'     = '9| Not applicable / Missing' / notsorted;
run;
* Close the listing output and produce a PDF;
ods listing close;
ods pdf (id=analstyle) 
        style=analysis 
        file=&pdfOutput.
        pdftoc=2;

title   "SUMMARY OF AUTOMATED CODING RESULTS USING PCCF+ VERSION &version.";
/*proc freq data=final_hlthout;
    title2 'Output File: Frequency of total file (including problem records)';
    format  link            $fmt_link.
            link_source     $fmt_linksrc.
            DMT			 	$fmt_DMT.
            prec            $fmt_prec.
            nCSD            fmt_nCSD.
            Rep_Pt_Type     $fmt_RepPt.
            RPF             $fmt_RPF.
            PCtype          $fmt_PCtyp.
            ResFlag         $fmt_ResF.
            InstFlag        $fmt_InsF.;
    tables  Link Link_Source DMT Prec nCSD Rep_Pt_Type RPF PCtype 
            ResFlag InstFlag /Missing;
run;
proc freq data=final_problem;
    title2 'Problem File: Frequency of problem records';
    format  link            $fmt_link.
            link_source     $fmt_linksrc.
            prec            $fmt_prec.
            ResFlag         $fmt_ResF.
            InstFlag        $fmt_InsF.
            DMT             $fmt_DMT.
            DMTDIFF         $fmt_DMT.;
    tables Link Link_Source Prec ResFlag InstFlag DMT DMTDIFF /Missing;
run;*/

options missing=0 orientation=portrait;

proc tabulate data=final_hlthout ;
    title2 'Output File: Frequency of total file (including problem records)';
  format link            $fmt_link.
         link_source     $fmt_linksrc.
		 DMT	         $fmt_DMT.
         prec            $fmt_prec.
         nCSD            fmt_nCSD.
         Rep_Pt_Type     $fmt_RepPt.
         RPF             $fmt_RPF.
         PCtype          $fmt_PCtyp.
         ResFlag         $fmt_ResF.
		 InstFlag		 $fmt_InsF.;	

	class link link_source DMT prec nCSD Rep_PT_Type RPF PCtype ResFlag InstFlag	/ 
				preloadfmt order=data missing /*exclusive*/ style=[textalign=center color=red backgroundcolor=yellow];
	table  	link all='Total'
			link_source all='Total' 
			DMT all='Total' 
			prec all='Total' 
			nCSD all='Total' 
			Rep_PT_Type all='Total' 
			RPF all='Total' 
			PCtype all='Total' 
			ResFlag all='Total'
			InstFlag all='Total', 
			N='Freq' PctN='Percent(%)' / printmiss style = header style = [bordercolor=white];

run;

proc tabulate data=final_problem ;
    title2 'Problem File: Frequency of problem records';
  format link            $fmt_link.
         link_source     $fmt_linksrc.
		 prec            $fmt_prec.
         ResFlag         $fmt_ResF.
		 InstFlag		 $fmt_InsF.
		 DMT	         $fmt_DMT.
         DMTDIFF         $fmt_DMT.;	

	class link link_source prec ResFlag InstFlag DMT DMTDIFF	/ 
				preloadfmt order=data missing /*exclusive*/ style=[textalign=center color=red backgroundcolor=yellow];
	table  	link all='Total'
			link_source all='Total' 
			prec all='Total' 
			ResFlag all='Total'
			InstFlag all='Total'
			DMT all='Total' 
			DMTDIFF all='Total', 
			N='Freq' PctN='Percent(%)' / printmiss style = header style = [bordercolor=white];

run;

;
* Close the PDF output and begin listing again;
options missing=.;
ods pdf(id=analstyle) close;
ods listing;


/********************************************************************************************************/
/* Format the final_hlthout and final_problem file and clean up processing variables                    */
data &outName.;
    retain  CODER VERSION ID PCODE PR DAuid DB DB_ir2021 CSDuid CSDname CSDtype
            CMA CMAtype CMAname CTname Tracted SACcode SACtype CCSuid 
            FEDuid FEDname 
            DPLuid DPLtype DPLname
            ERuid ERname CARuid CARname
            PopCtrRAPuid PopCtrRAname PopCtrRAtype PopCtrRAclass CSize CSizeMIZ
            /* Health Region identifiers */
            HRuid HRename HRfname AHRuid AHRename AHRfname
            /* PCCF variables */
            SLI Rep_Pt_type RPF PCtype DMT H_DMT DMTDIFF PO QI Source Lat Long Link_Source Link nCD nCSD PREC 
            Comm_Name AirLift InstFlag Resflag 
            /* Additional variables */
            InuitLands BTIPPE ATIPPE QABTIPPE QNBTIPPE DABTIPPE DNBTIPPE QAATIPPE QNATIPPE DAATIPPE DNATIPPE IMPFLG
            DA11uid DB11uid DA06uid DA01uid EA96uid EA91uid EA86uid EA81uid DA16uid DB16uid  
            ;
    set final_hlthout;
    keep   CODER VERSION ID PCODE PR DAuid DB DB_ir2021 CSDuid CSDname CSDtype
            CMA CMAtype CMAname CTname Tracted SACcode SACtype CCSuid 
            FEDuid FEDname 
            DPLuid DPLtype DPLname
            ERuid ERname CARuid CARname
            PopCtrRAPuid PopCtrRAname PopCtrRAtype PopCtrRAclass CSize CSizeMIZ
            /* Health Region identifiers */
            HRuid HRename HRfname AHRuid AHRename AHRfname
            /* PCCF variables */
            SLI Rep_Pt_type RPF PCtype DMT H_DMT DMTDIFF PO QI Source Lat Long Link_Source Link nCD nCSD PREC 
            Comm_Name AirLift InstFlag Resflag 
            /* Additional variables */
            InuitLands BTIPPE ATIPPE QABTIPPE QNBTIPPE DABTIPPE DNBTIPPE QAATIPPE QNATIPPE DAATIPPE DNATIPPE IMPFLG
            DA11uid DB11uid DA06uid DA01uid EA96uid EA91uid EA86uid EA81uid DA16uid DB16uid
            ;
	VERSION=&VERSION;
	if &CODEVERSION=0 then CODER="R";
	else if &CODEVERSION=1 then CODER="I";
run;
data &outName._problem;
    retain  CODER VERSION ID PCODE MESSAGE DMT DMTDIFF Link_Source 
            Link PR DAuid DB CSDuid CSDtype CMA CTname
            InstFlag ResFlag ADR;
    set final_problem;
    keep    CODER VERSION ID PCODE MESSAGE DMT DMTDIFF Link_Source 
            Link PR DAuid DB CSDuid CSDtype CMA CTname
            InstFlag ResFlag ADR;
	VERSION=&VERSION;
	if &CODEVERSION=0 then CODER="R";
	else if &CODEVERSION=1 then CODER="I";
run;

proc sort data=&outName; by /*id*/ pcode; run;
proc sort data=&outName._problem; by link DMT PCODE CSDuid DAuid ID; run;


/********************************************************************************************/
/* Perform the final cleanup to remove all input and processing files                       */
proc datasets nolist;
    delete cpc: geo: pccf: tmp:;
quit;

/* Optional SAS data export output */
libname sasout "&outData.";

data sasout.&outName.;
set &outName.;
run;

/* Export output files as comma separated */
proc export data= &outName
                  outfile="&outData.\&outName..csv"
                  dbms=csv /* to export to a text file, change to dbms=tab and change the file extension above to .txt */
				  replace; 
run;

proc export data= &outName._problem
                  outfile="&outData.\&outName._PROBLEM.csv" 
                  dbms=csv replace; /* to export to a text file, change to dbms=tab and change file extension above to .txt */
run;

filename outtxt "&outData.\&outName..txt";
filename prbtxt "&outData.\&outName._PROBLEM.txt";

data &outName._txt;
  set &outName;
	CD = substr(DAuid,3,2); 
	CSD = substr(CSDuid,5,3);
	DA = substr(DAuid,5,4); 
	DPL = substr(DPLuid,3,4);
	HR = substr(HRuid,3,2); 
	AHR = substr(AHRuid,3,2);
	FED = substr(FEDuid,3,3);
	ER = substr(ERuid,3,2); 
	CAR = substr(CARuid,3,2);
run;

proc export data= &outName._txt
                  outfile=outtxt
                  dbms=tab 
				  replace; 
run;

data &outName._problem_txt;
  set final_problem;
	CD = substr(DAuid,3,2); 
	CSD = substr(CSDuid,5,3);
	DA = substr(DAuid,5,4); 
	DPL = substr(DPLuid,3,4);
	HR = substr(HRuid,3,2); 
	AHR = substr(AHRuid,3,2);
	FED = substr(FEDuid,3,3);
	ER = substr(ERuid,3,2); 
	CAR = substr(CARuid,3,2);
	star='*';
	la_2=compress(left(lat));
	lo_2=compress(left(long));
	*lat2=substr(la_2,1,2);
	*long2=substr(lo_2,1,2);
	lat2=round(la_2, 1);
	long2=round(lo_2,1);


	*NADRC=PUT(NADR,1.);
	NCDC =PUT(NCD,1.);
	NCSDC=PUT(NCSD,1.);
	DIAG=DMTDIFF||DMT||LINK||SOURCE||NCSDC||NCDC||RPF||PCtype||PREC/*||NADRC*/;
run;


data header;
    file prbtxt /*PRINT HEADER=H*/ lrecl=150;
	PUT		
   		@  1  'ID'
   		@  16 'PCODE'
   		@  23 'PR'
   		@  25 'CD'
   		@  27 'CSD'
   		@  31 'CMA'
   		@  34 'CT'
   		@  42 'DABLK'
   		@  49 'LL'
   		@  54 'HR'
   		@  56 'DPL'
   		@  60 'DIAG'
   		@  68 'BLDG NAME,ADR(CPCOMM:CMA/DPL) :CDNAME        CDTYP'
   		@ 118 'CSDNAME'
   		@ 127 'TYP' /
   		@ 1   ' ';        /* BLANK LINE BEFORE LISTING */
run;

proc sort data=&outName._problem_txt;
by Link Prec Pcode ID;
run;

data &outName._problem_txt;
    set &outName._problem_txt;
	by Link;
    file prbtxt lrecl=150 MOD;
	IF LINK IN ('0' '1' '2' '3' '4' ); /* ERRORS & WARNINGS ONLY */
                                  /* CHANGE TO LE 7 TO SEE NOTES TOO */
	TITLE 'PARTIAL PRINT OF GEOPROB FILE (ERRORS & WARNINGS, BUT NO NOTES)';

	LINE=
  	'------------------------------------------------------------------';
	IF FIRST.LINK=1 THEN
	PUT
 	@ 1 LINE $133./
 	@ 1 MESSAGE  $133./
 	@ 1 LINE $133.;

    put
        @   1       ID				$15.
        @  16       Pcode			$6.
        @  22       ResFlag			$1.
        @  23       PR				$2.
        @  25       CD				$2.
        @  27       CSD				$3.
		@  30       CSDtype			$3.
        @  34       CMA				$3.
        @  37       CTname			$7.
        @  45       DA				$4.
        @  49       DB				$3.
        @  52       InstFlag		$1.
        @  53       LAT2			2.
        @  55       LONG2			4.
        @  59       HR				$2. 
		@  61       DPL				$4. 
        @  65       DMTDIFF			$1. 
        @  66       DMT				$1. 
        @  67       LINK			$1. 
		@  68       Link_Source		$1. 
		@  69       RPF				$1. 
		@  70       PCtype			$1. 
		@  71       Prec			$1. 
		@  72       ADR				$50. 
		@ 122       CSDname 		$8. 
		@ 130       CSDtype			$3. 
		@ 133       star			$1. 
        ;
		RETURN;
run;
