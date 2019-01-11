/*******************************************************************************
| Program Name:    SUBMIT_PARALLEL.sas
|
| Program Version: 1.3
| Program Purpose: Creates a tool that allows users to wrap standard by process 
|                  Code into a macro and run in parallel. Calling this tool
|                  automatically splits the starting dataset up and runs
|                  each part in a child session before collating all the results
|                  the idea being no extra parallel code is needed from the user.
| SAS Version:  9.4  
| Created By:   Thomas Drury
| Date:         20-04-18 
|--------------------------------------------------------------------------------
| Input Parameters:
|
| input_data = [REQ] Name of first dataset called by the code in the wrapper macro
| byvar_list = [OPT] Comma seperated list of by variables used in the by processing
| sessions   = [OPT] Number of sessions - this will depend on type of machine 
| macro_name = [REQ] Name of wrapper macro the standard code is in 
| mparm_list = [OPT] List of macro parameters (if any) in the wrapper macro
|
|--------------------------------------------------------------------------------
| Change Log: (OC = Original Code, UP = Update, BF = Bugfix)
|
| V1.1 OC: TD: 20MAY18: Working Concept originally called PARALLEL_PROCESS
| V1.2 UP: TD: 29MAY18: Added ability to accept macros with parameters. 
| V1.3 UP: TD: 16AUG18: #1: Changed macro name to submit_parallel
|                       #2: Log reporting summary of call including: Dataset,
|                           Sessions, Macros and Parameters.
|                       #3: Added a sort by any variables in the BYVARS list
|                       #4: Added warning when more sessions than cores asked for
|                       #5: Added correct handling if less groups possible than 
|                           cores requested. 
|
| NOTE: SEE GITHUB FOR VERSION CHANGES BEYOND THIS.
|
*********************************************************************************/;

**********************************************************************************;
***             START OF TOOL CODE TO INCLUDE AT START OF PROGRAM              ***;
**********************************************************************************;

*** FLUSH OUT ANY PROCEDURE RUNNING ***;
quit;
run;

*** SET UP OPTIONS TEMP LIBRARY OPTIONS ***;
options dlcreatedir;
%sysmstoreclear;
%let workdir = %sysfunc(pathname(work));
%let tempdir = &workdir./_parallel_;
libname templib "&tempdir."; 


*** TELL SAS TO STORE ANY COMPILED MACROS IN PERM LIBRARY LOCATION ***;
options mstored 
        sasmstore = templib;


*** CREATE MACRO TO INVOKE MULTIPLE SESSIONS FOR SEPERATE PROCESSING ***;
%macro submit_parallel(sessions   = 2
                      ,input_data =
                      ,byvar_list =
                      ,macro_name = 
                      ,mparm_list = ) / store source;

  %let toolname = SUBMIT_PARALLEL;

%********************************************************************************;
%*** SECTION 1: PROCESS INPUT MACRO PARAMETERS AND CREATE PARAMETERS NEEDED   ***;
%********************************************************************************;

 
  %*** LOG START TIME FOR PARALLEL MACRO ***;
  %let runstart = %sysfunc(datetime());


  %*** WORK OUT IF DATASET HAS PERM LIBRARY OR IS A TEMP WORK DATASET ***;
  %let lib_levels = %sysfunc(countw(&input_data.,%str(.)));
  %if &lib_levels. = 1 %then %do;
    %let inlib  = WORK;
    %let indata = &input_data.;
  %end;
  %else %if &lib_levels. = 2 %then %do;
    %let inlib  = %scan(&input_data.,1,%str(.));
    %let indata = %scan(&input_data.,2,%str(.));;
  %end;
  %else %do;
    %put ER%upcase(ror:(&toolname.):) Problem with input dataset. Macro will abort.;
    %abort cancel;
  %end;
 

  %*** CREATE BY VARS AND SPACE SEPERATED LIST ***;
  %let byvar_n = %sysfunc(countw(&byvar_list.,%str(,)));
  %let byvarlist=;
  %if &byvar_n. = 0 %then %do;
    %let byvarlist=NONE;
  %end;
  %else %do;
    %do ii = 1 %to &byvar_n.;
      %let byvar&ii. = %scan(&byvar_list.,&ii.,%str(,));
      %let byvarlist = &byvarlist. &&byvar&ii.;
    %end;
  %end;


  %*** CREATE CALL TO MACRO WITH PARAMETERS IF SPECIFIED ***;
  %let macro_call = &macro_name.;
  %if %length(&mparm_list.) ne 0 %then %do;
    %let mparm_n = %sysfunc(countw(&mparm_list.,%str(,)));
    %let mparm_list_tidy =;
    %do ii = 1 %to &mparm_n.;
      %let mparm&ii. = %scan(&mparm_list.,&ii.,%str(,));
      %if &ii. = 1 %then %do; %let mparm_list_tidy = &&mparm&ii. ; %end;
      %else %do; %let mparm_list_tidy = &mparm_list_tidy., &&mparm&ii. ; %end;
    %end;
    %let macro_call = &macro_call.(&mparm_list_tidy.);
  %end;


  %*** WORK OUT TYPE OF GSK SAS EVIRONMENT ***;
  %if %upcase(&sysscp.) = WIN %then %do;
    %let runenv = PCSAS;
  %end;
  %else %if %upcase(&sysscp.) = LIN X64 %then %do;
    %if %symexist(_clientapp) %then %do;
      %if %upcase(&_clientapp.) = 'SAS STUDIO' %then %do;
        %let runenv = SASSTUDIO;
      %end;
      %else %do;
        %put ER%upcase(ror: (&toolname.):) Unable to determine the SAS Environment. Macro will abort.;
        %abort cancel;
      %end;
    %end;
    %else %do;
      %let runenv = LINUXSAS;
    %end;
  %end;
  %else %do;
    %put ER%upcase(ror: (&toolname.):) Unable to determine the SAS Environment. Macro will abort. &=sysscp;
    %abort cancel;
  %end;

  %*** V1.3 UP: TD: 16AUG18: #2 ***;
  %*** WRITE BASIC CALL INFORMATION TO THE LOG ***;
  %put NO%upcase(te:(&toolname.):) ------------------------------------------------------------------;
  %put NO%upcase(te:(&toolname.):) | Parallel Processing Sessions Requested:;
  %put NO%upcase(te:(&toolname.):) ------------------------------------------------------------------;
  %put NO%upcase(te:(&toolname.):) | Dataset      : &input_data.;
  %put NO%upcase(te:(&toolname.):) | By Variables : &byvarlist.;
  %put NO%upcase(te:(&toolname.):) | Sessions     : &sessions.;
  %put NO%upcase(te:(&toolname.):) | Macro Code   : &macro_name..;

  %*** WRITE MACRO PARAMETERS CALL INFORMATION TO THE LOG ***;
  %if %length(&mparm_list.) ne 0 %then %do;
  %put NO%upcase(te:(&toolname.):) | Macro Parms  : ;
  %do ii = 1 %to &mparm_n.; 
    %put NO%upcase(te:(&toolname.):) |  &&mparm&ii.;
  %end;
  %end;
  %else %do;
    %put NO%upcase(te:(&toolname.):) | Macro Parms  : NONE;
  %end;

  %*** REPORT THE START TIME FOR PARALLEL PROCESSING ***;
  %put NO%upcase(te:(&toolname.):) ------------------------------------------------------------------;
  %put NO%upcase(te:(&toolname.):) | Start Time   : %sysfunc(putn(&runstart.,datetime18.));
  %put NO%upcase(te:(&toolname.):) ------------------------------------------------------------------;

  %*** REPORT HOW MANY SESSIONS ASKED FOR COMPARED TO CPUS AVAILABLE ***;
  %if &sessions. gt &sysncpu. %then %do;
    %put WA%upcase(RNING:(&toolname.):) Number of sessions requested (sessions=&sessions.) is more than;
    %put WA%upcase(RNING:(&toolname.):) the numberof CPUs available to SAS (CPUs=&sysncpu.). This may; 
    %put WA%upcase(RNING:(&toolname.):) result in slower performance or the failure to create parallel; 
    %put WA%upcase(RNING:(&toolname.):) sessions which will prevent the selected macro program from;
    %put WA%upcase(RNING:(&toolname.):) running correctly.;
  %end; 


%********************************************************************************;
%*** SECTION 2: PREPARE MULTIPLE TEMPORARY LOCATIONS FOR PARALLEL RUN         ***;
%********************************************************************************;


  *** CREATE COPY OF THE INPUT DATASET ***;
  data templib.&indata.;
    set &inlib..&indata.;
  run;

  *** SORT DATA TO ENSURE ITS IN THE CORRECT ORDER BASED ON BY STATEMENTS ***;
  %if &byvarlist. ne NONE %then %do; 
  proc sort data = templib.&indata.;
    by &byvarlist.;
  run;
  %end;

  *** SPLIT DATASET INTO GROUPS BASED ON LAST BYVAR IF SPECIFIED OR EACH ROW IF NOT ***;
  data _null_; 
    set templib.&indata. end = eof;
    %if &byvarlist. ne NONE %then %do; 
      by &byvarlist.;
      if first.&&byvar&byvar_n. then _total_ + 1;
    %end;
    %else %do;
    _total_ + 1;
    %end;
    if eof then call symput("total",strip(put(_total_,8.)) );
  run;

  *** CREATE GROUPINGS ***; 
  data templib.&indata.;
    set templib.&indata.;
    retain _count_ 0;
    %if &byvarlist. ne NONE %then %do; 
      by &byvarlist.;
      if first.&&byvar&byvar_n. then _count_ + 1;
    %end;
    %else %do;
    _count_ + 1;
    %end;
    
    %if &total. lt &sessions. %then %do;
    %put NO%upcase(te:(&toolname.):) The number of distinct by groups (&total.) is less than the number;
    %put NO%upcase(te:(&toolname.):) of sessions (&sessions.). Therefore only (&total.) sessions will be created.;
    _group_ = _count_ - 1;
    %end;
    %else %do;
    _group_ = floor( (_count_*(&sessions.)) / (&total.+1) );
    %end;
 
  run;

  %*** IF LESS GROUPS THAN SESSIONS UPDATE TOTAL SESSION NUMBER ***;
  %if &total. lt &sessions. %then %do;
    %let total_sessions = %sysfunc(min(&total.,&sessions.));
  %end;
  %else %do;
    %let total_sessions = &sessions.;
  %end;

  *** CREATE TEMP DIRECTORIES IN PARALLEL LOCATION FOR EACH SESSION ***;
  %do ii = 1 %to &total_sessions.;
    %let sess&ii.dir = &tempdir./_sess&ii._;
    libname sess&ii. "&&sess&ii.dir";
  %end;

  *** CREATE A COPY OF THE COMPILED MACRO CATALOG IN EACH SESSION SUBFOLDER ***;
  proc catalog cat = %if &runenv. = PCSAS or &runenv. = LINUXSAS %then %do; work.sasmacr %end;
                     %else %if &runenv. = SASSTUDIO %then %do; work.sasmac1 %end;
                     ;
    %do ii = 1 %to &total_sessions.;
      copy out=sess&ii..sasmacr;
    %end;
  run;
  quit;




%********************************************************************************;
%*** SECTION 3: CREATE CODE TO RUN IN MULTIPLE PARALLEL SESSIONS              ***;
%********************************************************************************;


  %*** CREATE PARALLEL SESSIONS ***;
  %do ii = 1 %to &total_sessions.;
    signon rs&ii. sascmd="!sascmd" signonwait=no connectwait=no cmacvar=status&ii.;
  %end;


  %*** PROCESS CODE IN EACH PARALLEL SESSION ***;
  %do ii = 1 %to &total_sessions.;

     %*** CHECK SIGN ON STATUS IF ONGOING SLEEP UNTIL AN OUTCOME RC IS SUPPLIED ***;
     %if &&status&ii. = 3 %then %do;
       %put NO%upcase(te:(&toolname.):) Waiting for connection to remote session to complete.;
       %do %until (&&status&ii. ne 3); 
          %let rc=%sysfunc(sleep(0.1,1)); 
       %end;
     %end;
     %if &&status&ii. = 0 %then %do;
       %put NO%upcase(te:(&toolname.):) Connection to remote session rs&ii. successful.;     
     %end;
     %else %if &&status&ii. = 2 %then %do;
       %put NO%upcase(te:(&toolname.):) Connection to remote session rs&ii. already established.;     
     %end;
     %else %if &&status&ii. = 1 %then %do;
       %put ER%upcase(ror:(&toolname.):) Connection to remote session rs&ii. failed. All other remote sessions terminated. Macro will abort.;
       signoff _all_;
       %abort cancel;
     %end;


     %*** MAKE MACRO VARIABLES AVAILABLE ON REMOTE SESSIONS  ***;
     %syslput tempdir     = &tempdir.     / remote = rs&ii.;
     %syslput sess&ii.dir = &&sess&ii.dir / remote = rs&ii.;
     %syslput inlib       = &inlib.       / remote = rs&ii.;
     %syslput indata      = &indata.      / remote = rs&ii.;
     %syslput byvarlist   = &byvarlist.   / remote = rs&ii.;
     %syslput ii          = &ii.          / remote = rs&ii.;
     %syslput macro_call  = &macro_call.  / remote = rs&ii.;


     *** CREATE PARALLEL SESSIONS ***;
     rsubmit rs&ii. wait=no cpersist=no; 

       *** SET UP LOCAL SESSION OPTIONS AND LINK TO PARENT LIBNAMES ***;
       libname templib  "&tempdir.";
       libname sess&ii. "&&sess&ii.dir.";
       options mstored sasmstore=sess&ii.;

       *** READ IN SECTION OF DATASET TO PROCESS ***;
       proc sort data = templib.&indata.
                 out  = &indata.;
         by &byvarlist.;
         where _group_ = %eval(&ii.-1);
       run;
       ;

      %&macro_call.;

       *** PUT ALL WORK DATASETS BACK INTO PARENT TEMPLIB ***;
       proc sql noprint;

         *** DATASETS CREATED BY CALLED MACRO ***;
         select distinct memname 
         into :dsetlist separated by ' ' 
         from sashelp.vmember 
         where libname = "WORK" and memtype = "DATA";

         *** RENAME LIST AS MACRO VARIABLE ***;
         select distinct cats(memname,'=',memname,"_SESS&ii.") 
         into :renamelist separated by ' ' 
         from sashelp.vmember 
         where libname = "WORK" and memtype = "DATA";

         *** COPY LIST AS A MACRO VARIABLE ***;
         select cats(memname,"_SESS&ii.")
         into :copylist separated by ' '
         from sashelp.vmember 
         where libname = "WORK" and memtype = "DATA";

       quit;

       *** ADD SUFFIX TO EACH DATASET AND COPY BACK TO TEMPLIB ***;
       proc datasets lib = work nolist;
         change &renamelist.;
         copy out = templib; 
         select &copylist.;
         delete &dsetlist.;
       run;
       quit;
 
     endrsubmit;

  %end;

  waitfor _all_ %do ii = 1 %to &total_sessions.; rs&ii. %end; ;
  signoff _all_;



%********************************************************************************;
%*** SECTION 4: WORK OUT ALL THE DATASETS CREATED AND STACK TOGETHER IN WORK  ***;
%********************************************************************************;

  proc sql noprint;

    *** WORK OUT UNIQUE DATASETS FROM ALL THE SESS DATASETS ***;
    create table dsets_to_stack as
    select distinct substr(memname,1,index(memname,"_SESS")-1) as dset 
      from sashelp.vmember 
      where libname = "TEMPLIB" and 
            memtype = "DATA" and 
            index(upcase(memname),"_SESS") ne 0;

    *** COUNT SESS DATASETS TO STACK TOGETHER ***; 
    select distinct count(dset) into :dsetn
    from dsets_to_stack;

    *** CREATE MACRO VARIABLES FOR EACH UNIQUE DATASET ***;
    select distinct dset into :dset1-:dset%sysfunc(strip(&dsetn.))
    from dsets_to_stack;
 
  quit;

  %*** STACK ALL THE DATASETS AND PUT BACK INTO WORK ***;
  %do ii = 1 %to &dsetn.;
  data &&dset&ii.;
    set templib.&&dset&ii.._sess1-templib.&&dset&ii.._sess&total_sessions. ;
    by &byvarlist.;
  run;
  %end;



%********************************************************************************;
%*** SECTION 5: CLEAN UP ALL THE TEMPORARY DATASETS FOLDERS AND LIBNAMES      ***;
%********************************************************************************;


  %*** DELETE SESSION DATASETS FROM TEMPLIB ***;
  proc datasets lib = templib nolist;
    delete  &input_data.
            %do ii = 1 %to &dsetn.; 
            &&dset&ii.._sess1-&&dset&ii.._sess&total_sessions.
            %end;
    ;
  quit;
  run;


  %*** DELETE DATASETS TO STACK ***;
  proc datasets lib = work nolist;
     delete dsets_to_stack;
  run;
  quit;

  %*** REMOVE MACRO CATALOGS FOR EACH SESSION ***;
  %do ii = 1 %to &total_sessions.;
  proc datasets lib = sess&ii. nolist;
     delete sasmacr / mt = cat;
  run;
  quit;
  %end;

  %*** REMOVE LIBNAMES FOR SESSIONS ***;
  %do ii = 1 %to &total_sessions.;
  libname sess&ii. clear; 
  %end;

  %*** DELETE SUBFOLDERS IN PARALLEL AREA ***;
  %do ii = 1 %to &total_sessions.;
  data _null_;
    filename fname "&tempdir./_sess&ii._";
    rc = fdelete("fname");
    if rc = 0 then do;
      msg = "NO"||"TE: "||upcase("(&toolname.):")||" Successfully deleted temp folder: &tempdir./_sess&ii._";
      put msg;
    end;
    else if rc ne 0 then do;
      msg=sysmsg();
      put msg;
    end;
  run;
  %end;

  %*** REMOVE SASMACRO CATALOG ***;
  proc datasets lib = templib nolist;
     delete sasmacr / mt = cat;
  run;
  quit;



%********************************************************************************;
%*** SECTION 6: REPORT SUCCESS TO THE LOG AND WORK OUT RUN TIME               ***;
%********************************************************************************;

  %*** CALCULATE RUN TIME IN DD:HH:MM:SS ***;
  %let runend  = %sysfunc(datetime());
  %let runtime = %sysfunc(sum(&runend.,-&runstart.));
  %let dd  = %sysfunc(putn(%sysfunc(int(%sysevalf( (&runtime.) / (24*60*60) ) )),z2.));
  %let hh  = %sysfunc(putn(%sysfunc(int(%sysevalf( (&runtime. - &dd.*24*60*60) / (60*60) ) )),z2.));
  %let mm  = %sysfunc(putn(%sysfunc(int(%sysevalf( (&runtime. - &dd.*24*60*60 - &hh.*60*60) / 60 ) )),z2.));
  %let ss  = %sysfunc(putn(%sysfunc(int(%sysevalf( (&runtime. - &dd.*24*60*60 - &hh.*60*60 - &mm.*60) ) )),z2.));

  %put NO%upcase(te:(&toolname.):) ------------------------------------------------------------------;
  %put NO%upcase(te:(&toolname.):) End of Parallel Processing;
  %put NO%upcase(te:(&toolname.):) End Time: %sysfunc(putn(&runend., datetime18.));
  %put NO%upcase(te:(&toolname.):) Processing Time: &dd.:&hh.:&mm.:&ss. (dd:hh:mm:ss).; 
  %put NO%upcase(te:(&toolname.):) ------------------------------------------------------------------;

%mend;

