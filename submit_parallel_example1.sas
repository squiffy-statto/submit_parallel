/*******************************************************************************
|
| Program Name:    SUBMIT_PARALLEL_EXAMPLE1.sas
| Program Purpose: Example code to demonstrate wrapping simulation code 
|                  in a macro and then submitting using the auto parallel by
|                  processing tool.
| SAS Version:  9.3
| Created By:   Thomas Drury
| Date:         20-04-18 
*********************************************************************************/;


**********************************************************************************;
*** EXAMPLE 1                                                                  ***;
***                                                                            ***;
*** BASIC SAMPLE SIZE SIMULATION FOR A SHARED RANDOM EFFECTS MODEL 20 SIMS     ***;
*** OF 100 SUBJECTS PER ARM. EACH SIMULATION IS ANALYSED USING MCMC            ***;
*** NOTE: 20 SIMS USED TO ILLUSTRATE IN REALITY MANY MORE WOULD BE NEEDED      ***;
***                                                                            ***;
**********************************************************************************;

**********************************************************************************;
*** STEP 1: CREATE SUBJECT LEVEL DATA FOR 5 SCENARIOS AND 20 SIMULATIONS       ***;
***         STRUCTURED IN A SINGLE DATASET BY SCENARIO SIMULATION THEN SUBJECT ***;
**********************************************************************************;

data simulations;
  
  do scenario = 1 to 5;

  *** TREATMENT MEANS ***;
  mu11 = 0;
  mu12 = 0;
  mu21 = 1;
  mu22 = 2;

  *** SHARED RE VARIABILITY ***;
  select(scenario);
    when (1) theta = 0.5;
    when (2) theta = 1.0;
    when (3) theta = 1.5;
    when (4) theta = 3.0;
    when (5) theta = 6.0;
    otherwise;
  end;

  *** VARIABILITY OF DATA ***;
  sigma = 5;
 
  do simulation = 1 to 20;
  do subject    = 1 to 2*100;

    trt = ifn(subject le 100, 0, 1);
    lp1 = trt*mu11 + (1-trt)*mu12;
    lp2 = trt*mu21 + (1-trt)*mu22;
 
    u  = rand("normal",0,theta);
    y1 = rand("normal", lp1+u, sigma);
    y2 = rand("normal", lp2+u, sigma);
    output;

  end;
  end;
  end;

run;


**********************************************************************************;
*** STEP 2: INCLUDE PROGRAM WITH MACRO TOOL TO MAKE AVAILABLE                  ***;
**********************************************************************************;

filename code1 url "https://raw.githubusercontent.com/squiffy-statto/submit_parallel/master/submit_parallel.sas";
%include code1;

**********************************************************************************;
*** STEP 3: CREATE NORMAL MCMC CODE TO ANALYSE EACH SCENARIO AND SIMULATION    ***;
***         USING A BY STATEMENT THEN WRAP THIS CODE IN A MACRO CALLED         ***;
***         MCMC_MODEL1 (MACRO HAS 2 PARAMETERS FOR CHAIN LENGTH AND THINING)  ***;
**********************************************************************************;


%macro mcmc_model1(nmc=1000,thin=1);

  ods graphics off;
  ods results off;
  ods select none;

proc mcmc data    = simulations 
          nbi     = 5000
          nmc     = &nmc.
          thin    = &thin.
          nthreads = 1
          statistics  = (summary interval)
          diagnostics = (mcse) dic
          monitor = (b11 b12 b21 b22 s1-s4);

  by scenario simulation;

  ods output postsummaries = post_sum;
  ods output postintervals = post_int;

  array y[2] y1 y2;
  array m[2] m1 m2;
  array s[2,2] s1-s4 (1.0  0.5  
                      0.5  1.0);

  array a[2,2] (1.0  0.0  
                0.0  1.0);

  parms b11 b12 b21 b22;
  parms s;

  prior b: ~ normal(0, sd=100);
  prior s  ~ iwish(2, a);

  m1 = trt*b11 + (1-trt)*b12;
  m2 = trt*b21 + (1-trt)*b22;

  model y ~ mvn(m, s);

run;

ods select all;
ods results on;
ods graphics on;

%mend; 


**********************************************************************************;
*** STEP 4: CALL THE SUBMIT_PARALLEL MACRO AND SUPPLY THE INPUT DATASET        ***;
***         SPECIFYING THE LIST OF BY VARIABLES, THE WRAPPER MACRO WITH THE    ***;
***         ANALYSIS CODE AND IF NEEDED ANY PARAMETERS IN THE WRAPPER MACRO    ***;
**********************************************************************************;

%submit_parallel(sessions   = 3
                ,input_data = simulations
                ,byvar_list = %str(scenario, simulation)
                ,macro_name = mcmc_model1
                ,mparm_list = %str(nmc=2000, thin=2)
                );


**********************************************************************************;
*** STEP 5: USE THE RESULTS DATA FROM MCMC AND SUMMARISE OVER THE SCENARIOS    ***;
***         AND SIMULATIONS                                                    ***;
**********************************************************************************;

proc means data = post_sum;
  class scenario parameter;
  var mean;
run;

