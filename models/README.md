# Elysium Models

# Clone the repo (in all the machines) ----Cloning to be done for every source
```bash
git clone https://gitlab.com/elysium-analytics/sia.git
```

# Switch to ITD-ATT-1.8.0-profile branch

```bash
cd sia
git checkout ITD-ATT-1.8.0-profile
cd ..
```

# To install Python 3.6.5 and all pip dependencies in the spark cluster (in all the machines). ------ do only once 

```bash
sia/models/scripts/install_python.sh
```
- To validate
    - Issue following command to check python version:-

        ```bash
        $ /usr/local/bin/python3.6 
        Python 3.6.5 (default, Jan 21 2019, 11:33:38) 
        [GCC 4.8.5 20150623 (Red Hat 4.8.5-36)] on linux
        Type "help", "copyright", "credits" or "license" for more information.
        ```
    - Issue following command to check pip3 version:-

        ```bash
        $ pip3 --version
        pip 9.0.3 from /usr/local/lib/python3.6/site-packages (python 3.6)
        ```
        
#To run the project (from Master Node)

## Ensure permission is given to the sia folder
```bash
chmod -R 777 sia
(Assuming cloning is done using root)
```

# login to tenant - say sstech
```bash
su sstech
```

# EVENT BASED MODEL ITD BATCH PROCESS

### To configure the config file for the project for msexchange - sstech tenant is considered

```bash
cd sia/models
./scripts/itd_deploy_models.sh demo /tmp info error null email_size,no_of_receivers \#att-models-log xoxb-501492022263-574304627685-xkt9wroHCGig8NQhZyJYinkt 10.10.150.21 8990 prod "{\"is_nu\":14,\"is_ne\":5,\"is_uatone\":15,\"is_uafromne\":4,\"is_nwpu\":16,\"is_nwpp\":11,\"is_nactpu\":10,\"is_nactpp\":17,\"is_nacnpu\":19,\"is_nacnpp\":1}" 10.10.110.44:6667,10.10.110.45:6667,10.10.110.180:6667 indexing /tmp/checkpoint msexchange  null null <KAFKAGROUPID> BATCH
cd ../..
```
* Note: BATCH or STREAM
* For BATCH process, use source name as msexchange and pass last parameter in above script as BATCH
* For STREAM process, use source name as msexchange and pass last parameter in above script as STREAM

### Initialize hive tables (One time after every deployment)

```bash
hive -f sia/models/scripts/hive/hive.hql
```



### To Train : MsExchange 

```bash
sia/models/scripts/cron/itd_event_anomaly_train.sh
```

### To Score : Batch job for event scoring  - msexchange (run for different dates as required) and cron run if required

```bash
sia/models/scripts/cron/itd_event_anomaly_batch_score.sh 2019 10 1
```

OR

```bash
sia/models/scripts/cron/itd_event_anomaly_batch_score.sh CRON
```


# EVENT BASED MODEL ITD STREAM PROCESS

### To Train: MsExchange 

```bash
sia/models/scripts/cron/itd_event_anomaly_train.sh
```
* Note: config.py should have msexchange as data source and script argument should also have msexchange and stream as argument

### To Score : MsExchange 

```bash
sia/models/scripts/cron/itd_event_anomaly_stream_score.sh 
```

# PROCEDURE FOR PROFILER BASED MODELS 

## for "MsExchange" source

### To configure the config file for MsExchange source under sstech tenant is considered as following
```bash
cd sia/models
./scripts/msexchange_profile_deploy_models.sh sstech /tmp/sstech info error null null \#att-models-log xoxb-501492022263-574304627685-xkt9wroHCGig8NQhZyJYinkt 10.10.110.180 8085 prod "{\"is_nu\":14,\"is_ne\":5,\"is_uatone\":15,\"is_uafromne\":4,\"is_nwpu\":16,\"is_nwpp\":11,\"is_nactpu\":10,\"is_nactpp\":17,\"is_nacnpu\":19,\"is_nacnpp\":1}" 10.10.110.44:6667,10.10.110.49:6667,10.10.110.180:6667 indexing2 /tmp/checkpoint2 msexchange null no_of_emails,ext_sndrs,tot_email_size,avg_email_size r2kafka1 emailmetric
cd ../..
```

### Initialize (create) hive tables (for the first time deployement in any server )
```bash
hive -f sia/models/scripts/hive/hive.hql
```

### To Train : MsExchange profiler model
```bash
sia/models/scripts/cron/msexchange_profile_anomaly_train.sh
```

### To Score : MsExchange profiler model
```bash
sia/models/scripts/cron/msexchange_profile_anomaly_batch_score.sh
```

## for "Windowsnxlog" source

### To configure the config file for windowsnxlog source under sstech tenant is considered as following
```bash
cd sia/models
./scripts/windowsnxlog_profile_deploy_models.sh sstech /tmp/sstech info error null null \#att-models-log xoxb-501492022263-574304627685-xkt9wroHCGig8NQhZyJYinkt 10.10.110.180 8085 prod "{\"is_nu\":14,\"is_ne\":5,\"is_uatone\":15,\"is_uafromne\":4,\"is_nwpu\":16,\"is_nwpp\":11,\"is_nactpu\":10,\"is_nactpp\":17,\"is_nacnpu\":19,\"is_nacnpp\":1}" 10.10.110.44:6667,10.10.110.49:6667,10.10.110.180:6667 indexing2 /tmp/checkpoint2 windowsnxlog null failedlogin_count,fileactivity_count,logins_count,other_scripts_count,pri_count,ps_count r2kafka1 windowsfeatures
cd ../..
```

### Initialize (create) hive tables (for the first time deployement in any server )
```bash
hive -f sia/models/scripts/hive/hive.hql
```

### To Train :  Windowsnxlog profiler model
```bash
sia/models/scripts/cron/windowsnxlog_profile_anomaly_train.sh
```

### To Score : Windowsnxlog profiler model
```bash
sia/models/scripts/cron/windowsnxlog_profile_anomaly_batch_score.sh
```



