#!/usr/bin/env bash

filename="config.py"
cp ./scripts/templates/win_profile_config.py.template ./$filename


if [ -z "$1" ]
  then
    echo "Provide tenant id as First Argument"
fi

if [ -z "$2" ]
  then
    echo "Provide absolute directory path for logging as Second Argument"
fi

if [ -z "$3" ]
  then
    echo "Provide log level for project logging as Third Argument"
fi

if [ -z "$4" ]
  then
    echo "Provide log level for pyspark logging as Fourth Argument"
fi

if [ -z "${5}" ]
  then
    echo "Provide  data source as fifth Argument"
	exit 0
fi

if [ -z "${6}" ]
  then
    echo "Provide list of categorical columns for Profile Data as sixth Argument"
	exit 0
fi

if [ -z "${7}" ]
  then
    echo "Provide list of numerical columns for Profile Data as seventh Argument"
	exit 0
fi

if [ -z "${8}" ]
  then
    echo "Provide database to connect to  hdfs as eighth Argument"
	exit 0
fi


if [ -z "${9}" ]
  then
    echo "Provide  table name from where we need to fetch the data as nineth Argument"
	exit 0
fi


if [ -z "${10}" ]
  then
    echo "Provide  table name  where we need to insert the data as tenth Argument"
	exit 0
fi


if [ -z "${11}" ]
  then
    echo "Provide date argument for which you want to run the model as eleventh Argument"
	exit 0
fi

if [ -z "${12}" ]
  then
    echo "Provide a number for how many days you want to run the model as twelth Argument"
	exit 0
fi




tenant_id=$1
log_folder=$(echo $2 | sed 's/\//\\\//g')
log_level=$(echo $3 | tr a-z A-Z)
pyspark_log_level=$(echo $4 | tr a-z A-Z)
data_source=${5}
profile_categorical_column_string=${6}
profile_numerical_column_string=${7}
database=${8}
table_name=${9}
write_table_name=${10}
date_argument=${11}
no_of_days=${12}


echo "Model Deployment for "$tenant_id" - started"
sed -ie "s/<TENANT-NAME>/\"$tenant_id\"/g" $filename
sed -ie "s/<LOG-FOLDER>/\"$log_folder\"/g" $filename
sed -ie "s/<LOG-LEVEL>/\"$log_level\"/g" $filename
sed -ie "s/<PYSPARK-LOGLEVEL>/\"$pyspark_log_level\"/g" $filename
sed -ie "s/<DATA-SOURCE>/\"$data_source\"/g" $filename
sed -ie "s/<PROFILE-MODEL-CATEGORICAL-COLUMNS-STRING>/\"$profile_categorical_column_string\"/g" $filename
sed -ie "s/<PROFILE-MODEL-NUMERICAL-COLUMNS-STRING>/\"$profile_numerical_column_string\"/g" $filename
sed -ie "s/<DATABASE>/\"$database\"/g" $filename
sed -ie "s/<TABLE-NAME>/\"$table_name\"/g" $filename
sed -ie "s/<WRITE-TABLE-NAME>/\"$write_table_name\"/g" $filename
sed -ie "s/<DATE-ARGUMENT>/\"$date_argument\"/g" $filename
sed -ie "s/<NO-OF-DAYS>/\"$no_of_days\"/g" $filename




profile_anomaly_train_filename="./scripts/cron/win_profile_anomaly_train_and_score.sh"

cp ./scripts/templates/pyspark_env.sh.template $profile_anomaly_train_filename

for i in $data_source
do
    while IFS= read -r line;do
        line=${line/<DATA-SOURCE>/$i}
        line=${line/<DATA-SOURCE>/$i}
        echo "$line" >> $profile_anomaly_train_filename
    done < "./scripts/templates/win_profile_anomaly_train_and_score.sh.template"
done

pwd=$(echo `pwd` | sed 's/\//\\\//g')
sed -ie "s/<PWD>/$pwd/g" $profile_anomaly_train_filename
rm -rf $profile_anomaly_train_filename"e"


echo "Model Deployment for "$tenant_id" - ended"