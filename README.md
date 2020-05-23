<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Overview
This is SIA - Security Intelligence and Analytics Package.

This projects unites all our components in one place.

Components are:
 - **3rdparty** - Metron and NiFi integrations JARs
 - **analytics** - Zeppelin notebooks
 - **dataflows** - NiFi dataflow templates
 - **datagen** - Data generator
 - **deployment** - scripts for getting Amabri Management Pack and RPM files for SIA
 - **hivestreaming** - Hive streaming components
 - **odm** - Open Data Model - table and view definitions
 - **parsers** - SIA custom parsers
 - **storage** - storage application for batch processing

# Build
To build run:

`mvn clean package`

To get RPM's:

`mvn clean package -Pbuild-rpms`

To get RPM's and Ambari MPacks:

`mvn clean package -Pbuild-rpms -Pmpack`

Artifacts after build:

- Ambari MPacks: `deployment/ambari-mpack/target/`
- RPM's: `deployment/rpm-docker/target/RPMS/noarch/`


# Notes
Please note that this project has dependency on Metron's 0.4.3 jars. Since this version is not in Maven repository need to build Metron manually and add some Metron's jar to local Maven repo manually:

```
mvn install:install-file -Dpackaging=jar -DgroupId=org.apache.metron -Dversion=0.4.3 -DartifactId=metron-parsers -Dfile=/var/lib/jenkins/userContent/metron-0.4.3-jars/metron-parsers-0.4.3.jar
mvn install:install-file -Dpackaging=jar -DgroupId=org.apache.metron -Dversion=0.4.3 -DartifactId=metron-enrichment -Dfile=/var/lib/jenkins/userContent/metron-0.4.3-jars/metron-enrichment-0.4.3.jar
mvn install:install-file -Dpackaging=jar -DgroupId=org.apache.metron -Dversion=0.4.3 -DartifactId=metron-common -Dfile=/var/lib/jenkins/userContent/metron-0.4.3-jars/metron-common-0.4.3.jar
mvn install:install-file -Dpackaging=jar -DgroupId=org.apache.metron -Dversion=0.4.3 -DartifactId=metron-hbase -Dfile=/var/lib/jenkins/userContent/metron-0.4.3-jars/metron-hbase-0.4.3.jar
mvn install:install-file -Dpackaging=jar -DgroupId=org.apache.metron -Dversion=0.4.3 -DartifactId=stellar-common -Dfile=/var/lib/jenkins/userContent/metron-0.4.3-jars/stellar-common-0.4.3.jar
mvn install:install-file -Dpackaging=jar -DgroupId=org.apache.metron -Dversion=0.4.3 -DartifactId=metron-storm-kafka -Dfile=metron-storm-kafka-0.4.3.jar
```
