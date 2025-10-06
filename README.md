# RUNNING ICARE ETL

Running ICare ETL module make sure to specify the following:

Run `docker exec -it [CONTAINER NAME] bash`

Get into `.OpenMRS` folder the print `openmrs-runtime.properties` by running the following command:

```
cd .OpenMRS

# Show openmrs-runtime.properties
cat openmrs-runtime.properties
```

Copy this and store somewhere else for you will use them later:


Now create `openmrs-runtime.properties` volume in docker compose to be able to expose these settings.

Once done, paste the openmrs-runtime.properties copied from the container to this new file.

Then update by appending this and set as you wish:

```
 mambaetl.analysis.db.driver=com.mysql.cj.jdbc.Driver

 mambaetl.analysis.db.url=jdbc\:mysql\://localhost\:3306/icare_analytics?useSSL\=false&autoReconnect\=true

 mambaetl.analysis.db.username=db_user

 mambaetl.analysis.db.password=password

 mambaetl.analysis.db.openmrs_database=openmrs

 mambaetl.analysis.db.etl_database=icare_analytics
 
 mambaetl.analysis.locale=en
 
 mambaetl.analysis.columns=100
 
 mambaetl.analysis.incremental_mode=1
 
 mambaetl.analysis.automated_flattening=1
 
 mambaetl.analysis.etl_interval=180
```

Then restart the iCare container.

Now, login into the database container (MySQL) and allow user to be able to create and work on the icare_analytics database accordingly:

Get mysql command line:

```
docker exec -it [CONTAINER NAME] mysql -u root -p
```

Enter password then run the following query:


```
GRANT ALL PRIVILEGES ON *.* TO 'db_user'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

once done, verify that privileges have been granted:
```
SHOW GRANTS FOR 'db_user'@'%';
```


Now deploy iCare ETL built module accordingly.

To build iCare ETL module clone the repository

```
git clone https://github.com/udsm-dhis2-lab/icare-etl
```

and clone mamba core:
```
git clone https://github.com/openmrs/openmrs-module-mamba-core
```

Make sure to have installed `maven` in your machine.

Build Mamba core first then icare-etl. 

get omod file from `icare-etl/omod/target/icare-etl-1.0.1-SNAPSHOT.omod` then deploy it on your running OpenMRS instance.

Enjoy