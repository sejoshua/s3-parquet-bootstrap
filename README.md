# S3 CSV to Parquet Converter

### To Run the Application
AWS access and secret keys are required to run the application. For example:
```shell
java -Daws.accessKey="<access_key>" -Daws.secretKey="<secret_key>" -jar s3-parquet-bootstrap.jar
```

Reference: [ApplicationConfig](src/main/java/org/example/config/ApplicationConfig.java)

### Introduction
The application is built without any framework. Spring framework was
considered, but it is not necessary to open any web interface, nor interact
with some other services (like messaging or database), nor a long run
application. Therefore, the converter is a simple Java application.

But the code was designed to be easily accommodated with Spring framework. For example,
[ApplicationConfig](src/main/java/org/example/config/ApplicationConfig.java) can be
annotated with Spring `@Configuration`, so that the application configuration can be 
externalized. [S3CsvFileConversionThread](src/main/java/org/example/service/S3CsvFileConversionThread.java)
can be initialized and run in an implementation of `CommandLineRunner`, to turn the whole
process in a long run mode.

### Assumption
A couple of assumptions were made in the implementation:
* All zip files on S3 have the extension "zip".
* Some of the CSV files in the zip file do not have header. For such case, the application
will name the columns as "col_1", "col_2", ... in the parquet file.
* As there is no easy way to detect if a CSV file has header or not, if the files in the
zip file have header or not are hard-coded.
* There is no type information for columns in CSV file, therefore all columns are
treated as string type. The ideal way is to create a configuration file for each column 
and its corresponding type and then build the parquet schema out of the configuration file.