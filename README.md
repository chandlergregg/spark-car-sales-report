# spark-car-sales-report

### About

This repo contains code for the Spark car sales report project, an example of using Spark to process data locally and in HDFS. This project is a Spark re-write of the [Hadoop car sales report](https://github.com/chandlergregg/hadoop-car-sales-report). This project is part of the Springboard Data Engineering Career Track.

### Details

As in the Hadoop project, we take data on car sales, repairs, and accidents to output a report of the number of accidents by car make and year. This output can be used to determine which cars are accident-prone and used-car buyers can use the output data to inform their car buying decisions.

The input data is missing some data for the accident records that we care about, so we start by filling in the make and year of every record from the records that do contain this data. Then we group by make and year to get a count of how many accidents each make-year combination has and output this data.

There are two versions of the code for this project:
1. Run locally on Jupyter notebook within Pipenv
2. Run on Hortonworks Sandbox virtual environment using `spark-submit`

##### Data definition of `data.csv`
| Column        | Type                                                                |
|---------------|---------------------------------------------------------------------|
| incident_id   | INT                                                                 |
| incident_type | STRING (I: initial sale, A: accident, R: repair)                    |
| vin_number    | STRING                                                              |
| year          | STRING (The year of the car, only populated with incident type “I”) |
| Incident_date | DATE (Date of the incident occurrence)                              |
| description   | STRING                                                              |

### Running locally

Clone this repo.
To run locally, set up a Pipenv with PySpark and Jupyter installed - see [here](https://www.lukaskawerau.com/local-pyspark-jupyter-mac/) for directions on how to do so. Once your Pipenv is set up, start Jupyter and open `autoinc_spark.ipynb` in a browser. You can run through the code in the notebook step-by-step and the data will be written to the `output` folder.

### Running on HDFS

Clone this repo.
Hortonworks Sandbox version `2.5.0.0-1245` running on a VirtualBoxVM was used to run the code in HDFS. For details on how to install on your local machine, see instructions [here](https://www.youtube.com/watch?v=735yx2Eak48&ab_channel=BinodSumanAcademy).

##### How to execute MapReduce jobs in HDFS:
1. SSH into VirtualBoxVM:
  - On local machine: `ssh root@127.0.0.1 -p 2222`
2. Copy `data.csv` into HDFS:
  - On local machine - copy from local to VirtualBoxVM:
    - `scp -P 2222 <path>/data.csv root@127.0.0.1:~/`
  - On VirtualBoxVM - copy from VirtualBoxVM to HDFS and make output directory:
    - `hdfs dfs -mkdir /input`
    - `hdfs dfs -mkdir /spark-output`
    - `hdfs dfs -put data.csv /input`
    - `rm data.csv`
3. Copy Python script and shell script to VirtualBoxVM:
  - On local machine: `scp -P 2222 <path>/autoinc_spark.py root@127.0.0.1:~/`
  - On local machine: `scp -P 2222 <path>/spark_submit.sh root@127.0.0.1:~/`
4. Run shell script to submit Python script to Spark:
  - On VirtualBoxVM: `sh spark_submit.sh`
5. Get output from HDFS to local:
  - On VirtualBoxVM: `hdfs dfs -get /spark-output ~/`

Once the output is on the VirtualBoxVM, you can compare the output with the `spark-output` folder that is cloned with this repo.
