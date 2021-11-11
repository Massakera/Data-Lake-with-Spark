# Project: Data Lake

## Project overview
Sparkify offers a music streaming service through desktop and hand-held devices.<br>
The music streaming startup has grown their user base and song database even more and want to move their data warehouse<br>
to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a<br>
directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them<br>
using Spark, and loads the data back into S3 as a set of dimensional tables. You'll deploy this Spark process on a cluster using AWS.<br>
This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Repository

#### Files to run project...
- [ **etl.py** ] (*Python 3 script*):<br>
  Reads song and app log data from S3 hosted json files, transforms them to create five different tables, and writes<br>
  them to partitioned parquet files in table directories back to S3.
  
- [ **dl.cfg** ] (*config text file*):<br>
  Contains user AWS credentials, utilised by etl.py script.
  
- [ **EMR_notebook.ipynb** ] (*Jupyter Notebook*):<br>
  EMR Notebook of etl.py script for running on cluster via AWS console.
  


### Running the project locally
(Prerequisite: ensure Python 3, Pyspark package and Apache Spark are all installed on local machine)

1. Download project Python script and config file, as listed above, to a local directory.
2. Add AWS 'Access Key ID & 'Secret Access Key' credentials to details to dl.cfg.
3. Within etl.py script, set 'output_data' variable to S3 bucket where parquet files are to be written.
4. Open your system CLI and change directory to where the project files are saved.<br>
   
        C:\users\username>cd C:\users\username\path\to\project

5. Run Python script to create a spark session and process data... *etl.py*;<br>

        C:\users\username>cd C:\users\username\path\to\project>python3 etl.py

---
## Dataset
2No. datasets are available for ingest and processing, residing in AWS S3.<br>

    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data

### Song data
Song data resides in JSON format, with each file containing metadata about a specific song, and the song's artist.<br>
Within Sparkify's file storage, song files are partitioned by the first three letters of each song's track ID.

Filepath example...

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

TRAABJL12903CDCF1A.json song file content...

    {
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
    }

###  Log data
User activity logs, collected via the Sparkify music streaming applications, also resides in JSON format.<br>
Each file represents a single day and contains information about each user and their session details for that day.
Within Sparkify's file storage, log files are partitioned by the month and year.

    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json

2018-11-12-events.json log file content...

    {
    "artist":null,
    "auth":"Logged In",
    "firstName":"Celeste",
    "gender":"F",
    "itemInSession":0,
    "lastName":"Williams",
    "length":null,
    "level":"free",
    "location":"Klamath Falls, OR",
    "method":"GET",
    "page":"Home",
    "registration":1541077528796.0,
    "sessionId":438,
    "song":null,
    "status":200,
    "ts":1541990217796,
    "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64)<br>
                AppleWebKit\/537.36 (KHTML, like Gecko)<br>
                Chrome\/37.0.2062.103 Safari\/537.36\"",
    "userId":"53"
    }

---

### Table summary

Each of the five tables are written to parquet files in a separate analytics directory on S3.<br>
Each table has its own folder within the directory.<br>
Songplays table files are partitioned by year and month.<br>
Songs table files are partitioned by year and then artist.<br>
Time table files are partitioned by year and month.

**Table Name**  | **Description**
--------------- | ---------------
**songplays** | Fact Table;  Log data associated with song plays, filtered by user action 'Next Song'.
**users** | Dimension Table; Registered application users
**songs** | Dimension Table; Songs in music database
**artists** | Dimension Table; Artists in music database
**time** | Dimension Table; Timestamps of **songplays** records, broken down into specific units