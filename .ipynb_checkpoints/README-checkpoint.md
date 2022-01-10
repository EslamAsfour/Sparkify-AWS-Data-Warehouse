# Sparkify Data Warehouse Project "Udacity Nano Degree"
## 1. Project Overview
A music streaming startup, ***Sparkify***, has grown their user base and song database and want to ***move their processes and data onto the cloud***.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Required Process :
+ Extracts their data from S3
+ Stages them in Redshift
+ Transforms data into a set of Dimensional tables and Fact Table

## 2. Data Warehouse Design 
### 1) Fact Table 
#### ***songplays*** - Records in event data associated with song plays i.e. records with page NextSong
Attributes :
+ songplay_id
+ start_time
+ user_id
+ level
+ song_id
+ artist_id
+ session_id
+ location
+ user_agent

### 2) Dimension Tables
#### 1. users - users in the app
+ user_id
+ first_name
+ last_name
+ gender
+ level
#### 2. songs - songs in music database
+ song_id
+ title
+ artist_id
+ year
+ duration
#### 3. artists - artists in music database
+ artist_id
+ name
+ location
+ lattitude
+ longitude

#### 4. time - timestamps of records in songplays broken down into specific units
+ start_time
+ hour
+ day
+ week
+ month
+ year
+ weekday

## 3. ETL Pipeline
1. Create a new IAM user
2. Create clients for IAM, EC2, S3 and Redshift
3. Create Bucket and Add Out Dataset
4. Open an incoming TCP port to access the cluster endpoint
5. Run "Create_tables.py" to create the tables
6. Run "etl.py" to insert data to the tables
7. Clean up your resources


## 4. HOWTO use
1. Follow along with the AWS Setup.ipynb
2. Run create_tables.py with the CMD command ` python create_tables.py  `
3. Run etl.py with CMD command ` python etl.py `
4. Do your Analysis on the notebook
5. Clean up your resources




