# Smart City Real-Time Data Engineering

This repository encompasses a comprehensive Smart City data engineering project that develops a real-time data pipeline. The system monitors motorcycle training maneuvers, employing technologies such as AWS services, Kafka streams, Spark processing, and S3 storage, with visualization powered by PowerBI.

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [Acknowledgments](#acknowledgments)

### Project Overview

The project integrates multiple data sources, including GPX files for route information, weather data through OpenWeather API, and simulated failure events, to provide a real-time view of motorcycle training sessions.
![Project_architecture.png](assets%2FProject_architecture.png)
### Technologies Used

- AWS (S3, Glue, Redshift, Lambda)
- Apache Kafka
- Apache Spark
- Docker
- PowerBI

### Installation

```bash
pip install gpxpy
```
### Usage

This project utilizes Docker for environment management. Start by logging into Docker Hub to pull the necessary Kafka images. 
```bash
docker compose up -d
```

To begin streaming data:
Start Kafka to send data to topics.

```bash
python jobs/main.py
```
Execute Spark to consume topics and push data to S3.

```bash
     ##Windows PowerShell##
docker exec -it smartcityrvm-spark-master-1 spark-submit `
--master spark://spark-master:7077 `
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 `
jobs/spark-city.py
```

## Contributing
To contribute to this project, please send a pull request or open an issue to discuss what you would like to change.

## Acknowledgments
I'd like to express my sincere gratitude to all contributors and mentors who have supported this project. A special shoutout to my teacher Yusuf Ganiyu for his guidance and to my family for their patience and support.
