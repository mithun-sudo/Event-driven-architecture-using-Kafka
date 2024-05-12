# IPL Ball Event Data Streaming with Kafka

## Overview

This project entails the development of a Kafka producer that fetches ball event data from an API provided by IPL (Indian Premier League) in real-time. The fetched data is then streamed to a Kafka broker server, where it is consumed by multiple services. The architecture primarily consists of:

- **Kafka Producer**: Fetches ball event data from the IPL API and streams it to the Kafka broker server.
- **Kafka Consumers**:
  - **Analytics Service**: Consumes ball event data from Kafka, flattens out the data, and writes it to a PostgreSQL table named `ipl_stats`. This table stores the data at its lowest granularity level, allowing for direct querying or the creation of views on top of it.
  - **Notification Service**: Consumes ball event data from Kafka for the purpose of delivering notifications.

## Architecture

The architecture is designed as follows:

- The Kafka producer continuously fetches ball event data from the IPL API.
- This data is then written to the Kafka broker server.
- The analytics service consumes this data from Kafka, processes it, and stores it in a PostgreSQL table named `ipl_stats`.
- The notification service consumes the same data for its own purposes.

## Repository Contents

This repository contains the following components:

- **SQL Scripts**: Contains scripts related to database setup and schema.
- **Kafka Producer**: Implementation of the Kafka producer that fetches ball event data from the IPL API and streams it to Kafka.
- **Kafka Consumer Scripts**:
  - **Analytics Consumer**: Consumes data from Kafka, processes it, and writes it to the `ipl_stats` table.
  - **Notification Consumer**: Consumes data from Kafka for notification purposes.
- **Docker Compose File**: Docker-compose.yaml file for easy deployment and management of Kafka, PostgreSQL, and other services.

## Usage

1. Clone the repository to your local machine.
2. Set up Docker and Docker Compose.
3. Execute the Docker Compose file to set up the required services (Kafka, PostgreSQL).
4. Execute the SQL scripts to set up the necessary database schema.
5. Run the Kafka producer and consumer scripts as per your requirements.

## Future Enhancements

- Implement error handling and fault tolerance mechanisms in the Kafka consumers.
- Enhance the analytics service to provide more advanced statistical analysis.
- Implement additional notification mechanisms in the notification service.
