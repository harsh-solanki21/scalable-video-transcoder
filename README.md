# Scalable Video Transcoding Service

## Scalable Backend Architecture

![Backend-Diagram](https://github.com/user-attachments/assets/ea27b04b-e77d-4a8c-af4a-fd880baf2698)

### Technologies Used

- **AWS Services**: S3, SQS, Lambda, ECR, ECS, CloudWatch
- **Backend**: Golang
- **Docker**
- **Serverless Framework**

### Project Structure

The Backend is organized into 2 components:

- **Dispatcher**: AWS Lambda function triggered by SQS, initiates video transcoding tasks.
- **Worker**: Manages the transcoding process by spinning up Docker containers on ECS.

### Features

- **Transcoding**: Automatically transcodes uploaded videos into multiple resolutions (1080p, 720p, 480p, 360p).
- **AWS Integration**: Utilizes AWS services like S3 for video storage, SQS for task queue, Lambda for serverless computing, ECR for storing service images, ECS for scalable containerized transcoding and CloudWatch for logging.
- **Dockerized Video Transcoding**: Leverages Docker containers for efficient and scalable video transcoding tasks.
- **Event-Driven Architecture**: Initiates video transcoding tasks automatically upon video upload using S3 events and triggers further processing using SQS and Lambda functions.

<br />

Frontend is Under Construction 🏗️, will be live in a week 👷
