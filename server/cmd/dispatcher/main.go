package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type VideoMetadata struct {
	Duration  float64
	Height    int
	Size      int64
	Codec     string
	Container string
}

type Config struct {
	ECSCluster    string
	ECSTaskDef    string
	ECSSubnets    []string
	InputBucket   string
	OutputBucket  string
	ContainerName string
	QueueURL      string
	Logger        *log.Logger
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		ECSCluster:    os.Getenv("ECS_CLUSTER"),
		ECSTaskDef:    os.Getenv("ECS_TASK_DEFINITION"),
		ECSSubnets:    strings.Split(os.Getenv("ECS_SUBNETS"), ","),
		InputBucket:   os.Getenv("INPUT_BUCKET"),
		OutputBucket:  os.Getenv("OUTPUT_BUCKET"),
		ContainerName: os.Getenv("CONTAINER_NAME"),
		QueueURL:      os.Getenv("SQS_QUEUE_URL"),
		Logger:        log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile),
	}

	return cfg, nil
}

func handleRequest(ctx context.Context, sqsEvent events.SQSEvent) error {
	cfg, err := loadConfig()
	log.Printf("Loaded config: %+v", cfg)

	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	ecsClient := ecs.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)

	for _, record := range sqsEvent.Records {
		if err := processRecord(ctx, record, cfg, ecsClient, s3Client, sqsClient); err != nil {
			cfg.Logger.Printf("Error processing record: %v", err)
			// Continue processing other records even if one fails
			continue
		}
	}

	return nil
}

func processRecord(ctx context.Context, record events.SQSMessage, cfg *Config, ecsClient *ecs.Client, s3Client *s3.Client, sqsClient *sqs.Client) error {
	var s3Event events.S3Event
	if err := json.Unmarshal([]byte(record.Body), &s3Event); err != nil {
		return fmt.Errorf("error unmarshalling S3 event: %w", err)
	}

	for _, s3Record := range s3Event.Records {
		metadata, err := getVideoMetadata(ctx, s3Client, s3Record.S3.Bucket.Name, s3Record.S3.Object.Key)
		if err != nil {
			return fmt.Errorf("error getting video metadata: %w", err)
		}

		if err := launchECSTask(ctx, ecsClient, cfg, metadata, s3Record.S3.Object.Key); err != nil {
			return fmt.Errorf("error launching ECS task: %w", err)
		}

		if err := deleteMessage(ctx, sqsClient, cfg.QueueURL, record.ReceiptHandle); err != nil {
			return fmt.Errorf("error deleting SQS message: %w", err)
		}

		cfg.Logger.Printf("Successfully processed video: %s", s3Record.S3.Object.Key)
	}

	return nil
}

func getVideoMetadata(ctx context.Context, client *s3.Client, bucket, key string) (*VideoMetadata, error) {
	headOutput, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object metadata: %w", err)
	}

	// Using aws.ToInt64 to safely handle the pointer conversion
	return &VideoMetadata{
		Size:      aws.ToInt64(headOutput.ContentLength),
		Container: aws.ToString(headOutput.ContentType),
	}, nil
}

func launchECSTask(ctx context.Context, client *ecs.Client, cfg *Config, metadata *VideoMetadata, videoName string) error {
	input := &ecs.RunTaskInput{
		Cluster:        aws.String(cfg.ECSCluster),
		TaskDefinition: aws.String(cfg.ECSTaskDef),
		Count:          aws.Int32(1),
		LaunchType:     types.LaunchTypeFargate,
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				Subnets:        cfg.ECSSubnets,
				AssignPublicIp: types.AssignPublicIpEnabled,
			},
		},
		Overrides: &types.TaskOverride{
			ContainerOverrides: []types.ContainerOverride{
				{
					Name: aws.String(cfg.ContainerName),
					Environment: []types.KeyValuePair{
						{
							Name:  aws.String("INPUT_BUCKET"),
							Value: aws.String(cfg.InputBucket),
						},
						{
							Name:  aws.String("OUTPUT_BUCKET"),
							Value: aws.String(cfg.OutputBucket),
						},
						{
							Name:  aws.String("VIDEO_NAME"),
							Value: aws.String(videoName),
						},
						{
							Name:  aws.String("AWS_REGION"),
							Value: aws.String(os.Getenv("AWS_REGION_NAME")),
						},
					},
					Cpu:    aws.Int32(int32(calculateCPUUnits(metadata))),
					Memory: aws.Int32(int32(calculateMemoryMB(metadata))),
				},
			},
		},
	}

	_, err := client.RunTask(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to run ECS task: %w", err)
	}
	return nil
}

func calculateCPUUnits(metadata *VideoMetadata) int {
	baseCPU := 1024                 // 1 vCPU
	if metadata.Size > 2000000000 { // 2GB
		baseCPU = 2048 // 2 vCPU
	}
	return baseCPU
}

func calculateMemoryMB(metadata *VideoMetadata) int {
	baseMemory := 2048              // 2GB
	if metadata.Size > 1000000000 { // 1GB
		baseMemory = 4096 // 4GB
	}
	return baseMemory
}

func deleteMessage(ctx context.Context, client *sqs.Client, queueURL, receiptHandle string) error {
	_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
}

func main() {
	lambda.Start(handleRequest)
}
