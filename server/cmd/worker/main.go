package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Metrics struct {
	Duration   time.Duration
	Resolution int
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	inputBucket := os.Getenv("INPUT_BUCKET")
	outputBucket := os.Getenv("OUTPUT_BUCKET")
	awsRegion := os.Getenv("AWS_REGION")
	videoName := os.Getenv("VIDEO_NAME")

	if inputBucket == "" || outputBucket == "" || awsRegion == "" || videoName == "" {
		logger.Fatal("Required environment variables not set")
	}

	logger.Printf("Starting worker with config - Input bucket: %s, Output bucket: %s, Region: %s, Video: %s",
		inputBucket, outputBucket, awsRegion, videoName)

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		logger.Fatalf("Unable to load AWS SDK config: %v", err)
	}
	log.Println("Loaded AWS SDK config = ", cfg)

	// Initialize S3 client
	s3Client := s3.NewFromConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		cancel()
	}()

	// Validate video file format
	if !isVideoFile(videoName) {
		logger.Fatalf("Invalid video file format: %s", videoName)
	}

	// Process the video
	metrics, err := processVideo(ctx, s3Client, inputBucket, outputBucket, videoName)
	if err != nil {
		logger.Printf("Error processing video: %v", err)
		os.Exit(1)
	}

	logger.Printf("Video processed successfully in %v with resolution %d", metrics.Duration, metrics.Resolution)
}

func isVideoFile(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	videoExtensions := []string{".mp4", ".mov", ".avi", ".mkv"}
	for _, validExt := range videoExtensions {
		if ext == validExt {
			return true
		}
	}
	return false
}

func processVideo(ctx context.Context, s3Client *s3.Client, inputBucket, outputBucket, videoName string) (*Metrics, error) {
	startTime := time.Now()
	metrics := &Metrics{}

	// Create temp directory for processing
	tempDir, err := os.MkdirTemp("", filepath.Base(videoName))
	log.Printf("Created temp directory: %s", tempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Download video directly using GetObject
	inputPath, err := downloadVideo(ctx, s3Client, inputBucket, videoName, tempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to download video: %w", err)
	}

	// Verify file exists and is not empty
	if info, err := os.Stat(inputPath); err != nil || info.Size() == 0 {
		return nil, fmt.Errorf("downloaded file is invalid or empty: %w", err)
	}

	// Process video
	resolution, err := getVideoResolution(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get video resolution: %w", err)
	}
	metrics.Resolution = resolution

	outputFiles, err := transcodeVideo(ctx, inputPath, resolution)
	if err != nil {
		return nil, fmt.Errorf("failed to transcode video: %w", err)
	}

	// Verify transcoded files
	for _, file := range outputFiles {
		if info, err := os.Stat(file); err != nil || info.Size() == 0 {
			return nil, fmt.Errorf("transcoded file %s is invalid or empty", file)
		}
	}

	// Upload transcoded files
	if err := uploadTranscodedFiles(ctx, s3Client, outputFiles, outputBucket, videoName); err != nil {
		return nil, fmt.Errorf("failed to upload transcoded files: %w", err)
	}

	metrics.Duration = time.Since(startTime)
	return metrics, nil
}

func downloadVideo(ctx context.Context, client *s3.Client, bucket, videoName, tempDir string) (string, error) {
	outputPath := filepath.Join(tempDir, filepath.Base(videoName))
	file, err := os.Create(outputPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	log.Printf("Context: %v, client: %v, bucket: %v, videoName: %v, outputPath: %v", ctx, client, bucket, videoName, outputPath)

	// Get the object directly using the videoName
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(videoName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	if _, err := io.Copy(file, result.Body); err != nil {
		return "", fmt.Errorf("failed to write file: %w", err)
	}

	return outputPath, nil
}

func getVideoResolution(filepath string) (int, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=height",
		"-of", "default=noprint_wrappers=1:nokey=1",
		filepath)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("ffprobe failed: %w, output: %s", err, out.String())
	}

	height, err := strconv.Atoi(strings.TrimSpace(out.String()))
	if err != nil {
		return 0, fmt.Errorf("invalid height value: %w", err)
	}

	return height, nil
}

func transcodeVideo(ctx context.Context, inputPath string, resolution int) ([]string, error) {
	outputFiles := []string{}
	qualities := getTranscodeQualities(resolution)

	for _, quality := range qualities {
		outputPath := fmt.Sprintf("%s_%dp.mp4", strings.TrimSuffix(inputPath, filepath.Ext(inputPath)), quality)
		outputFiles = append(outputFiles, outputPath)

		args := []string{
			"-i", inputPath,
			"-c:v", "libx264",
			"-preset", "medium",
			"-crf", "23",
			"-c:a", "aac",
			"-b:a", "128k",
			"-movflags", "+faststart",
			"-y",
			"-vf", fmt.Sprintf("scale=-2:%d", quality),
			outputPath,
		}

		cmd := exec.CommandContext(ctx, "ffmpeg", args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("ffmpeg transcode failed for %dp: %w, stderr: %s", quality, err, stderr.String())
		}
	}

	return outputFiles, nil
}

func getTranscodeQualities(sourceHeight int) []int {
	qualities := []int{}

	// Standard qualities we'll generate
	possibleQualities := []int{720, 480, 360}

	// Only generate qualities lower than or equal to source
	for _, quality := range possibleQualities {
		if quality <= sourceHeight {
			qualities = append(qualities, quality)
		}
	}

	// Always include at least one quality
	if len(qualities) == 0 {
		qualities = append(qualities, possibleQualities[len(possibleQualities)-1])
	}

	return qualities
}

func uploadTranscodedFiles(ctx context.Context, client *s3.Client, files []string, bucket, videoName string) error {
	baseName := strings.TrimSuffix(videoName, filepath.Ext(videoName))

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", file, err)
		}

		quality := filepath.Base(file)
		quality = strings.TrimSuffix(quality, filepath.Ext(quality))
		quality = strings.TrimPrefix(quality, filepath.Base(baseName)+"_")

		key := fmt.Sprintf("%s/%s/%s%s",
			baseName,
			"transcoded",
			quality,
			filepath.Ext(file),
		)

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        f,
			ContentType: aws.String("video/mp4"),
		})
		f.Close()
		if err != nil {
			return fmt.Errorf("failed to upload file %s: %w", file, err)
		}
	}

	return nil
}

func deleteFromS3(ctx context.Context, client *s3.Client, bucket, videoName string) error {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(videoName),
	})
	return err
}
