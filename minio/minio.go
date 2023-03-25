package minio

import (
	"context"
	"fmt"
	_ "github.com/joho/godotenv/autoload" // load .env file automatically
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"log"
	"strconv"
)

type SetMinio struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	Location        string
	UseSSL          string
}

type PutFile struct {
	Bucket      string
	Name        string
	Path        string
	ContentType string
}

// Hello returns a greeting for the named person.
func Hello(name string) string {
	// Return a greeting that embeds the name in a message.
	message := fmt.Sprintf("Hi, %v. Welcome!", name)
	return message
}

func SetConfig(setMinio SetMinio) (*minio.Client, error) {
	useSSL, errSsl := strconv.ParseBool(setMinio.UseSSL)
	if errSsl != nil {
		log.Println(errSsl)
		return nil, nil
	}

	// Initialize minio client object.
	minioClient, errInit := minio.New(setMinio.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(setMinio.AccessKeyID, setMinio.SecretAccessKey, ""),
		Secure: useSSL,
	})
	return minioClient, errInit
}

func AddFile(minioClient *minio.Client, putFile PutFile, setMinio SetMinio) {
	ctx := context.Background()

	// Make a new bucket called dev-minio.
	err := minioClient.MakeBucket(ctx, putFile.Bucket, minio.MakeBucketOptions{Region: setMinio.Location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, putFile.Bucket)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", putFile.Bucket)
		} else {
			log.Printf(err.Error())
			return
		}
	} else {
		log.Printf("Successfully created %s\n", putFile.Bucket)
	}

	// Upload the zip file with FPutObject
	info, err := minioClient.FPutObject(ctx, putFile.Bucket, putFile.Name, putFile.Path, minio.PutObjectOptions{ContentType: putFile.ContentType})
	if err != nil {
		log.Printf(err.Error())
		return
	}

	log.Printf("Successfully uploaded %s of size %d\n", putFile.Name, info.Size)
}

func AddBucket(minioClient *minio.Client, bucket string, setMinio SetMinio) {
	ctx := context.Background()

	// Make a new bucket called dev-minio.
	err := minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: setMinio.Location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, bucket)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucket)
		} else {
			log.Printf(err.Error())
			return
		}
	} else {
		log.Printf("Successfully created %s\n", bucket)
	}
}

func AddBinObject(minioClient *minio.Client, bucket, filename string, file io.ReadSeeker, size int64) {
	uploadInfo, err := minioClient.PutObject(
		context.Background(),
		bucket,
		filename,
		file,
		size,
		minio.PutObjectOptions{ContentType: "application/octet-stream"})

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully uploaded: ", uploadInfo)
}

func RemoveAllFromBucket(minioClient *minio.Client, bucket string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objectsCh := make(chan minio.ObjectInfo)

	// Send object names that are needed to be removed to objectsCh
	go func() {
		defer close(objectsCh)
		// List all objects from a bucket-name with a matching prefix.
		for object := range minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Recursive: true,
		}) {
			if object.Err != nil {
				log.Fatalln(object.Err)
			}
			objectsCh <- object
		}
	}()

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}

	for rErr := range minioClient.RemoveObjects(context.Background(), bucket, objectsCh, opts) {
		fmt.Println("Error detected during deletion: ", rErr)
	}
}
