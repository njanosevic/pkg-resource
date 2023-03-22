package minio

import (
	"context"
	"fmt"
	_ "github.com/joho/godotenv/autoload" // load .env file automatically
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
