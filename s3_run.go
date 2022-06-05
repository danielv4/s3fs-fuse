/*
 * sshfs.go
 *
 * Copyright 2022 Daniel Vanderloo
 */
/*
 * This file is part of Cgofuse.
 *
 * It is licensed under the MIT license. The full license text can be found
 * in the License.txt file at the root of this project.
 */

package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"path"
	"time"
)

type S3Config struct {
	
	SecretAccessKey string 
	AccessKeyId     string
	Region          string
}


type S3 struct {
	
	client *aws_s3.S3
	bucket string 
	config S3Config
}


type S3FileObject struct {
	
	IsDir         bool
	Name          string
	Size          int
	LastModified  time.Time
}


func (self *S3) ReadDir(dirname string) ([]S3FileObject, error) {

	var err error
	var arr []S3FileObject

	input := aws_s3.ListObjectsV2Input{}
	input.Bucket = aws.String(self.bucket)
	input.Delimiter = aws.String("/")
	
	if dirname != "/" {
		input.Prefix = aws.String(dirname[1:] + "/")
	}

	resp, err := self.client.ListObjectsV2(&input)
	if err != nil {
		return arr, err
	}
	
	fmt.Printf("%+v\n", resp)
	
	
	// dir
	for _, item := range resp.CommonPrefixes {
	
		obj := S3FileObject{}
		obj.IsDir = true
		obj.Name = path.Base(path.Dir(*item.Prefix))
		arr = append(arr, obj)
    }	
	
	// files
	for _, item := range resp.Contents {
	
		name := *item.Key
		last := name[len(name)-1:]
		if last != "/" {
			obj := S3FileObject{}
			obj.IsDir = false
			obj.Name = path.Base(name)
			obj.LastModified = *item.LastModified
			obj.Size = int(*item.Size)
			arr = append(arr, obj)		
		}
    }
	

	return arr, err
}


func NewClient(bucketName string, config S3Config) (*S3, error) {
	
	var err error
	s3 := new(S3)
	
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.SecretAccessKey, config.AccessKeyId, ""),
	})
	
	if err != nil {
		return s3, err
	}
	
	svc := aws_s3.New(sess)	
	
	s3.client = svc
	s3.config = config
	s3.bucket = bucketName
	
	//result, err := svc.ListBuckets(&aws_s3.ListBucketsInput{})
	//if err != nil {
	//	return s3, err
    //}
	//fmt.Printf("%+v\n", result)
	
	
	return s3, err
}



func main() {


	config := S3Config{}
	config.SecretAccessKey = "SecretAccessKey"
	config.AccessKeyId = "AccessKeyId"
	config.Region = "us-west-2"

	s3, err := NewClient("swift2", config)
	if err != nil {
		fmt.Println("unable to create aws session")
	}


	files, err := s3.ReadDir("bytes/")
	if err != nil {
		fmt.Println("unable to ReadDir")
    }
	fmt.Printf("%+v\n", files)
}
