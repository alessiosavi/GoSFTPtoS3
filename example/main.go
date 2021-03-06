package main

import (
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/alessiosavi/GoSFTPtoS3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

func main() {
	var sftpConf = &GoSFTPtoS3.SFTPConf{
		Host:     "test.rebex.net",
		User:     "demo",
		Password: "password",
		Port:     22,
		Bucket:   "bucket-ftp",
		Timeout:  5,
	}

	conn, err := sftpConf.NewConn([]string{"diffie-hellman-group-exchange-sha256"})
	if err != nil {
		panic(err)
	}
	// Init the default configuration and initialize a new session
	awsConfig := aws.Config{Region: aws.String("us-east-2")}
	sess, err := session.NewSessionWithOptions(session.Options{Config: awsConfig})
	if err != nil {
		panic(err)
	}
	s3session := s3.New(sess)

	conn.PutToS3("", "CUSTOM_PREFIX", "text/csv", s3session, renameFile)
}

// renameFile is a custom function delegated to rename the file before writing to S3
// If you want to mantain the same file of the s3, just use `return fName`
// In this case, we delete the initial path and return the other part of the file
func renameFile(fName string) string {
	s := strings.Split(fName, "/")
	return stringutils.JoinSeparator("/", s[1:]...)
}
