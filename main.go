package GoSFTPtoS3

import (
	"bytes"
	"fmt"
	httputils "github.com/alessiosavi/GoGPUtils/http"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPConf struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"pass"`
	Bucket   string `json:"bucket"`
	Port     int    `json:"port"`
	Timeout  int    `json:"timeout"`
}

func (c *SFTPConf) Validate() {
	if stringutils.IsBlank(c.Host) {
		panic("SFTP host not provided")
	}
	if stringutils.IsBlank(c.User) {
		panic("SFTP user not provided")
	}
	if stringutils.IsBlank(c.Password) {
		panic("SFTP password not provided")
	}
	if stringutils.IsBlank(c.Bucket) {
		panic("SFTP bucket not provided")
	}
	if !httputils.ValidatePort(c.Port) {
		panic("SFTP port not provided")
	}
}

type SFTPClient struct {
	AWSSession *session.Session
	Client     *sftp.Client
	Bucket     string
}

// Create a new SFTP connection by given parameters
func (c *SFTPConf) NewConn(keyExchanges []string) (*SFTPClient, error) {
	var sess *session.Session
	var err error
	var conn *ssh.Client

	c.Validate() // Panic in case of missing configuration

	// initialize AWS Session
	if sess, err = session.NewSession(); err != nil {
		panic(err)
	}

	get, err := sess.Config.Credentials.Get()
	if err == nil {
		log.Printf("Using the following credentials: %+v\n", get)
	}

	config := &ssh.ClientConfig{
		User:            c.User,
		Auth:            []ssh.AuthMethod{ssh.Password(c.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(c.Timeout) * time.Second,
	}
	config.Config.KeyExchanges = append(config.Config.KeyExchanges, keyExchanges...)
	// connect to ssh
	addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
	log.Println("Connecting to: " + addr)
	if conn, err = ssh.Dial("tcp", addr, config); err != nil {
		return nil, err
	}

	// create sftp client
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}
	return &SFTPClient{AWSSession: sess, Client: client, Bucket: c.Bucket}, nil
}

func (c *SFTPClient) Get(remoteFile string) (*bytes.Buffer, error) {
	log.Println("Downloading file: " + remoteFile)
	srcFile, err := c.Client.Open(remoteFile)
	if err != nil {
		return nil, err
	}
	defer srcFile.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, srcFile)
	return buf, err
}

// PutToS3 is delegated to download the file from the SFTP server and load into an S3 bucket
// folderName: name of the SFTP folder (use an empty string for scan all the folder present)
// predix: optional prefix; filter only the file that start with the given prefix
// s3session: aws S3 session object in order to connect to the bucket
// f: function delegated to rename the file to save in the S3 bucket.
//  If no modification are needed, use a function that return the input parameter as following:
//  func rename(fName string)string{return fName}
func (c *SFTPClient) PutToS3(folderName, prefix, contentType string, s3session *s3.S3, renameFile func(fName string) string) {
	walker := c.Client.Walk(folderName)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			log.Printf("Error with file: %s | Err: %s\n", walker.Path(), err)
			continue
		}

		currentPath := walker.Path()
		log.Println(currentPath)
		temp := strings.Split(currentPath, "/")
		fName := temp[len(temp)-1]

		// Avoid to manage the first path (input parameter)
		// If prefix provided, verify that the filename start with it
		if currentPath == folderName || (!stringutils.IsBlank(prefix) && !strings.HasPrefix(fName, prefix)) {
			log.Printf("Current file [%s] does not start with prefix [%s], skipping ...\n", fName, prefix)
			continue
		}
		// Recursive download all sub folder if the current filepath is a folder
		if walker.Stat().IsDir() {
			c.PutToS3(path.Join(currentPath), prefix, contentType, s3session, renameFile)
		} else {
			get, err := c.Get(currentPath)
			if err != nil {
				panic(err)
			}
			// Apply the given renaming function to rename the S3 file name
			s3FileName := renameFile(currentPath)
			log.Printf("Saving file in: %s/%s\n", c.Bucket, s3FileName)
			if stringutils.IsBlank(contentType) {
				contentType = http.DetectContentType(get.Bytes())
			}
			if _, err := s3session.PutObject(&s3.PutObjectInput{
				Body:        bytes.NewReader(get.Bytes()),
				Bucket:      aws.String(c.Bucket),
				ContentType: aws.String(contentType),
				Key:         aws.String(s3FileName),
			}); err != nil {
				panic(err)
			}
		}
	}
}

func RenameFile(fName string) string {
	s := strings.Split(fName, "/")
	return stringutils.JoinSeparator("/", s[1:]...)
}
