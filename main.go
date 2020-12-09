package GoSFTPtoS3

import (
	"bytes"
	"fmt"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"net/http"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPConf struct {
	// Host server
	Host string
	// User for authenticate
	User string
	// Password for authenticate
	Password string
	// Bucket name of the target S3 folder
	Bucket string
	// Port of the host
	Port int
	// Timeout in seconds for the connection
	Timeout int
}

type SFTPClient struct {
	AWSSession *session.Session
	Client     *sftp.Client
	Bucket     string
}

// Create a new SFTP connection by given parameters
func (sftpConf *SFTPConf) NewConn(keyExchanges []string) (*SFTPClient, error) {
	var sess *session.Session
	var err error
	var conn *ssh.Client
	if sftpConf == nil || stringutils.IsBlank(sftpConf.Host) || stringutils.IsBlank(sftpConf.User) ||
		stringutils.IsBlank(sftpConf.Password) || stringutils.IsBlank(sftpConf.Bucket) ||
		sftpConf.Port < 22 || sftpConf.Port > 65535 {
		panic("invalid credentials")
	}

	// initialize AWS Session
	if sess, err = session.NewSession(); err != nil {
		panic(err)
	}

	get, err := sess.Config.Credentials.Get()
	if err == nil {
		fmt.Printf("Using the following credentials: %+v\n", get)
	}

	config := &ssh.ClientConfig{
		User:            sftpConf.User,
		Auth:            []ssh.AuthMethod{ssh.Password(sftpConf.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(sftpConf.Timeout) * time.Second,
	}
	config.Config.KeyExchanges = append(config.Config.KeyExchanges, keyExchanges...)
	// connet to ssh
	addr := fmt.Sprintf("%s:%d", sftpConf.Host, sftpConf.Port)
	log.Println("Connecting to: " + addr)
	if conn, err = ssh.Dial("tcp", addr, config); err != nil {
		return nil, err
	}

	// create sftp client
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}
	return &SFTPClient{AWSSession: sess, Client: client, Bucket: sftpConf.Bucket}, nil
}

func (c *SFTPClient) Get(remoteFile string) (*bytes.Buffer, error) {
	fmt.Println("Downloading file: " + remoteFile)
	srcFile, err := c.Client.Open(remoteFile)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, srcFile)
	srcFile.Close()
	return buf, err
}

// PutToS3 is delegated to load the
func (c *SFTPClient) PutToS3(folderName string, s3session *s3.S3, f func(fName string) string) {
	walker := c.Client.Walk(folderName)
	for walker.Step() {
		fmt.Println(walker.Path())
		if walker.Path() != folderName {
			if err := walker.Err(); err != nil {
				fmt.Println(err)
				continue
			}
			if walker.Stat().IsDir() { // Recursive download all folder
				c.PutToS3(path.Join(walker.Path()), s3session, f)
			} else {
				get, err := c.Get(walker.Path())
				if err != nil {
					panic(err)
				}
				// Save only the file name, not the path
				s3FileName := f(walker.Path())
				log.Println("Saving file in: " + s3FileName)
				if _, err := s3session.PutObject(&s3.PutObjectInput{
					Body:        bytes.NewReader(get.Bytes()),
					Bucket:      aws.String(c.Bucket),
					ContentType: aws.String(http.DetectContentType(get.Bytes())),
					Key:         aws.String(s3FileName),
				}); err != nil {
					panic(err)
				}
			}
		}
	}
}
