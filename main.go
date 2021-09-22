package GoSFTPtoS3

import (
	"bytes"
	"errors"
	"fmt"
	s3utils "github.com/alessiosavi/GoGPUtils/aws/S3"
	httputils "github.com/alessiosavi/GoGPUtils/http"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"io"
	"log"
	"path"
	"strings"
	"time"

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

func (c *SFTPConf) Validate() error {
	if stringutils.IsBlank(c.Host) {
		return errors.New("SFTP host not provided")
	}
	if stringutils.IsBlank(c.User) {
		return errors.New("SFTP user not provided")
	}
	if stringutils.IsBlank(c.Password) {
		return errors.New("SFTP password not provided")
	}
	if stringutils.IsBlank(c.Bucket) {
		return errors.New("SFTP bucket not provided")
	}
	if !httputils.ValidatePort(c.Port) {
		return errors.New("SFTP port not provided")
	}
	return nil
}

type SFTPClient struct {
	Client *sftp.Client
	Bucket string
}

// NewConn Create a new SFTP connection by given parameters
func (c *SFTPConf) NewConn(keyExchanges []string) (*SFTPClient, error) {
	var err error
	var conn *ssh.Client

	if err = c.Validate(); err != nil {
		return nil, err
	}

	// initialize AWS Session
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
	return &SFTPClient{Client: client, Bucket: c.Bucket}, nil
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
func (c *SFTPClient) PutToS3(folderName, prefix string, ignores []string, renameFile func(fName string) string) error {
	walker := c.Client.Walk(folderName)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			log.Printf("Error with file: %s | Err: %s\n", walker.Path(), err)
			continue
		}

		currentPath := walker.Path()
		var ignoreFile bool = false
		for _, ignore := range ignores {
			if strings.Contains(strings.ToLower(currentPath), strings.ToLower(ignore)) {
				log.Printf("Avoid to manage file [%s] due to ignore [%s]\n", currentPath, ignore)
				ignoreFile = true
				break
			}
		}

		fName := path.Base(currentPath)

		// Avoid to manage the first path (input parameter)
		// If prefix provided, verify that the filename start with it
		if currentPath == folderName || ignoreFile || (!stringutils.IsBlank(prefix) && !strings.HasPrefix(fName, prefix)) {
			log.Printf("Current file [%s] does not start with prefix [%s], skipping ...\n", fName, prefix)
			continue
		}
		// If the current filepath is a folder, recursive download all sub folder
		if walker.Stat().IsDir() {
			if err := c.PutToS3(path.Join(currentPath), prefix, ignores, renameFile); err != nil {
				return err
			}
		} else {
			get, err := c.Get(currentPath)
			if err != nil {
				return err
			}
			// Apply the given renaming function to rename the S3 file name
			s3FileName := renameFile(currentPath)
			log.Printf("Saving file in: Bucket: [%s] | Key: [%s]\n", c.Bucket, s3FileName)
			if err = s3utils.PutObject(c.Bucket, s3FileName, get.Bytes()); err != nil {
				return err
			}
		}
	}
	return nil
}

// RenameFile Example function for rename the file before upload to S3
// In this case we remove the first folder from the name
// Example: first_folder/second_folder/file_name.txt --> second_folder/file_name.txt
func RenameFile(fName string) string {
	s := strings.Split(fName, "/")
	return stringutils.JoinSeparator("/", s[1:]...)
}

func (c *SFTPClient) DeleteFile(path string) error {
	if exists, err := c.Exist(path); err != nil {
		return err
	} else if exists {
		return c.Client.Remove(path)
	} else {
		return errors.New(fmt.Sprintf("file %s does not exists", path))
	}
}

func (c *SFTPClient) Exist(path string) (bool, error) {
	_, err := c.Client.Lstat(path)
	return err != nil, err
}
