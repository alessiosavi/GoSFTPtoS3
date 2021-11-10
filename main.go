package GoSFTPtoS3

import (
	"bytes"
	"errors"
	"fmt"
	arrayutils "github.com/alessiosavi/GoGPUtils/array"
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

var DEFAULT_KEY_EXCHANGE_ALGO = []string{"diffie-hellman-group-exchange-sha256"}

type SFTPConf struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"pass"`
	Bucket   string `json:"bucket"`
	Port     int    `json:"port"`
	Timeout  int    `json:"timeout"`
	PrivKey  string `json:"priv_key"`
}

func (c *SFTPConf) Validate() error {
	if stringutils.IsBlank(c.Host) {
		return errors.New("SFTP host not provided")
	}
	if stringutils.IsBlank(c.User) {
		return errors.New("SFTP user not provided")
	}
	if stringutils.IsBlank(c.Password) && stringutils.IsBlank(c.PrivKey) {
		return errors.New("SFTP password and priv_key not provided")
	}
	// FIXME: Maybe this can be blank for usage different for sync SSH/SFTP to S3
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

//NewConn Create a new SFTP connection by given parameters
func (c *SFTPConf) NewConn(keyExchanges ...string) (*SFTPClient, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	// Add default key exchange algorithm
	for _, algo := range DEFAULT_KEY_EXCHANGE_ALGO {
		if !arrayutils.InStrings(keyExchanges, algo) {
			keyExchanges = append(keyExchanges, algo)
		}
	}
	var auth []ssh.AuthMethod

	if !stringutils.IsBlank(c.PrivKey) {
		key, err := ssh.ParsePrivateKey([]byte(c.PrivKey))
		if err != nil {
			return nil, err
		}
		auth = append(auth, ssh.PublicKeys(key))
	} else if !stringutils.IsBlank(c.Password) && !stringutils.IsBlank(c.User) {
		auth = append(auth, ssh.Password(c.Password))
	}

	config := &ssh.ClientConfig{
		User:            c.User,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(c.Timeout) * time.Second,
	}

	config.Config.KeyExchanges = append(config.Config.KeyExchanges, keyExchanges...)
	addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
	log.Println("Connecting to: " + addr)
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}
	client, err := sftp.NewClient(conn) // create sftp client
	if err != nil {
		return nil, err
	}
	return &SFTPClient{Client: client}, nil
}

func (c *SFTPClient) Get(remoteFile string) (*bytes.Buffer, error) {
	srcFile, err := c.Client.Open(remoteFile)
	if err != nil {
		return nil, err
	}
	defer srcFile.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, srcFile)
	return buf, err
}
func (c *SFTPClient) Put(data []byte, fpath string) error {
	dirname := path.Dir(fpath)
	exist, err := c.Exist(dirname)
	if err != nil {
		return err
	}
	if !exist {
		if err := c.CreateDirectory(dirname); err != nil {
			return err
		}
	}
	f, err := c.Client.Create(fpath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}

func (c *SFTPClient) CreateDirectory(path string) error {
	return c.Client.MkdirAll(path)
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

func (c *SFTPClient) DeleteDirectory(path string) error {
	return c.Client.RemoveDirectory(path)
}

func (c *SFTPClient) List(path string) ([]string, error) {
	exist, err := c.Exist(path)
	if err != nil {
		return nil, err
	} else if !exist {
		return nil, errors.New(fmt.Sprintf("path %s does not exists!", path))
	}
	isDir, err := c.IsDir(path)
	if err != nil {
		return nil, err
	}
	if !isDir {
		return nil, errors.New(fmt.Sprintf("path %s is not a dir!", path))
	}

	walker := c.Client.Walk(path)
	var files []string
	for walker.Step() {
		if err = walker.Err(); err != nil {
			log.Printf("Error with file: %s | Err: %s\n", walker.Path(), err)
			continue
		}
		files = append(files, walker.Path())
	}
	return files, nil
}

func (c *SFTPClient) Exist(path string) (bool, error) {
	_, err := c.Client.Lstat(path)
	if err != nil && err.Error() == "file does not exist" {
		return false, nil
	}
	return err == nil, err
}

func (c *SFTPClient) IsDir(path string) (bool, error) {
	lstat, err := c.Client.Lstat(path)
	if err != nil {
		return false, err
	}
	return lstat.IsDir(), nil
}
func (c *SFTPClient) IsFile(path string) (bool, error) {
	lstat, err := c.Client.Lstat(path)
	if err != nil {
		return false, err
	}
	return !lstat.IsDir(), nil
}

func (c *SFTPClient) Close() error {
	return c.Client.Close()
}
