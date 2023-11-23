package GoSFTPtoS3

import (
	"bytes"
	"errors"
	"fmt"
	arrayutils "github.com/alessiosavi/GoGPUtils/array"
	s3utils "github.com/alessiosavi/GoGPUtils/aws/S3"
	httputils "github.com/alessiosavi/GoGPUtils/http"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/schollz/progressbar/v3"
	"io"
	"log"
	"net"
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
	Port     int    `json:"port"`
	Timeout  int    `json:"timeout"`
	PrivKey  string `json:"priv_key"`
}

func isIgnore(ignores []string, currentPath string) bool {
	for _, ignore := range ignores {
		if strings.Contains(strings.ToLower(currentPath), strings.ToLower(ignore)) {
			return true
		}
	}
	return false
}

type SFTPClient struct {
	Client *sftp.Client `json:"client,omitempty"`
	conn   *ssh.Client
}

// PutToS3 is delegated to download the file from the SFTP server and load into an S3 bucket
// sftpFolder: name of the SFTP folder (use an empty string for scan all the folder present)
// bucket: name of the bucket for copy the SFTP data
// ignores, prefix:  ignore some files, filter only the files that start with the given prefix
// renameFile: function delegated to rename the file to save in the S3 bucket. This function is necessary to give the possibility to rewrite
//
//				the SFTP filename with a given logic. func rename(fName string)string{return "CEGID/upload/SFTP_DATA_EXAMPLE/"+sftpFile}
//	If no modification are needed, use a function that return the input parameter as following:
//	func rename(fName string)string{return fName}
func (c SFTPClient) PutToS3(sftpFolder, bucket string, ignores, prefix []string, renameFile func(fName string) string) ([]string, error) {
	var fileProcessed []string
	walker := c.Client.Walk(sftpFolder)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			log.Printf("Error with file: %s | Err: %s\n", walker.Path(), err)
			continue
		}
		var currentPath string
		// Avoid to manage the first path (input parameter)
		if currentPath = walker.Path(); currentPath == sftpFolder {
			continue
		}

		fName := path.Base(currentPath)
		// If prefix provided, verify that the filename start with it
		if !stringutils.HasPrefixArray(prefix, fName) {
			continue
		}

		if isIgnore(ignores, currentPath) {
			continue
		}

		// If the current filepath is a folder, recursive download all sub folder
		if walker.Stat().IsDir() {
			if _, err := c.PutToS3(path.Join(currentPath), bucket, prefix, ignores, renameFile); err != nil {
				return nil, err
			}
		} else {
			get, err := c.Get(currentPath)
			if err != nil {
				return nil, err
			}
			// Apply the given renaming function to rename the S3 file name
			s3FileName := renameFile(currentPath)
			log.Printf(fmt.Sprintf("Saving file %s in s3://%s/%s", currentPath, bucket, s3FileName))
			if err = s3utils.PutObject(bucket, s3FileName, get.Bytes()); err != nil {
				return nil, err
			}
			fileProcessed = append(fileProcessed, currentPath)
		}
	}
	return fileProcessed, nil
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

	// Set Port 22 and 60s as timeout if not provided
	if !httputils.ValidatePort(c.Port) {
		(*c).Port = 22
	}
	if c.Timeout == 0 {
		(*c).Timeout = 60
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

// NewConn Create a new SFTP connection by given parameters
func (c SFTPConf) NewConn(keyExchanges ...string) (*SFTPClient, error) {
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

	// Verify if the private key is provided. If not provided, try with username and password.
	if !stringutils.IsBlank(c.PrivKey) {
		key, err := ssh.ParsePrivateKey([]byte(c.PrivKey))
		if err != nil {
			return nil, err
		}
		auth = append(auth, ssh.PublicKeys(key))
	} else if !stringutils.IsBlank(c.Password) && !stringutils.IsBlank(c.User) {
		auth = append(auth, ssh.Password(c.Password))
	} else {
		panic("Credentials not provided. Provide PrivKey or User and password")
	}

	config := &ssh.ClientConfig{
		User:            c.User,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(c.Timeout) * time.Second,
	}

	config.Config.KeyExchanges = append(config.Config.KeyExchanges, keyExchanges...)
	addr := net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port))
	log.Printf("Dialing %s ...\n", addr)
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}
	log.Printf("Initializing new SFTP client to: %s@%s\n", c.User, addr)
	client, err := sftp.NewClient(conn, sftp.UseFstat(true), sftp.MaxConcurrentRequestsPerFile(1)) // create sftp client
	if err != nil {
		return nil, err
	}
	return &SFTPClient{Client: client, conn: conn}, nil
}

func (c SFTPClient) Get(remoteFile string) (*bytes.Buffer, error) {
	srcFile, err := c.Client.Open(remoteFile)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	bar := progressbar.DefaultBytes(-1,
		"Downloading file ", remoteFile,
	)
	_, err = io.Copy(io.MultiWriter(buf, bar), srcFile)
	srcFile.Close()
	bar.Close()
	return buf, err
}
func (c SFTPClient) Put(data []byte, fpath string) error {
	dirname := path.Dir(fpath)
	if exist, err := c.Exist(dirname); err != nil {
		return err
	} else if !exist {
		if err = c.CreateDirectory(dirname); err != nil {
			return err
		}
	}

	f, err := c.Client.Create(fpath)
	if err != nil {
		return err
	}

	bar := progressbar.DefaultBytes(
		int64(len(data)),
		"Uploading file ", fpath,
	)
	_, err = io.Copy(io.MultiWriter(f, bar), bytes.NewReader(data))
	f.Sync()
	f.Close()
	bar.Close()
	return err
}

func (c SFTPClient) CreateDirectory(path string) error {
	return c.Client.MkdirAll(path)
}

func (c SFTPClient) DeleteFile(path string) error {
	if exists, err := c.Exist(path); err != nil {
		return err
	} else if exists {
		return c.Client.Remove(path)
	}
	return fmt.Errorf("file %s does not exists", path)
}

func (c SFTPClient) DeleteDirectory(path string) error {
	return c.Client.RemoveDirectory(path)
}

func (c SFTPClient) List(path string) ([]string, error) {
	exist, err := c.Exist(path)
	if err != nil {
		return nil, err
	} else if !exist {
		return nil, fmt.Errorf("path %s does not exists", path)
	}
	isDir, err := c.IsDir(path)
	if err != nil {
		return nil, err
	}
	if !isDir {
		return nil, fmt.Errorf("path %s is not a dir", path)
	}

	walker := c.Client.Walk(path)
	var files []string
	for walker.Step() {
		if err = walker.Err(); err != nil {
			log.Printf("Error with file: %s | Err: %s\n", walker.Path(), err)
			continue
		}
		if walker.Path() != path {
			files = append(files, walker.Path())
		}
	}
	return files, nil
}

func (c SFTPClient) Exist(path string) (bool, error) {
	_, err := c.Client.Lstat(path)
	if err != nil && err.Error() == "file does not exist" {
		return false, nil
	}
	return err == nil, err
}

func (c SFTPClient) IsDir(path string) (bool, error) {
	lstat, err := c.Client.Lstat(path)
	if err != nil {
		return false, err
	}
	return lstat.IsDir(), nil
}
func (c SFTPClient) IsFile(path string) (bool, error) {
	lstat, err := c.Client.Lstat(path)
	if err != nil {
		return false, err
	}
	return !lstat.IsDir(), nil
}

func (c SFTPClient) Close() error {
	if err := c.Client.Close(); err != nil {
		log.Printf("Unable to close SFTP connection: %s", err.Error())
		return err
	}
	if err := c.conn.Close(); err != nil {
		log.Printf("Unable to close SSH connection: %s", err.Error())
		return err
	}
	return nil
}

func (c SFTPClient) Rename(fname, newName string) error {
	return c.Client.Rename(fname, newName)
}
