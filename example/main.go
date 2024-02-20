package main

import (
	"github.com/alessiosavi/GoGPUtils/helper"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/alessiosavi/GoSFTPtoS3"
	"log"
	"os"
	"strings"
)

func Test1() {
	var sftpConf = &GoSFTPtoS3.SFTPConf{
		Host:     "test.rebex.net",
		User:     "demo",
		Password: "password",
		Port:     22,
		Timeout:  5,
	}

	var bucket = "bucket-ftp"
	conn, err := sftpConf.NewConn("diffie-hellman-group-exchange-sha256")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	// Init the default configuration and initialize a new session
	if _, err = conn.PutToS3("sftp_folder", bucket, nil, []string{"CUSTOM_PREFIX"}, renameFile); err != nil {
		panic(err)
	}
}

// renameFile is a custom function delegated to rename the file before writing to S3
// If you want to mantain the same file of the s3, just use `return fName`
// In this case, we delete the initial path and return the other part of the file
func renameFile(fName string) string {
	s := strings.Split(fName, "/")
	return stringutils.JoinSeparator("/", s[1:]...)
}

func Test2() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	var sftpConf = &GoSFTPtoS3.SFTPConf{
		Host:     "localhost",
		User:     "alessiosavi",
		Password: "",
		Port:     22,
		Timeout:  50,
		PrivKey:  "",
	}

	file, err := os.ReadFile("/home/alessiosavi/.ssh/mykey.pem")
	if err != nil {
		panic(err)
	}
	sftpConf.PrivKey = string(file)

	conn, err := sftpConf.NewConn()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	list, err := conn.List("/tmp")
	if err != nil {
		panic(err)
	}

	log.Println(helper.MarshalIndent(list))

}

func main() {
	Test1()
	Test2()
}
