package main

import (
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/alessiosavi/GoSFTPtoS3"
	"strings"
)

func main1() {
	var sftpConf = &GoSFTPtoS3.SFTPConf{
		Host:     "test.rebex.net",
		User:     "demo",
		Password: "password",
		Port:     22,
		Bucket:   "bucket-ftp",
		Timeout:  5,
	}

	conn, err := sftpConf.NewConn("diffie-hellman-group-exchange-sha256")
	if err != nil {
		panic(err)
	}
	// Init the default configuration and initialize a new session
	if err = conn.PutToS3("", "CUSTOM_PREFIX", nil, renameFile); err != nil {
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

func main() {
	var sftpConf = &GoSFTPtoS3.SFTPConf{
		Host:     "172.26.112.1",
		User:     "root",
		Password: "",
		Port:     22,
		Bucket:   "bucket-ftp",
		Timeout:  5,
		PrivKey:  "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAvDirtyEqTX3eqYSlPMZ2mkg5s7BZQa6Mzl2pl80avNYK2vvU\nb8EVy1g8h76iddIe84S7kcSJrRCvJxp9G71H6g+asIoUpquVqhE3O0vMl28SjHPY\nrBV+CvXsskCTn5h+F44Q9hntdDapiYWSSeSVvpTRyDAYrhck+YLMnWralD2EeiRG\nrRc954+2Fz8x4tU04ys5mEAG/0bYhHgQdaLVUi9/tKaK2lETmlzXsYmZPQSiO4hI\niDFUvkG+NgL7AdH89KP5gSJjPrRUoEvrlrDpKZ+SES+ZepKt/14E7VofEBqpIN/N\nOSfgeh0B6tw6dJjOebVJR0D8eAEAECnQWby8nwIDAQABAoIBAGZdv+5CSFqK2V2C\nucda/Mgd/dvfTjvtrcDSqdjYgPEwzAibK8d30N2d6JW/NWY02AaDKuw1YtdQGqJ+\nwooioIkI4Y2gG2kmKqiq4koKiIPXsdPXcYDt03YsmAW+H0uOSQ2Qg2MFaOJqlDwp\n/AzaGCLll05z8ghEqCRXYec/ZQ93kWD2FXAIgB/sKOlcj1FFxLEd46eFH+hJEdwo\n7IGGoVVoAUGnfG3qGkHIDyHOR56IcbcVDvc7iGFB77i6+xaECkuv+drHJKLae5GP\nXtHBv5/+SxF+ioBAFGhmd+uR44UIYSyIQc6Gmz41te1BTLGcC3NmFESBCNHUMV6k\nez4HkgkCgYEA9XkzgpuHftWIXWz8F94ko2wtR9wgnsk8JmCIJkcIUbwCXvNfj37c\n4bt7nj8na/Nb7rktdNeBBpzgyp5Q/EL9XkoSYOPvGxvov7xPL6hBchRW9MAdswYW\nz4jGo+Phq/h1mj23oqiisG5Sw5Y8GI8KuT3QLo7UGzOUAG1msg/P6OsCgYEAxEr1\nVLv/sGpZC+601hMaoyCDMU9ZCPpBlQzeenx+uf/vCgD42hBx5wRkDvbZ/yvaEiAh\ne5jEl0UFupThx83ek2uzJevNxy1WittGh6Z23g7igLfYQxKe0EKkctd7mht876bo\niu52VhXEQJ86EImjKaSaN3A57u3Xtlkx0DS3jh0CgYAcs8pJGER0mSUzv12OsOPZ\n0/lLcLrDtkX5OspQp3eajwA79/sRfUT4hAKFU18ZqT/znuVoxxYIHunN50sS5AZa\nMxEJET3RDqddW/hqWyMj4qr0PU57s1eRdq27Vhb6E7g0i8jgFRXIyW2V/wgR2w/m\n7wpbl0nH0HUaw7ABXAX0dwKBgDxwZBeiBzl3lVFyP2YG3dXKhfqh1uVHwPe2za3j\nNVXp8t0erYDfPWMHXBOreDX0d6HLGOQohqeZDgmEG/zca6Lyr2eGsoaYdCQvHglY\nfRMkfCNr9/+29QVk76OYq6souZBE0ScuA2vAKfxHyqYa6w1AbGeTe5MQ0rGHxym3\ngakVAoGBAI7My0ZgRXhdmNmXcFZfqio/yxylOYHEaPkGdAiHauA0EF2CP22ySLz0\n/Cqh87lBimy33bo+7+JBRF9RJ3HqCNqTiWWl/T7gSn+1B8Ou6MIn74vT0HuGS3UV\nZG/np5zwtTAtXJzL0j6ioGEBbdFz3NucZHCVb3BessT+8em9n5yK\n-----END RSA PRIVATE KEY-----",
	}

	_, err := sftpConf.NewConn("diffie-hellman-group-exchange-sha256")
	if err != nil {
		panic(err)
	}


}
