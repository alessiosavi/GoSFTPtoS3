package GoSFTPtoS3

import (
	"os"
	"testing"
)

func TestMain(t *testing.M) {
	//setup()
	code := t.Run()
	// shutdown()
	os.Exit(code)
}
