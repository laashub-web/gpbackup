package helper

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Non-flag variables
 */

var (
	CleanupGroup  *sync.WaitGroup
	currentPipe   string
	errBuf        bytes.Buffer
	lastPipe      string
	nextPipe      string
	version       string
	wasTerminated bool
	writeHandle   *os.File
	writer        *bufio.Writer
)

/*
 * Command-line flags
 */
var (
	backupAgent      *bool
	compressionLevel *int
	content          *int
	dataFile         *string
	oidFile          *string
	onErrorContinue  *bool
	pipeFile         *string
	pluginConfigFile *string
	printVersion     *bool
	restoreAgent     *bool
	tocFile          *string
	isFilter		 *bool
)

func DoHelper() {
	var err error
	defer func() {
		if wasTerminated {
			CleanupGroup.Wait()
			return
		}
		DoCleanup()
		os.Exit(gplog.GetErrorCode())
	}()

	InitializeGlobals()
	// Initialize signal handler
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range signalChan {
			fmt.Println() // Add newline after "^C" is printed
			gplog.Warn("Received a termination signal, aborting helper agent on segment %d", *content)
			wasTerminated = true
			DoCleanup()
			os.Exit(2)
		}
	}()

	if *backupAgent {
		err = doBackupAgent()
	} else if *restoreAgent {
		err = doRestoreAgent()
	}
	if err != nil {
		gplog.Error(fmt.Sprintf("%v: %s", err, debug.Stack()))
		handle, _ := utils.OpenFileForWrite(fmt.Sprintf("%s_error", *pipeFile))
		_ = handle.Close()
	}
}

func InitializeGlobals() {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gpbackup_helper", "")

	backupAgent = flag.Bool("backup-agent", false, "Use gpbackup_helper as an agent for backup")
	content = flag.Int("content", -2, "Content ID of the corresponding segment")
	compressionLevel = flag.Int("compression-level", 0, "The level of compression to use with gzip. O indicates no compression.")
	dataFile = flag.String("data-file", "", "Absolute path to the data file")
	oidFile = flag.String("oid-file", "", "Absolute path to the file containing a list of oids to restore")
	onErrorContinue = flag.Bool("on-error-continue", false, "Continue restore even when encountering an error")
	pipeFile = flag.String("pipe-file", "", "Absolute path to the pipe file")
	pluginConfigFile = flag.String("plugin-config", "", "The configuration file to use for a plugin")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	restoreAgent = flag.Bool("restore-agent", false, "Use gpbackup_helper as an agent for restore")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	isFilter = flag.Bool("with-filters", false, "Used with table/schema filters")

	if *onErrorContinue && !*restoreAgent {
		fmt.Printf("--on-error-continue flag can only be used with --restore-agent flag")
		os.Exit(1)
	}

	flag.Parse()
	if *printVersion {
		fmt.Printf("gpbackup_helper version %s\n", version)
		os.Exit(0)
	}
	operating.InitializeSystemFunctions()
}

/*
 * Shared functions
 */

func createPipe(pipe string) error {
	err := syscall.Mkfifo(pipe, 0777)
	return err
}

func getOidListFromFile() ([]int, error) {
	oidStr, err := operating.System.ReadFile(*oidFile)
	if err != nil {
		return nil, err
	}
	oidStrList := strings.Split(strings.TrimSpace(fmt.Sprintf("%s", oidStr)), "\n")
	oidList := make([]int, len(oidStrList))
	for i, oid := range oidStrList {
		num, _ := strconv.Atoi(oid)
		oidList[i] = num
	}
	sort.Ints(oidList)
	return oidList, nil
}

func flushAndCloseRestoreWriter() error {
	if writer != nil {
		err := writer.Flush()
		if err != nil {
			return err
		}
		writer = nil
	}
	if writeHandle != nil {
		err := writeHandle.Close()
		if err != nil {
			return err
		}
		writeHandle = nil
	}
	return nil
}


/*
 * Shared helper functions
 */

func DoCleanup() {
	defer CleanupGroup.Done()
	if wasTerminated {
		/*
		 * If the agent dies during the last table copy, it can still report
		 * success, so we create an error file and check for its presence in
		 * gprestore after the COPYs are finished.
		 */
		handle, _ := utils.OpenFileForWrite(fmt.Sprintf("%s_error", *pipeFile))
		_ = handle.Close()
	}
	err := flushAndCloseRestoreWriter()
	if err != nil {
		log("Encountered error during cleanup: %v", err)
	}
	err = utils.RemoveFileIfExists(lastPipe)
	if err != nil {
		log("Encountered error during cleanup: %v", err)
	}
	err = utils.RemoveFileIfExists(currentPipe)
	if err != nil {
		log("Encountered error during cleanup: %v", err)
	}
	err = utils.RemoveFileIfExists(nextPipe)
	if err != nil {
		log("Encountered error during cleanup: %v", err)
	}
	log("Cleanup complete")
}

func log(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	gplog.Verbose(s, v...)
}

func logError(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	gplog.Error(s, v...)
}
