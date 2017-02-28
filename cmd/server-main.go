/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"runtime"

	"github.com/Sirupsen/logrus"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: "Bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname.",
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "Start object storage server.",
	Flags:  append(serverFlags, globalFlags...),
	Action: mainServer,
	CustomHelpTemplate: `NAME:
 {{.HelpName}} - {{.Usage}}

USAGE:
 {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}PATH [PATH...]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Custom username or access key of 5 to 20 characters in length.
     MINIO_SECRET_KEY: Custom password or secret key of 8 to 40 characters in length.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

EXAMPLES:
  1. Start minio server on "/home/shared" directory.
      $ {{.HelpName}} /home/shared

  2. Start minio server bound to a specific ADDRESS:PORT.
      $ {{.HelpName}} --address 192.168.1.101:9000 /home/shared

  3. Start erasure coded minio server on a 12 disks server.
      $ {{.HelpName}} /mnt/export1/ /mnt/export2/ /mnt/export3/ /mnt/export4/ \
          /mnt/export5/ /mnt/export6/ /mnt/export7/ /mnt/export8/ /mnt/export9/ \
          /mnt/export10/ /mnt/export11/ /mnt/export12/

  4. Start erasure coded distributed minio server on a 4 node setup with 1 drive each. Run following commands on all the 4 nodes.
      $ export MINIO_ACCESS_KEY=minio
      $ export MINIO_SECRET_KEY=miniostorage
      $ {{.HelpName}} http://192.168.1.11/mnt/export/ http://192.168.1.12/mnt/export/ \
          http://192.168.1.13/mnt/export/ http://192.168.1.14/mnt/export/
`,
}

type serverCmdConfig struct {
	serverAddr string
	endpoints  []*url.URL
}

// Parse an array of end-points (from the command line)
func parseStorageEndpoints(eps []string) (endpoints []*url.URL, err error) {
	for _, ep := range eps {
		if ep == "" {
			return nil, errInvalidArgument
		}
		var u *url.URL
		u, err = url.Parse(ep)
		if err != nil {
			return nil, err
		}
		if u.Host != "" {
			_, port, err := net.SplitHostPort(u.Host)
			// Ignore the missing port error as the default port can be setup.serverAddr.
			if err != nil && !strings.Contains(err.Error(), "missing port in address") {
				return nil, err
			}

			if setup.serverAddr == "" {
				// For ex.: minio server host1:port1 host2:port2...
				// we return error as port is configurable only
				// using "--address :port"
				if port != "" {
					return nil, fmt.Errorf("Invalid Argument %s, port configurable using --address :<port>", u.Host)
				}
				u.Host = net.JoinHostPort(u.Host, setup.serverAddr)
			} else {
				// For ex.: minio server --address host:port host1:port1 host2:port2...
				// i.e if "--address host:port" is specified
				// port info in u.Host is mandatory else return error.
				if port == "" {
					return nil, fmt.Errorf("Invalid Argument %s, port mandatory when --address <host>:<port> is used", u.Host)
				}
			}
		}
		endpoints = append(endpoints, u)
	}
	return endpoints, nil
}

// initServer initialize server config.
func initServerConfig(c *cli.Context) {
	// Initialization such as config generating/loading config, enable logging, ..
	minioInit(c)

	// Set maxOpenFiles, This is necessary since default operating
	// system limits of 1024, 2048 are not enough for Minio server.
	setMaxOpenFiles()

	// Set maxMemory, This is necessary since default operating
	// system limits might be changed and we need to make sure we
	// do not crash the server so the set the maxCacheSize appropriately.
	setMaxMemory()

	// Do not fail if this is not allowed, lower limits are fine as well.
}

// Validate if input disks are sufficient for initializing XL.
func checkSufficientDisks(eps []*url.URL) error {
	// Verify total number of disks.
	total := len(eps)
	if total > maxErasureBlocks {
		return errXLMaxDisks
	}
	if total < minErasureBlocks {
		return errXLMinDisks
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// Verify if we have even number of disks.
	// only combination of 4, 6, 8, 10, 12, 14, 16 are supported.
	if !isEven(total) {
		return errXLNumDisks
	}

	// Success.
	return nil
}

// Returns if slice of disks is a distributed setup.
func isDistributedSetup(eps []*url.URL) bool {
	// Validate if one the disks is not local.
	for _, ep := range eps {
		if !isLocalStorage(ep) {
			// One or more disks supplied as arguments are
			// not attached to the local node.
			return true
		}
	}
	return false
}

// Returns true if path is empty, or equals to '.', '/', '\' characters.
func isPathSentinel(path string) bool {
	return path == "" || path == "." || path == "/" || path == `\`
}

// Returned when path is empty or root path.
var errEmptyRootPath = errors.New("Empty or root path is not allowed")

// Invalid scheme passed.
var errInvalidScheme = errors.New("Invalid scheme")

// Check if endpoint is in expected syntax by valid scheme/path across all platforms.
func checkEndpointURL(endpointURL *url.URL) (err error) {
	// Applicable to all OS.
	if endpointURL.Scheme == "" || endpointURL.Scheme == httpScheme || endpointURL.Scheme == httpsScheme {
		if isPathSentinel(path.Clean(endpointURL.Path)) {
			err = errEmptyRootPath
		}

		return err
	}

	// Applicable to Windows only.
	if runtime.GOOS == globalWindowsOSName {
		// On Windows, endpoint can be a path with drive eg. C:\Export and its URL.Scheme is 'C'.
		// Check if URL.Scheme is a single letter alphabet to represent a drive.
		// Note: URL.Parse() converts scheme into lower case always.
		if len(endpointURL.Scheme) == 1 && endpointURL.Scheme[0] >= 'a' && endpointURL.Scheme[0] <= 'z' {
			// If endpoint is C:\ or C:\export, URL.Path does not have path information like \ or \export
			// hence we directly work with endpoint.
			if isPathSentinel(strings.SplitN(path.Clean(endpointURL.String()), ":", 2)[1]) {
				err = errEmptyRootPath
			}

			return err
		}
	}

	return errInvalidScheme
}

// Check if endpoints are in expected syntax by valid scheme/path across all platforms.
func checkEndpointsSyntax(eps []*url.URL, disks []string) error {
	for i, u := range eps {
		if err := checkEndpointURL(u); err != nil {
			return fmt.Errorf("%s: %s (%s)", err.Error(), u.Path, disks[i])
		}
	}

	return nil
}

// Make sure all the command line parameters are OK and exit in case of invalid parameters.
func checkServerSyntax(c *cli.Context) {
	serverAddr := c.String("address")

	host, portStr, err := net.SplitHostPort(serverAddr)
	fatalIf(err, "Unable to parse %s.", serverAddr)

	// Verify syntax for all the XL disks.
	disks := c.Args()

	// Parse disks check if they comply with expected URI style.
	endpoints, err := parseStorageEndpoints(disks)
	fatalIf(err, "Unable to parse storage endpoints %s", strings.Join(disks, " "))

	// Validate if endpoints follow the expected syntax.
	err = checkEndpointsSyntax(endpoints, disks)
	fatalIf(err, "Invalid endpoints found %s", strings.Join(disks, " "))

	// Validate for duplicate endpoints are supplied.
	err = checkDuplicateEndpoints(endpoints)
	fatalIf(err, "Duplicate entries in %s", strings.Join(disks, " "))

	if len(endpoints) > 1 {
		// Validate if we have sufficient disks for XL setup.
		err = checkSufficientDisks(endpoints)
		fatalIf(err, "Insufficient number of disks.")
	} else {
		// Validate if we have invalid disk for FS setup.
		if endpoints[0].Host != "" && endpoints[0].Scheme != "" {
			fatalIf(errInvalidArgument, "%s, FS setup expects a filesystem path", endpoints[0])
		}
	}

	if !isDistributedSetup(endpoints) {
		// for FS and singlenode-XL validation is done, return.
		return
	}

	// Rest of the checks applies only to distributed XL setup.
	if host != "" {
		// We are here implies --address host:port is passed, hence the user is trying
		// to run one minio process per export disk.
		if portStr == "" {
			fatalIf(errInvalidArgument, "Port missing, Host:Port should be specified for --address")
		}
		foundCnt := 0
		for _, ep := range endpoints {
			if ep.Host == serverAddr {
				foundCnt++
			}
		}
		if foundCnt == 0 {
			// --address host:port should be available in the XL disk list.
			fatalIf(errInvalidArgument, "%s is not available in %s", serverAddr, strings.Join(disks, " "))
		}
		if foundCnt > 1 {
			// --address host:port should match exactly one entry in the XL disk list.
			fatalIf(errInvalidArgument, "%s matches % entries in %s", serverAddr, foundCnt, strings.Join(disks, " "))
		}
	}

	for _, ep := range endpoints {
		if ep.Scheme == httpsScheme && !setup.secureConn {
			// Certificates should be provided for https configuration.
			fatalIf(errInvalidArgument, "Certificates not provided for secure configuration")
		}
	}
}

// Checks if any of the endpoints supplied is local to this server.
func isAnyEndpointLocal(eps []*url.URL) bool {
	anyLocalEp := false
	for _, ep := range eps {
		if isLocalStorage(ep) {
			anyLocalEp = true
			break
		}
	}
	return anyLocalEp
}

// Returned when there are no ports.
var errEmptyPort = errors.New("Port cannot be empty or '0', please use `--address` to pick a specific port")

// Convert an input address of form host:port into, host and port, returns if any.
func getHostPort(address string) (host, port string, err error) {
	// Check if requested port is available.
	host, port, err = net.SplitHostPort(address)
	if err != nil {
		return "", "", err
	}

	// Empty ports.
	if port == "0" || port == "" {
		// Port zero or empty means use requested to choose any freely available
		// port. Avoid this since it won't work with any configured clients,
		// can lead to serious loss of availability.
		return "", "", errEmptyPort
	}

	// Parse port.
	if _, err = strconv.Atoi(port); err != nil {
		return "", "", err
	}

	if runtime.GOOS == "darwin" {
		// On macOS, if a process already listens on 127.0.0.1:PORT, net.Listen() falls back
		// to IPv6 address ie minio will start listening on IPv6 address whereas another
		// (non-)minio process is listening on IPv4 of given port.
		// To avoid this error sutiation we check for port availability only for macOS.
		if err = checkPortAvailability(port); err != nil {
			return "", "", err
		}
	}

	// Success.
	return host, port, nil
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(srvCmdCfg serverCmdConfig) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.
	isFS := len(srvCmdCfg.endpoints) == 1
	if isFS {
		// Unescape is needed for some UNC paths on windows
		// which are of this form \\127.0.0.1\\export\test.
		var fsPath string
		fsPath, err = url.QueryUnescape(srvCmdCfg.endpoints[0].String())
		if err != nil {
			return nil, err
		}

		// Initialize new FS object layer.
		newObject, err = newFSObjectLayer(fsPath)
		if err != nil {
			return nil, err
		}

		// FS initialized, return.
		return newObject, nil
	}

	// First disk argument check if it is local.
	firstDisk := isLocalStorage(srvCmdCfg.endpoints[0])

	// Initialize storage disks.
	storageDisks, err := initStorageDisks(setup)
	if err != nil {
		return nil, err
	}

	// Wait for formatting disks for XL backend.
	var formattedDisks []StorageAPI
	formattedDisks, err = waitForFormatXLDisks(firstDisk, srvCmdCfg.endpoints, storageDisks)
	if err != nil {
		return nil, err
	}

	// Cleanup objects that weren't successfully written into the namespace.
	if err = houseKeeping(storageDisks); err != nil {
		return nil, err
	}

	// Once XL formatted, initialize object layer.
	newObject, err = newXLObjectLayer(formattedDisks)
	if err != nil {
		return nil, err
	}

	// XL initialized, return.
	return newObject, nil
}

///
/// new setup
///
func checkConfigDir(configDir string) error {
	fi, err := os.Stat(configDir)
	if err == nil {
		if fi.IsDir() {
			return nil
		}

		return fmt.Errorf("`%s' is not a directory", configDir)
	}

	if !os.IsNotExist(err) {
		return err
	}

	// As configDir does not exist, we will create it later, but check if its parent directory exists.
	parentConfigDir := filepath.Dir(configDir)
	fi, err = os.Stat(parentConfigDir)
	if err != nil {
		return err
	}

	if !fi.IsDir() {
		return fmt.Errorf("`%s' is not a directory", parentConfigDir)
	}

	return nil
}

func mkConfigDir(configDir string) (err error) {
	if err = os.Mkdir(configDir, 0755); err != nil && !os.IsExist(err) {
		// Ignore if directory already exists
		return err
	}

	certsDir := filepath.Join(configDir, "certs", "CAs")
	if err = mkdirAll(certsDir, 0700); os.IsExist(err) {
		// Ignore if directory already exists
		err = nil
	}

	return err
}

func getServerConfig(configDir string, envCred credential) (serverConfig *serverConfigV14New, cred credential, err error) {
	cred = envCred
	configFile := filepath.Join(configDir, "config.json")
	fi, err := os.Stat(configFile)

	if err == nil {
		if !fi.Mode().IsRegular() {
			err = fmt.Errorf("config file `%s' is not a file", configFile)
			return serverConfig, cred, err
		}

		if err = migrateConfig(configFile); err != nil {
			return serverConfig, cred, err
		}

		if serverConfig, err = loadServerConfig(configFile); err != nil {
			err = fmt.Errorf("unable to load configuration file %s. %s", configFile, err)
			return serverConfig, cred, err
		}

		if !cred.IsValid() {
			cred = setup.serverConfig.GetCredential()
			// Create freshly to generate secret hash key
			cred = createCredential(cred.AccessKey, cred.SecretKey)
		}
	} else if os.IsNotExist(err) {
		if !cred.IsValid() {
			cred = newCredential()
		}

		browser := "on"
		if setup.isBrowserDisabled {
			browser = "off"
		}
		serverConfig = newServerConfig(cred, browser)
		if err = setup.serverConfig.Save(configFile); err != nil {
			err = fmt.Errorf("unable to initialize configuration file %s. %s", configFile, err)
		}
	}

	return serverConfig, cred, err
}

var setup Setup

// mainServer handler called for 'minio server' command.
func mainServer(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "server", -1)
	}

	quiet := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	quietPrintf := func(format string, args ...interface{}) {
		if !quiet {
			console.Printf(format, args...)
		}
	}

	quietPrintf("Validating command line arguments and environment variables\n")

	configDir := ctx.String("config-dir")
	if err := checkConfigDir(configDir); err != nil {
		quietPrintf("%s\n", err)
		os.Exit(-1)
	}

	serverAddr := ctx.String("address")
	err := CheckLocalServerAddr(serverAddr)
	if err != nil {
		quietPrintf("server address `%s': %s\n", serverAddr, err)
		os.Exit(-1)
	}

	args := ctx.Args()
	setup, err = NewSetup(serverAddr, args...)
	if err != nil {
		quietPrintf("%s\n", err)
		os.Exit(-1)
	}

	profileMode := os.Getenv("_MINIO_PROFILER")
	if err = checkProfileMode(profileMode); err != nil {
		quietPrintf("%s\n", err)
		os.Exit(-1)
	}

	isCacheDisabled := strings.EqualFold(os.Getenv("_MINIO_CACHE"), "off")
	isBrowserDisabled := strings.EqualFold(os.Getenv("MINIO_BROWSER"), "off")

	envAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	if envAccessKey != "" {
		if !isAccessKeyValid(envAccessKey) {
			quietPrintf("invalid access key in environment variable MINIO_ACCESS_KEY. %s\n", err)
			os.Exit(-1)
		}
	}

	envSecretKey := os.Getenv("MINIO_SECRET_KEY")
	if envSecretKey != "" {
		if !isSecretKeyValid(envSecretKey) {
			quietPrintf("invalid secret key in environment variable MINIO_SECRET_KEY. %s\n", err)
			os.Exit(-1)
		}
	}

	if envAccessKey != "" && envSecretKey != "" {
		setup.cred = createCredential(envAccessKey, envSecretKey)
		setup.isEnvCred = true
	}

	quietPrintf("Checking SSL certificates\n")
	setup.publicCrtFile = filepath.Join(configDir, "certs", "public.crt")
	setup.privateKeyFile = filepath.Join(configDir, "certs", "private.key")
	if isFile(setup.publicCrtFile) && isFile(setup.privateKeyFile) {
		if setup.publicCerts, err = parsePublicCertFile(setup.publicCrtFile); err != nil {
			quietPrintf("Unable to parse public certificate file `%s'. %s\n", setup.publicCrtFile, err)
			os.Exit(1)
		}

		certsCAsDir := filepath.Join(configDir, "certs", "CAs")
		if setup.rootCAs, err = getRootCAs(certsCAsDir); err != nil {
			quietPrintf("Unable to load CA files in `%s'. %s\n", certsCAsDir, err)
			os.Exit(1)
		}
		quietPrintf("HTTPS Server will be run\n")
		setup.secureConn = true
	} else {
		quietPrintf("No SSL certificates found.  HTTP Server will be run\n")
		setup.secureConn = false
	}

	setup.configDir = configDir
	setup.profileMode = profileMode
	setup.isCacheDisabled = isCacheDisabled
	setup.isBrowserDisabled = isBrowserDisabled

	if setup.secureConn {
		setup.endpoints.SetSSL()
	} else {
		setup.endpoints.SetNonSSL()
	}

	// Check for new update
	if !quiet {
		quietPrintf("Checking for new updates\n")
		if older, downloadURL, uerr := getUpdateInfo(1 * time.Second); uerr == nil && older > time.Duration(0) {
			quietPrintf(colorizeUpdateMessage(downloadURL, older))
		}
	}

	// Load or migrate config file or create config file
	if err = mkConfigDir(configDir); err != nil {
		quietPrintf("%v\n", err)
		os.Exit(1)
	}

	if setup.serverConfig, setup.cred, err = getServerConfig(configDir, setup.cred); err != nil {
		quietPrintf("%v\n", err)
		os.Exit(1)
	}

	setup.profiler = startProfiler(profileMode)

	fmt.Printf("%+v\n", setup)

	quietPrintf("Initialize loggers\n")
	clogger := setup.serverConfig.Logger.GetConsole()
	if clogger.Enable {
		level, err := logrus.ParseLevel(clogger.Level)
		if err != nil {
			level = logrus.ErrorLevel
			quietPrintf("Unknown log level `%s' found in Logger.Console in config file. Fallback to %s\n", clogger.Level, level)
		}

		logger := logrus.New()
		logger.Level = level
		logger.Formatter = new(logrus.TextFormatter)
		log.loggers = append(log.loggers, logger)
	}

	flogger := setup.serverConfig.Logger.GetFile()
	if flogger.Enable {
		if flogger.Filename == "" {
			quietPrintf("No file is given. Ignoring Logger.File\n")
		} else {
			level, err := logrus.ParseLevel(flogger.Level)
			if err != nil {
				quietPrintf("Unknown log level `%s' found in Logger.File in config file. Ignoring Logger.File\n")
			} else {
				// Creates the named file with mode 0666, honors system umask.
				file, err := os.OpenFile(flogger.Filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
				if err != nil {
					quietPrintf("Unable to open log file `%s'. Ignoring Logger.File\n", flogger.Filename)
				} else {
					logger := logrus.New()
					logger.Level = level
					logger.Formatter = new(logrus.JSONFormatter)
					logger.Hooks.Add(&localFile{file})
					logger.Out = ioutil.Discard
					log.loggers = append(log.loggers, logger)
				}
			}
		}
	}

	// Init the error tracing module.
	initError()

	// Init lock subsystem.
	initLockSystem(setup)

	handler, err := newServerHandler(setup)
	fatalIf(err, "Unable to initialize RPC services.")

	// Initialize a new HTTP server.
	apiServer := NewServerMux(setup.serverAddr, handler)

	initS3Peers(setup)
	initAdminPeers(setup)

	// Run api server in background.
	go func() {
		var publicCrtFile, privateKeyFile string
		if setup.secureConn {
			publicCrtFile, privateKeyFile = setup.publicCrtFile, setup.privateKeyFile
		}

		serr := apiServer.ListenAndServe(publicCrtFile, privateKeyFile)
		fatalIf(serr, "Failed to start HTTP server.")
	}()

	objectLayer, err := initObjectLayer(setup)
	fatalIf(err, "Initializing object layer failed")

	globalObjLayerMutex.Lock()
	globalObjectAPI = objectLayer
	globalObjLayerMutex.Unlock()

	// Prints the formatted startup message once object layer is initialized.
	fmt.Printf("%+v\n", setup)
	// printStartupMessage(apiEndPoints)

	// Set uptime time after object layer has initialized.
	setup.bootTime = time.Now().UTC()

	// Waits on the server.
	<-globalServiceDoneCh
}

func isFile(name string) bool {
	fi, err := os.Stat(name)
	// Return if no error and it is a regular file.
	return (err == nil && fi.Mode().IsRegular())
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func initObjectLayer(setup Setup) (objectLayer ObjectLayer, err error) {
	if setup.setupType == FSSetupType {
		objectLayer, err = newFSObjectLayer(setup.endpoints[0].Value)
		if err != nil {
			return nil, err
		}

		// Initialize and load bucket policies.
		if err = initBucketPolicies(objectLayer); err != nil {
			return nil, fmt.Errorf("Unable to load all bucket policies. %s", err)
		}

		// Initialize a new event notifier.
		if err = initEventNotifier2(setup, objectLayer); err != nil {
			return nil, fmt.Errorf("Unable to initialize event notification. %s", err)
		}

		return objectLayer, err
	}

	storageDisks := make([]StorageAPI, len(setup.endpoints))
	for index, endpoint := range setup.endpoints {
		var storage StorageAPI
		if setup.setupType == XLSetupType {
			storage, err = newPosix(endpoint.Value)
		} else if endpoint.IsLocal {
			storage, err = newPosix(endpoint.URL.Path)
		} else {
			storage, err = newStorageRPCClient(endpoint, setup.cred, setup.secureConn)
		}
		// Intentionally ignore disk not found errors. XL is designed
		// to handle these errors internally.
		if err != nil && err != errDiskNotFound {
			return nil, err
		}

		storageDisks[index] = storage
	}

	// // First disk argument check if it is local.
	// firstDisk := setup.endpoints[0].IsLocal

	// // Wait for formatting disks for XL backend.
	// var formattedDisks []StorageAPI
	// formattedDisks, err = waitForFormatXLDisks(firstDisk, setup.endpoints, storageDisks)
	// if err != nil {
	// 	return nil, err
	// }

	// Cleanup objects that weren't successfully written into the namespace.
	if err = houseKeeping(storageDisks); err != nil {
		return nil, err
	}

	// Once XL formatted, initialize object layer.
	return newXLObjectLayer(storageDisks)
}
