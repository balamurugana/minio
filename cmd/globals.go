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
	"runtime"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/objcache"
)

// minio configuration related constants.
const (
	globalMinioConfigDir          = ".minio"
	globalMinioCertsDir           = "certs"
	globalMinioCertsCADir         = "CAs"
	globalMinioCertFile           = "public.crt"
	globalMinioKeyFile            = "private.key"
	globalMinioConfigFile         = "config.json"
	globalMinioCertExpireWarnDays = time.Hour * 24 * 30 // 30 days.

	globalMinioDefaultRegion       = "us-east-1"
	globalMinioDefaultOwnerID      = "minio"
	globalMinioDefaultStorageClass = "STANDARD"
	globalWindowsOSName            = "windows"
	globalNetBSDOSName             = "netbsd"
	globalSolarisOSName            = "solaris"
	// Add new global values here.
)

const (
	// Limit fields size (except file) to 1Mib since Policy document
	// can reach that size according to https://aws.amazon.com/articles/1434
	maxFormFieldSize = int64(1 * humanize.MiByte)

	// Limit memory allocation to store multipart data
	maxFormMemory = int64(5 * humanize.MiByte)

	// Represents the minimum required RAM size before
	// we enable caching.
	minRAMSize = 8 * humanize.GiByte

	// The maximum allowed difference between the request generation time and the server processing time
	globalMaxSkewTime = 15 * time.Minute

	// Maximum size of internal objects parts
	globalPutPartSize = int64(64 * 1024 * 1024)

	// Keeps the connection active by waiting for following amount of time.
	// Primarily used in ListenBucketNotification.
	globalSNSConnAlive = 5 * time.Second
)

var (
	globalQuiet     = false               // quiet flag set via command line.
	globalConfigDir = mustGetConfigPath() // config-dir flag set via command line
	// Add new global flags here.

	// This flag is set to 'true' when MINIO_BROWSER env is set.
	globalIsEnvBrowser = false

	// Maximum cache size. Defaults to disabled.
	// Caching is enabled only for RAM size > 8GiB.
	globalMaxCacheSize = uint64(0)

	// Cache expiry.
	globalCacheExpiry = objcache.DefaultExpiry

	// Peer communication struct
	globalS3Peers = s3Peers{}

	// List of admin peers.
	globalAdminPeers = adminPeers{}

	// Minio server user agent string.
	globalServerUserAgent = "Minio/" + ReleaseTag + " (" + runtime.GOOS + "; " + runtime.GOARCH + ")"

	// Global server's network statistics
	globalConnStats = newConnStats()

	// Global HTTP request statisitics
	globalHTTPStats = newHTTPStats()

	// Add new variable global values here.
)

// Parse command arguments and set global variables accordingly
func setGlobalsFromContext(c *cli.Context) {
	// Set config dir
	switch {
	case c.IsSet("config-dir"):
		globalConfigDir = c.String("config-dir")
	case c.GlobalIsSet("config-dir"):
		globalConfigDir = c.GlobalString("config-dir")
	}
	if globalConfigDir == "" {
		console.Fatalf("Unable to get config file. Config directory is empty.")
	}

	// Set global quiet flag.
	globalQuiet = c.Bool("quiet") || c.GlobalBool("quiet")
}
