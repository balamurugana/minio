/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/minio/minio/cmd/format"
)

// Initialize storage disks based on input arguments.
func initStorageDisks(endpoints EndpointList) ([]StorageAPI, error) {
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(endpoints))
	for index, endpoint := range endpoints {
		storage, err := newStorageAPI(endpoint)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
		storageDisks[index] = storage
	}
	return storageDisks, nil
}

// relinquishes the underlying connection for all storage disks.
func closeStorageDisks(storageDisks []StorageAPI) {
	for _, disk := range storageDisks {
		if disk == nil {
			continue
		}
		disk.Close()
	}
}

func readFormatXL(disk StorageAPI) (*format.XLV3, error) {
	data, err := disk.ReadAll(minioMetaBucket, formatConfigFile)
	if err != nil {
		if err != errFileNotFound && err != errVolumeNotFound {
			return nil, err
		}

		// As empty format.json found, check whether any user data present.
		vols, err := disk.ListVols()
		if err != nil {
			return nil, err
		}

		// No user data is allowed except .minio.sys or lost+found.
		if len(vols) > 1 || (len(vols) == 1 && vols[0].Name != minioMetaBucket && vols[0].Name != "lost+found") {
			return nil, errCorruptedFormat
		}

		// Return empty format.XLV3
		return nil, nil
	}

	var f format.XLV3
	if err = json.Unmarshal(data, &f); err != nil {
		return nil, err
	}

	if !f.IsEmpty() {
		if err := f.Validate(); err != nil {
			return nil, err
		}
	}

	return &f, nil
}

func readFormatXLs(storageDisks []StorageAPI, f *format.XLV3, emptyCheck bool) (formats []*format.XLV3, healRequired bool) {
	disks := storageDisks

	formats = make([]*format.XLV3, len(disks))

	// Closure reads format.json from all disks in parallel.
	readFormats := func() []error {
		errs := make([]error, len(disks))

		var wg sync.WaitGroup
		for i := range disks {
			if disks[i] == nil {
				continue
			}

			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				if formats[i], errs[i] = readFormatXL(disks[i]); errs[i] != nil {
					return
				}

				if formats[i].IsEmpty() {
					return
				}

				if formats[i].Index() != i {
					errs[i] = fmt.Errorf("%v contains wrong index in format.json.  expected: %v, got: %v", disks[i], i, f.Index())
					return
				}

				if !f.Match(*formats[i]) {
					errs[i] = fmt.Errorf("format.json does not match in %v", disks[i])
					return
				}
			}(i)
		}
		wg.Wait()

		return errs
	}

	timer := time.NewTimer(1 * time.Minute)
	defer timer.Stop()

	successCount := 0
	for {
		errs := readFormats()
		disks = make([]StorageAPI, len(storageDisks))
		for i := range storageDisks {
			if errs[i] != nil {
				if emptyCheck && !isNetError(errs[i]) {
					return formats, true
				}

				disks[i] = storageDisks[i]
				continue
			}

			if emptyCheck {
				if formats[i] != nil {
					return formats, true
				}

				successCount++
				continue
			}

			if formats[i] != nil {
				successCount++
			} else {
				disks[i] = storageDisks[i]
			}
		}

		if successCount == len(storageDisks) {
			break
		}

		if !emptyCheck && successCount >= len(storageDisks)/2 {
			break
		}

		log.Printf("Waiting for valid format.json from %v endpoints\n", len(storageDisks)-successCount)

		timer.Reset(1 * time.Minute)
		select {
		case <-globalServiceDoneCh:
			break
		case <-timer.C:
		}
	}

	return formats, false
}

var initMetaVolIgnoredErrs = append(baseIgnoredErrs, errVolumeExists)

func writeFormatXL(disk StorageAPI, f *format.XLV3) error {
	// Create .minio.sys.
	if err := disk.MakeVol(minioMetaBucket); err != nil {
		if !IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}

	// Create .minio.sys/tmp.
	if err := disk.MakeVol(minioMetaTmpBucket); err != nil {
		if !IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}

	// Create .minio.sys/multipart.
	if err := disk.MakeVol(minioMetaMultipartBucket); err != nil {
		if !IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}

	data, err := json.Marshal(f)
	if err != nil {
		return err
	}

	if err = disk.DeleteFile(minioMetaBucket, formatConfigFileTmp); err != nil {
		if err != errFileNotFound && err != errVolumeNotFound {
			return err
		}
	}

	if err := disk.AppendFile(minioMetaBucket, formatConfigFileTmp, data); err != nil {
		return err
	}

	// Rename format.json.tmp to format.json.
	return disk.RenameFile(minioMetaBucket, formatConfigFileTmp, minioMetaBucket, formatConfigFile)
}

func writeFormatXLs(storageDisks []StorageAPI, formats []*format.XLV3) {
	disks := storageDisks

	if len(disks) != len(formats) {
		panic(fmt.Errorf("disk count %v and format count %v should be equal", len(disks), len(formats)))
	}

	errs := make([]error, len(storageDisks))

	timer := time.NewTimer(1 * time.Minute)
	defer timer.Stop()

	successCount := 0

	netError := false

	for {
		var wg sync.WaitGroup
		for i := range disks {
			if disks[i] == nil {
				continue
			}

			wg.Add(1)
			go func(i int) {
				errs[i] = writeFormatXL(disks[i], formats[i])
				wg.Done()
			}(i)
		}
		wg.Wait()

		disks = make([]StorageAPI, len(storageDisks))
		for i := range errs {
			if errs[i] != nil {
				if isNetError(errs[i]) && !netError {
					netError = true
				}

				disks[i] = storageDisks[i]
			} else {
				successCount++
			}
		}

		if successCount == len(storageDisks) {
			break
		}

		log.Printf("Retrying to write format.json to %v endpoints\n", len(storageDisks)-successCount)

		timer.Reset(1 * time.Minute)
		select {
		case <-globalServiceDoneCh:
			break
		case <-timer.C:
		}
	}
}

func waitToFormat(isFirstEndpoint bool) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		if isFirstEndpoint {
			log.Printf("waiting for admin to format endpoint %v by running 'mc admin heal' command\n", globalEndpoints[0])
		} else {
			log.Printf("waiting for %v to format\n", globalEndpoints[0].Host)
		}

		doneCh := getFormatDoneCh()
		select {
		case <-globalServiceDoneCh:
			return
		case <-doneCh:
			return
		case <-ticker.C:
		}
	}
}

// initFormatXL
// * Empty format.json in first endpoint:
//   - If all endpoints have empty format.json, then create fresh setup;
//     else wait for admin to heal.
//
// * Empty format.json in other endpoint:
//   - Wait for first endpoint or admin to heal.
//
// * finally
//   1. Read format.json from all endpoints.
//   2. Loop (1) if success is less than read quorum success.
//
func initFormatXL(setCount, setSize int) error {
	var thisEndpoint Endpoint
	for _, endpoint := range globalEndpoints {
		if endpoint.IsLocal {
			thisEndpoint = endpoint
		}
	}

	thisFormat, migrated, err := format.LoadXL(path.Join(thisEndpoint.Path, minioMetaBucket, formatConfigFile))
	if err != nil {
		return err
	}

	if migrated {
		multipartDir := path.Join(thisEndpoint.Path, minioMetaMultipartBucket)
		if err = os.RemoveAll(multipartDir); err != nil {
			return err
		}
		if err = os.MkdirAll(multipartDir, os.ModePerm); err != nil {
			return err
		}
	}

	isFirstEndpoint := thisEndpoint.String() == globalEndpoints[0].String()
	emptyCheck := thisFormat == nil

	// Start HTTP server.
	go func() {
		server := &http.Server{
			Addr:    thisEndpoint.Host,
			Handler: http.HandlerFunc(readFormatHandler),
		}

		log.Fatal(server.ListenAndServe())
	}()

	if !isFirstEndpoint && emptyCheck {
		// Wait for first endpoint or admin to heal.
		waitToFormat(isFirstEndpoint)
		emptyCheck = false
	}

	storageDisks, err := initStorageDisks(globalEndpoints)
	if err != nil {
		return err
	}
	defer closeStorageDisks(storageDisks)

	formats, healRequired := readFormatXLs(storageDisks, thisFormat, emptyCheck)

	if isFirstEndpoint {
		if emptyCheck && healRequired {
			// Wait for admin to heal.
			waitToFormat(isFirstEndpoint)
			emptyCheck = false
		} else {
			formats = format.GenerateXL(setCount, setSize)
			writeFormatXLs(storageDisks, formats)
			thisFormat = formats[0]
		}
	}

	return nil
}
