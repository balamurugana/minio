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
	"sync"
	"time"

	"github.com/minio/minio/cmd/format"
)

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

func readFormatXLs(disks []StorageAPI) ([]*format.XLV3, []error) {
	formats := make([]*format.XLV3, len(disks))
	errs := make([]error, len(disks))

	var wg sync.WaitGroup
	for i := range disks {
		if disks[i] != nil {
			wg.Add(1)
			go func(i int) {
				formats[i], errs[i] = readFormatXL(disks[i])
				wg.Done()
			}(i)
		}
	}
	wg.Wait()

	return formats, errs
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

func initFormatXL(setCount, setSize int) error {
	storageDisks, err := initStorageDisks(globalEndpoints)
	if err != nil {
		return err
	}
	defer closeStorageDisks(storageDisks)

	configLock := globalNSMutex.NewNSLock(minioMetaBucket, formatConfigFile)

	var formats []*format.XLV3
	var errs []error

	isEmpty := func(f []*format.XLV3, errs []error) bool {
		for i := range storageDisks {
			if errs[i] != nil || formats[i] != nil {
				return false
			}
		}

		return true
	}

	disks := storageDisks
	for {
		// Get read lock.
		for {
			if err := configLock.GetRLock(1 * time.Second); err == nil {
				break
			}
		}

		var f []*format.XLV3
		f, errs = readFormatXLs(disks)

		disks = make([]StorageAPI, len(disks))
		errCount := 0
		for i := range errs {
			if errs[i] != nil {
				errCount++
				disks[i] = storageDisks[i]
			}
		}

		if errCount > len(globalEndpoints)/2 {
			disks = storageDisks
			configLock.RUnlock()
			continue
		}

		if globalEndpoints[0].IsLocal {
			// As first endpoint is empty, check whether all endpoints are empty.
			if errs[0] != nil && formats[0] == nil && errCount == 0 && isEmpty(f, errs) {
				// We have to init the setup.
				break
			}
		}

		// Check if all local endpoints are empty or not.
		isLocalEmpty := true
		for i := range storageDisks {
			if globalEndpoints[i].IsLocal {
				if errs[i] != nil && formats[i] != nil {
					isLocalEmpty = false
					break
				}
			}
		}

		configLock.RUnlock()
		if isLocalEmpty {
			if globalEndpoints[0].IsLocal {
				log.Printf("waiting for admin to format by running 'mc admin heal' command\n")
			} else {
				log.Printf("waiting for first peer to format\n")
			}
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	if err := configLock.GetLock(1 * time.Second); err == nil {
		continue
	}

	formats = format.GenerateXL(setCount, setSize)
	writeFormatXLs(storageDisks, formats)
	configLock.Unlock()
}
