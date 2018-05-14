package cmd

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/quorum"
)

var errReadQuorum = errors.New("quorum is not met on read call")
var errWriteQuorum = errors.New("quorum is not met on write call")

func callFunctions(functions []quorum.Func, quorumValue int, maxExecTime, maxSuccessWait time.Duration, readQuorum bool) error {
	rc, errs := quorum.Call(functions, quorumValue, maxExecTime, maxSuccessWait)

	var err error
	if len(errs) > 0 {
		err = errs[len(errs)-1]
		if _, ok := err.(*quorum.Error); ok {
			if readQuorum {
				err = errReadQuorum
			} else {
				err = errWriteQuorum
			}
		} else {
			errs = errs[:len(errs)-1]
		}
	}

	if len(errs) > 0 {
		logger.LogIf(context.Background(), fmt.Errorf("%v", errs))
	}

	if rc < 0 {
		return err
	}

	return nil
}

type StorageClients struct {
	Disks          []StorageAPI
	ReadQuorum     int
	WriteQuorum    int
	MaxExecTime    time.Duration
	MaxSuccessWait time.Duration
}

func (clients *StorageClients) readQuorumCall(functions []quorum.Func) error {
	return callFunctions(functions, clients.ReadQuorum, clients.MaxExecTime, clients.MaxSuccessWait, true)
}

func (clients *StorageClients) writeQuorumCall(functions []quorum.Func) error {
	return callFunctions(functions, clients.WriteQuorum, clients.MaxExecTime, clients.MaxSuccessWait, false)
}

// // DiskInfo - get disk information from all disks.
// func (clients *StorageClients) DiskInfo() ([]disk.Info, error) {
// 	infos := make([]disk.Info, len(clients.Disks))
//
// 	quorumFunc := func(i int) quorum.Func {
// 		return quorum.Func(func() <-chan quorum.Error {
// 			errch := make(chan quorum.Error)
//
// 			go func() {
// 				defer close(errch)
// 				var err error
// 				if infos[i], err = clients.Disks[i].DiskInfo(); err != nil {
// 					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
// 				}
// 			}()
//
// 			return errch
// 		})
// 	}
//
// 	functions := []quorum.Func{}
// 	for i := range clients.Disks {
// 		functions = append(functions, quorumFunc(i))
// 	}
//
// 	if err := clients.readQuorumCall(functions); err != nil {
// 		return nil, err
// 	}
//
// 	return infos, nil
// }

// GetStorageInfo - computes storage information from all disks.
func (clients *StorageClients) GetStorageInfo() (*StorageInfo, error) {
	replies := make([]*disk.Info, len(clients.Disks))

	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				info, err := clients.Disks[i].DiskInfo()
				if err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				} else {
					replies[i] = &info
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	infos := []*disk.Info{}
	for _, reply := range replies {
		if reply != nil {
			infos = append(infos, reply)
		}
	}

	// Sort infos by Total.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Total < infos[j].Total
	})

	_, sscParity := getRedundancyCount(standardStorageClass, len(clients.Disks))
	_, rrscparity := getRedundancyCount(reducedRedundancyStorageClass, len(clients.Disks))

	dataCount := uint64(len(infos) - sscParity)
	if dataCount <= 0 {
		dataCount = uint64(len(infos))
	}

	storageInfo := &StorageInfo{
		Total: infos[0].Total * dataCount,
		Free:  infos[0].Free * dataCount,
	}

	storageInfo.Backend.Type = Erasure
	storageInfo.Backend.OnlineDisks = len(infos)
	storageInfo.Backend.OfflineDisks = len(clients.Disks) - len(infos)

	storageInfo.Backend.StandardSCParity = sscParity
	storageInfo.Backend.RRSCParity = rrscparity

	return storageInfo, nil
}

// MakeVol - create a volume from all disks.
func (clients *StorageClients) MakeVol(volume string) (err error) {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.Disks[i].MakeVol(volume); err != nil {
					// Do not treat errVolumeExists is an error.
					if err != errVolumeExists {
						errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
					}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// ListVols - list all volumes from all disks.
func (clients *StorageClients) ListVols() ([][]VolInfo, error) {
	infos := make([][]VolInfo, len(clients.Disks))

	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if infos[i], err = clients.Disks[i].ListVols(); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	return infos, nil
}

// StatVol - get volume info from all disks.
func (clients *StorageClients) StatVol(volume string) ([]VolInfo, error) {
	infos := make([]VolInfo, len(clients.Disks))

	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if infos[i], err = clients.Disks[i].StatVol(volume); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	return infos, nil
}

// DeleteVol - deletes volume from all disks.
func (clients *StorageClients) DeleteVol(volume string) (err error) {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.Disks[i].DeleteVol(volume); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// PrepareFile - reserve length size for file under volume in all disks.
func (clients *StorageClients) PrepareFile(volume, path string, length int64) (err error) {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.Disks[i].PrepareFile(volume, path, length); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// AppendFile - append data into file under volume in all disks.
func (clients *StorageClients) AppendFile(volume, path string, buffer []byte) (err error) {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.Disks[i].AppendFile(volume, path, buffer); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// StatFile - get information of file under volume from all disks.
func (clients *StorageClients) StatFile(volume, path string) ([]FileInfo, error) {
	infos := make([]FileInfo, len(clients.Disks))

	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if infos[i], err = clients.Disks[i].StatFile(volume, path); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	return infos, nil
}

// ReadAll - reads all data from file under volume from all disks.
func (clients *StorageClients) ReadAll(volume, path string) ([][]byte, error) {
	bufs := make([][]byte, len(clients.Disks))

	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if bufs[i], err = clients.Disks[i].ReadAll(volume, path); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	return bufs, nil
}

// ReadFile - reads buffer length from offset in file under volume from all disks.
func (clients *StorageClients) ReadFile(volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier) ([]int64, error) {
	ns := make([]int64, len(clients.Disks))

	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if ns[i], err = clients.Disks[i].ReadFile(volume, path, offset, buffer, verifier); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	return ns, nil
}

// ListDir - list count number of entries from path under volume from all disks.
func (clients *StorageClients) ListDir(volume, path string, count int) ([][]string, error) {
	infos := make([][]string, len(clients.Disks))

	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if infos[i], err = clients.Disks[i].ListDir(volume, path, count); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	return infos, nil
}

// DeleteFile - delete file under volume from all disks.
func (clients *StorageClients) DeleteFile(volume, path string) (err error) {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.Disks[i].DeleteFile(volume, path); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}

// RenameFile - rename source path under source volume to distination path under distination volume from all disks.
func (clients *StorageClients) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.Disks[i].RenameFile(srcVolume, srcPath, dstVolume, dstPath); err != nil {
					errch <- quorum.Error{ID: clients.Disks[i].String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.Disks {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions)
}
