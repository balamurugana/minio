package backend

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	xos "github.com/balamurugana/minio/test/os"
)

func (disk *Disk) DeleteBucket(requestID, bucketName string) error {
	objectsDir := disk.objectsDir(bucketName)

	isEmptyDir := true
	err := xos.Readdirnames(objectsDir,
		func(name string, mode os.FileMode) (stop bool) {
			isEmptyDir = false
			return true
		},
	)

	if err != nil {
		if os.IsNotExist(err) {
			return ErrBucketNotFound
		}

		return err
	}

	if !isEmptyDir {
		return ErrBucketNotEmpty
	}

	return os.Rename(disk.bucketDir(bucketName), disk.transDir(requestID))
}

func (disk *Disk) CloseDeleteBucket(requestID, bucketName string, undo bool) error {
	if undo {
		if err := os.Rename(disk.transDir(requestID), disk.bucketDir(bucketName)); err != nil {
			return err
		}
	}

	os.RemoveAll(disk.transDir(requestID))
	return nil
}

func (disk *Disk) readdir(bucketName, baseDir, prefix, startAfter string, maxKeys int) ([]string, error) {
	entries := []string{}
	found := false
	dir := path.Join(disk.objectsDir(bucketName), baseDir)

	err := xos.Readdirnames(dir, func(name string, mode os.FileMode) (stop bool) {
		if mode == os.ModeType {
			fi, err := os.Stat(path.Join(dir, name))
			if err != nil {
				return false
			}

			mode = fi.Mode()
		}

		if !mode.IsDir() {
			return false
		}

		name = path.Join(baseDir, name)
		metaFile := path.Join(disk.objectsDir(bucketName), name, disk.metaFilename(""))
		if !xos.Exist(metaFile) {
			name += "/"
		}

		if !strings.HasPrefix(name, prefix) {
			return false
		}

		entries = append(entries, name)
		if name == startAfter {
			found = true
		}

		return false
	})

	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBucketNotFound
		}

		return nil, err
	}

	sort.Strings(entries)
	if startAfter != "" {
		if !found {
			return nil, nil
		}

		i := sort.SearchStrings(entries, startAfter)
		entries = entries[i+1:]
	}

	if len(entries) > maxKeys {
		entries = entries[:maxKeys]
	}

	return entries, err
}

func (disk *Disk) readdirRecursive(bucketName, baseDir, prefix, startAfter string, maxKeys int) ([]string, error) {
	entries, err := disk.readdir(bucketName, baseDir, prefix, startAfter, maxKeys)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(entries); i++ {
		if !strings.HasSuffix(entries[i], "/") {
			continue
		}

		if i > 0 && strings.HasPrefix(entries[i-1], entries[i]) {
			continue
		}

		parentEntries := entries

		dir := path.Join(baseDir, entries[i])
		childEntries, err := disk.readdir(bucketName, dir, "", "", maxKeys)
		if err != nil {
			return nil, err
		}

		entries = append([]string{}, entries[:i]...)
		wanted := maxKeys - i
		if wanted > len(childEntries) {
			entries = append(entries, childEntries...)
			wanted -= len(childEntries)
			if wanted > 0 {
				if wanted > len(parentEntries[i:]) {
					entries = append(entries, parentEntries[i:]...)
				} else {
					entries = append(entries, parentEntries[i:i+wanted]...)
				}
			}
		} else {
			entries = append(entries, childEntries[:wanted]...)
		}
	}

	return entries, nil
}

func (disk *Disk) GetBucket(bucketName, prefix, startAfter string, maxKeys int) (keys, prefixes []string, marker string, err error) {
	if prefix != "" {
		if startAfter != "" && !strings.HasPrefix(startAfter, prefix) {
			return nil, nil, "", fmt.Errorf("startAfter must start with prefix")
		}
	}

	dirname := func(dir string) string {
		dir = path.Dir(dir)
		if dir == "." {
			dir = ""
		}

		return dir
	}

	limit := maxKeys + 1
	var entries []string

	baseDir := dirname(prefix)
	for {
		if startAfter != "" {
			baseDir = dirname(startAfter)
			if strings.HasSuffix(startAfter, "/") {
				baseDir = dirname(baseDir)
				startAfter = dirname(startAfter)
			}
		}

		var parentEntries []string
		if parentEntries, err = disk.readdirRecursive(bucketName, baseDir, prefix, startAfter, limit); err != nil {
			return nil, nil, "", err
		}

		entries = append(entries, parentEntries...)

		limit -= len(entries)
		startAfter = ""
		baseDir = dirname(baseDir)

		if limit == 0 || baseDir == "" || !strings.HasPrefix(baseDir, prefix) {
			break
		}
	}

	if len(entries) == maxKeys+1 {
		marker = entries[maxKeys-1]
		entries = entries[:maxKeys]
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry, "/") {
			prefixes = append(prefixes, entry)
		} else {
			keys = append(keys, entry)
		}
	}

	return keys, prefixes, marker, nil
}

func (disk *Disk) headBucket(bucketName string) (os.FileInfo, error) {
	fi, err := os.Lstat(disk.bucketDir(bucketName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBucketNotFound
		}

		return nil, err
	}

	return fi, nil
}

func (disk *Disk) HeadBucket(bucketName string) (*BucketInfo, error) {
	fi, err := disk.headBucket(bucketName)
	if err != nil {
		return nil, err
	}

	return &BucketInfo{bucketName, fi.ModTime()}, nil
}

func (disk *Disk) PutBucket(requestID, bucketName string) (err error) {
	tempDir := disk.tempDir(requestID)
	defer os.RemoveAll(tempDir)

	if err = os.Mkdir(tempDir, os.ModePerm); err != nil {
		return err
	}

	if err = os.Mkdir(path.Join(tempDir, "objects"), os.ModePerm); err != nil {
		return err
	}

	if err = os.Rename(tempDir, disk.bucketDir(bucketName)); err != nil {
		if os.IsExist(err) {
			return ErrBucketAlreadyExists
		}

		return err
	}

	return nil
}

func (disk *Disk) ClosePutBucket(requestID, bucketName string, undo bool) error {
	if undo {
		// FIXME: The problem mentioned in comment in ClosePutObject().failure() also applies here, but allow them to fail in this case.
		return os.RemoveAll(disk.bucketDir(bucketName))
	}

	return nil
}
