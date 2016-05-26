package main

import (
	"fmt"
	"io"
	"log"
	"minio2/pkg/storage"
	"os"
)

func main() {
	// single disk backend
	fmt.Println("single disk backend:")
	if disk, err := storage.NewDisk("/tmp/mydisk"); err != nil {
		log.Println(err)
	} else {
		defer func() {
			fmt.Println(disk.DeleteFolder("mybucket"))
		}()
		fmt.Println(disk.CreateFolder("mybucket", nil))
		fmt.Println(disk.GetFolderInfo("mybucket"))
		fmt.Println(disk.ListFolders())

		if wc, err := disk.Store("mybucket/y.go", nil); err != nil {
			log.Println(err)
		} else if file, err := os.Open("main.go"); err != nil {
			log.Println(err)
		} else {
			fmt.Println(io.Copy(wc, file))
			if err := wc.Close(); err != nil {
				log.Println(err)
			}
			if rc, err := disk.Get("mybucket/y.go", 10, 200); err != nil {
				log.Println(err)
			} else {
				fmt.Println(io.Copy(os.Stderr, rc))
				fmt.Println(rc.Close())
			}
		}
	}
	fmt.Println("=====================================================================================")

	fmt.Println("multi-disk erasure coded backend:")
	// multi-disk erasure coded backend
	if disk, err := storage.NewErasureDisk(4, storage.DefaultStripeSize, "/tmp/e1", "/tmp/e2", "/tmp/e3", "/tmp/e4", "/tmp/e5", "/tmp/e6", "/tmp/e7", "/tmp/e8"); err != nil {
		log.Println(err)
	} else {
		defer func() {
			fmt.Println(disk.DeleteFolder("mybucket"))
		}()
		fmt.Println(disk.CreateFolder("mybucket", nil))
		fmt.Println(disk.GetFolderInfo("mybucket"))
		fmt.Println(disk.ListFolders())

		if wc, err := disk.Store("mybucket/y.go", nil); err != nil {
			log.Println(err)
		} else if file, err := os.Open("main.go"); err != nil {
			log.Println(err)
		} else {
			fmt.Println(io.Copy(wc, file))
			if err := wc.Close(); err != nil {
				log.Println(err)
			}
			if rc, err := disk.Get("mybucket/y.go", 10, 200); err != nil {
				log.Println(err)
			} else {
				fmt.Println(io.Copy(os.Stderr, rc))
				fmt.Println(rc.Close())
			}
		}
	}
	fmt.Println("=====================================================================================")
}
