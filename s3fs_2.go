/*
 * s3fs-fuse.go
 * Windows FUSE-based file system backed by Amazon S3
 * Copyright 2022 Daniel Vanderloo
 */
/*
 * This file is part of Cgofuse.
 *
 * It is licensed under the MIT license. The full license text can be found
 * in the License.txt file at the root of this project.
 */

package main

import (
	"os"
	"fmt"
	
	"github.com/winfsp/cgofuse/fuse"
	
	//"io"
	//"path"


	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"path"
	"time"
	
	"bytes"
	"io/ioutil"
	//"net/http"
	"errors"
	
	
	s3 "github.com/fclairamb/afero-s3"
	
	//"github.com/eikenb/pipeat"
)



// set CPATH=C:\Program Files (x86)\WinFsp\inc\fuse













// Cache
type Node struct {
	
	Path  string
	IsDir bool
	Size  int
	fp *sftp.File
	mknod *sftp.File
}


type Sshfs struct {
	fuse.FileSystemBase
	client *sftp.Client
	nodes map[string]*Node
}




func (self *Sshfs) Open(path string, flags int) (errc int, fh uint64) {

	fmt.Printf("Open() %s\n", path)

	if _, found := self.nodes[path]; found {
	
		//OpenFile(path string, f int) (*File, error)
	
		fp, err := self.client.Open(path)
		if err != nil {
			fmt.Println(err)
			return -fuse.ENOENT, ^uint64(0)
		}
		self.nodes[path].fp = fp

		return 0, 0
		
	} else {
		return -fuse.ENOENT, ^uint64(0)
	}
}


func (self *Sshfs) Opendir(path string) (errc int, fh uint64) {
	fmt.Printf("Opendir() %s\n", path)
	return 0, 0
}


func (self *Sshfs) Unlink(path string) (errc int) {
	
	err := self.client.Remove(path)
	if err != nil {
		fmt.Println(err)
	}
	return 0
}


func (self *Sshfs) Rmdir(path string) (errc int) {
	
	err := self.client.RemoveDirectory(path)
	if err != nil {
		fmt.Println(err)
	}
	return 0
}


func (self *Sshfs) Rename(oldpath string, newpath string) (errc int) {

	fmt.Printf("Rename() %s %s\n", oldpath, newpath)

	err := self.client.Rename(oldpath, newpath)
	if err != nil {
		fmt.Println(err)
	}	
	
	info, err := self.client.Stat(newpath)	
	if err != nil {
		fmt.Println(err)
	}	
	
	node := new(Node)
	node.IsDir = info.IsDir()
	node.Size = int(info.Size())
	node.Path = newpath	
	self.nodes[newpath] = node
	
	return 0
}


func (self *Sshfs) Mkdir(path string, mode uint32) (errc int) {
	// pre_write
	// create file 
	// then open
	fmt.Printf("Mkdir => %s\n", path)
	
	err := self.client.MkdirAll(path)
	if err != nil {
		return
	}
	
	node := new(Node)
	node.IsDir = true
	node.Size = 0
	node.Path = path	
	self.nodes[path] = node

	return
}


func (self *Sshfs) Mknod(path string, mode uint32, dev uint64) (errc int) {

	// pre_write
	// create file 
	// then open
	fmt.Printf("Mknod => %s\n", path)
	

	fp, err := self.client.Create(path)
	if err != nil {
		return
	}
	
	node := new(Node)
	node.IsDir = false
	node.Size = 0
	node.Path = path	
	node.mknod = fp
	self.nodes[path] = node

	return
}


func (self *Sshfs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {

	fmt.Printf("Getattr() %s\n", path)
	//fmt.Printf("%+v\n", self.nodes)
	
	if path == "/" {
		stat.Mode = fuse.S_IFDIR | 0777
		return 0	
	} else if node, found := self.nodes[path]; found {
	
		if node.IsDir == true {
			stat.Mode = fuse.S_IFDIR | 0777
		} else {
			stat.Mode = fuse.S_IFREG | 0777
			stat.Size = int64(node.Size)	
		}

		return 0		
	} else {
		return -fuse.ENOENT
	}
}


func (self *Sshfs) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {

	fmt.Printf("Write() %s\n", path)
	fmt.Printf("Write(?) %d\n", len(buff))

	if node, found := self.nodes[path]; found {
	
		n, err := node.mknod.WriteAt(buff, ofst)
		if nil != err && io.EOF != err {
			//n = fuseErrc(err)
			return 0
		}
		self.nodes[path].Size += n

		return n		
	} else {
		n = -fuse.EIO
		return 0 
	}

	return 0
}


func (self *Sshfs) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {

	fmt.Printf("Read() %s\n", path)

	if node, found := self.nodes[path]; found {
	
		n, err := node.fp.ReadAt(buff, ofst)
		if nil != err && io.EOF != err {
			//n = fuseErrc(err)
			return 0
		}

		return n		
	} else {
		n = -fuse.EIO
		return 0 
	}

	return 0
}


func (self *Sshfs) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	
	
	fill(".", nil, 0)
	fill("..", nil, 0)
	
	
	entries, err := self.client.ReadDir(path)
	if err != nil {
		fmt.Println(err)
	} else {
	
		//self.updateInodes(path, entries)
	
		for _, entry := range entries {
		
			fill(entry.Name(), nil, 0)
			
			// add node to Cache for Getattr()
			node := new(Node)
			node.IsDir = entry.IsDir()
			node.Size = int(entry.Size())
			if path == "/" {
				node.Path = path + entry.Name()
			} else {
				node.Path = path + "/" + entry.Name()
			}
			
			
			
			fmt.Printf("%+v\n", node)
			
			self.nodes[node.Path] = node
			
			
		}
	}	
	
	
	// update Cache when file deleted by another session
	

	return 0
}


func (self *S3fs) Statfs(path string, stat *fuse.Statfs_t) (err int) {
	
	//fmt.Printf("STAT FS!!! %s\n", path)
	stat.Bsize = 4096
	// f_frsize
	stat.Frsize = 4096

	// 8 EB - 1
	vtotal := (8 << 50) / stat.Frsize * 1024 - 1
	vavail := (2 << 50) / stat.Frsize * 1024
	vfree  := (1 << 50) / stat.Frsize * 1024
	//used := total - free

	// f_blocks
	stat.Blocks = vtotal
	stat.Bfree  = vfree
	stat.Bavail = vavail

	stat.Files  = 2240224
	stat.Ffree  = 1927486
	stat.Favail = 9900000

	stat.Namemax = 255
	return 0
}


func main() {

	sess, errSession := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("key1", "key2", ""),
		Region:           aws.String("us-west-2"),
		S3ForcePathStyle: aws.Bool(true),
	})

	if errSession != nil {
		fmt.Println("Could not create session:", errSession)
	}
	fs := afero.NewFs("testeder-r", sess)	
	
	
	
	
	// init
	s3fs.client = s3
	s3fs.nodes = make(map[string]*Node)
	
	
	host := fuse.NewFileSystemHost(s3fs)
	host.SetCapReaddirPlus(true)
	host.Mount("", append([]string{
		"-o", "ExactFileSystemName=NTFS",
		"-o", fmt.Sprintf("volname=%s", "S3"),
	}, os.Args[1:]...))	
}
