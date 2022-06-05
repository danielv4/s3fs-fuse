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
	
	//"github.com/eikenb/pipeat"
)



// set CPATH=C:\Program Files (x86)\WinFsp\inc\fuse


type S3Config struct {
	
	SecretAccessKey string 
	AccessKeyId     string
	Region          string
}


type S3 struct {
	
	client *aws_s3.S3
	bucket string 
	config S3Config
	uploader *s3manager.Uploader
}


type S3FileObject struct {
	
	IsDir         bool
	Name          string
	Size          int
	LastModified  time.Time
}


func (self *S3) ReadDir(dirname string) ([]S3FileObject, error) {

	var err error
	var arr []S3FileObject

	input := aws_s3.ListObjectsV2Input{}
	input.Bucket = aws.String(self.bucket)
	input.Delimiter = aws.String("/")
	
	if dirname != "/" {
		input.Prefix = aws.String(dirname[1:] + "/")
	}

	resp, err := self.client.ListObjectsV2(&input)
	if err != nil {
		return arr, err
	}
	
	//fmt.Printf("%+v\n", resp)
	
	
	// dir
	for _, item := range resp.CommonPrefixes {
	
		obj := S3FileObject{}
		obj.IsDir = true
		obj.Name = path.Base(path.Dir(*item.Prefix))
		arr = append(arr, obj)
    }	
	
	// files
	for _, item := range resp.Contents {
	
		name := *item.Key
		last := name[len(name)-1:]
		if last != "/" {
			obj := S3FileObject{}
			obj.IsDir = false
			obj.Name = path.Base(name)
			obj.LastModified = *item.LastModified
			obj.Size = int(*item.Size)
			arr = append(arr, obj)		
		}
    }
	

	return arr, err
}


func (self *S3) Open(fpath string) (*bytes.Reader, error) {

	var err error
	reader := new(bytes.Reader)

	r, err := self.client.GetObject(&aws_s3.GetObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(fpath),
	})

	arr, err := ioutil.ReadAll(r.Body)
    if err != nil {
        return reader, err
    }	
	
	reader = bytes.NewReader(arr)
	return reader, err

}


func (self *S3) Create(fpath string, bs []byte) (error) {

	var err error
	buf := &bytes.Buffer{}
	buf.Write(bs)

	go func() {

		dpath := fpath[1:]
		fmt.Println(dpath)
		
		req := &aws_s3.PutObjectInput{
			Bucket: aws.String(self.bucket),
			Key:    aws.String(dpath),
			Body:   bytes.NewReader(buf.Bytes()),
			ACL:    aws.String("private"),
		}
		_, err = self.client.PutObject(req)
		if err != nil {
			//return err
		}
	
	}()
	
	return err
}

func (self *S3) Remove(fpath string) (error) {

	dpath := fpath[1:]
	fmt.Println(dpath)
	
	req := &aws_s3.DeleteObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(dpath),
	}
	_, err := self.client.DeleteObject(req)
	if err != nil {
		return err
	}
	
	return err
}


func (self *S3) Rmdir(fpath string) (error) {

	dpath := fpath[1:] + "/"
	fmt.Println(dpath)
	
	req := &aws_s3.DeleteObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(dpath),
	}
	_, err := self.client.DeleteObject(req)
	if err != nil {
		return err
	}
	
	return err
}


func (self *S3) Mkdir(fpath string, bs []byte) (error) {

	var err error
	buf := &bytes.Buffer{}
	buf.Write(bs)

	dpath := fpath[1:] + "/"
	fmt.Println(dpath)
	
	req := &aws_s3.PutObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(dpath),
		Body:   bytes.NewReader(buf.Bytes()),
		ACL:    aws.String("private"),
	}
	_, err = self.client.PutObject(req)
	if err != nil {
		return err
	}
	
	return err
}


func NewClient(bucketName string, config S3Config) (*S3, error) {
	
	var err error
	s3 := new(S3)
	
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.SecretAccessKey, config.AccessKeyId, ""),
	})
	
	if err != nil {
		return s3, err
	}
	
	svc := aws_s3.New(sess)	
	uploader := s3manager.NewUploader(sess)
	
	s3.client  = svc
	s3.config  = config
	s3.bucket  = bucketName
	s3.uploader = uploader
	
	//result, err := svc.ListBuckets(&aws_s3.ListBucketsInput{})
	//if err != nil {
	//	return s3, err
    //}
	//fmt.Printf("%+v\n", result)
	
	
	return s3, err
}











// WriteBuffer is a simple type that implements io.WriterAt on an in-memory buffer.
// The zero value of this type is an empty buffer ready to use.
type WriteBuffer struct {
    d []byte
    m int
}

// NewWriteBuffer creates and returns a new WriteBuffer with the given initial size and
// maximum. If maximum is <= 0 it is unlimited.
func NewWriteBuffer(size, max int) *WriteBuffer {
    if max < size && max >= 0 {
        max = size
    }
    return &WriteBuffer{make([]byte, size), max}
}

// SetMax sets the maximum capacity of the WriteBuffer. If the provided maximum is lower
// than the current capacity but greater than 0 it is set to the current capacity, if
// less than or equal to zero it is unlimited..
func (wb *WriteBuffer) SetMax(max int) {
    if max < len(wb.d) && max >= 0 {
        max = len(wb.d)
    }
    wb.m = max
}

// Bytes returns the WriteBuffer's underlying data. This value will remain valid so long
// as no other methods are called on the WriteBuffer.
func (wb *WriteBuffer) Bytes() []byte {
    return wb.d
}

// Shape returns the current WriteBuffer size and its maximum if one was provided.
func (wb *WriteBuffer) Shape() (int, int) {
    return len(wb.d), wb.m
}

func (wb *WriteBuffer) WriteAt(dat []byte, off int64) (int, error) {
    // Range/sanity checks.
    if int(off) < 0 {
        return 0, errors.New("Offset out of range (too small).")
    }
    if int(off)+len(dat) >= wb.m && wb.m > 0 {
        return 0, errors.New("Offset+data length out of range (too large).")
    }

    // Check fast path extension
    if int(off) == len(wb.d) {
        wb.d = append(wb.d, dat...)
        return len(dat), nil
    }

    // Check slower path extension
    if int(off)+len(dat) >= len(wb.d) {
        nd := make([]byte, int(off)+len(dat))
        copy(nd, wb.d)
        wb.d = nd
    }

    // Once no extension is needed just copy bytes into place.
    copy(wb.d[int(off):], dat)
    return len(dat), nil
}








// Cache
type Node struct {
	
	Path    string
	IsDir   bool
	Size    int
	fp      *bytes.Reader
	mknod   *WriteBuffer
}


type S3fs struct {
	fuse.FileSystemBase
	client *S3
	nodes map[string]*Node
}






func (self *S3fs) Unlink(path string) (errc int) {

	fmt.Printf("Unlink => %s\n", path)
	
	err := self.client.Remove(path)
	if err != nil {
		fmt.Println(err)
	}
	return 0
}


func (self *S3fs) Rmdir(path string) (errc int) {
	
	fmt.Printf("Rmdir => %s\n", path)
	
	err := self.client.Rmdir(path)
	if err != nil {
		return 0
	}	
	
	return 0
}


func (self *S3fs) Mkdir(path string, mode uint32) (errc int) {

	//fmt.Printf("Mkdir => %s\n", path)
	
	err := self.client.Mkdir(path, []byte(""))
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


func (self *S3fs) Mknod(path string, mode uint32, dev uint64) (errc int) {

	// pre_write
	// create file 
	// then open
	fmt.Printf("Mknod => %s\n", path)
	
	// 1000MB
	sizeInMB := 10000 * 1024 * 1024
	fp := NewWriteBuffer(0, sizeInMB)
	
	node := new(Node)
	node.IsDir = false
	node.Size = 0
	node.Path = path	
	node.mknod = fp
	self.nodes[path] = node

	return
}


func (self *S3fs) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {

	//fmt.Printf("Write() %s\n", path)
	//fmt.Printf("Write(?) %d\n", len(buff))

	if node, found := self.nodes[path]; found {
	
		n, err := node.mknod.WriteAt(buff, ofst)
		if nil != err {
			//n = fuseErrc(err)
			return 0
		}
		fmt.Printf("Written => %d \n", n)
		
		self.nodes[path].Size += n

		return n		
	} else {
		n = -fuse.EIO
		return 0 
	}

	return 0
}


func (self *S3fs) Open(path string, flags int) (errc int, fh uint64) {

	fmt.Printf("Open() %s %+v\n", path, flags)
	// use (Read) instead to init the open once instead of Open

	return 0, 0
}


func (self *S3fs) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {

	//fmt.Printf("Read() %s\n", path)

	if node, found := self.nodes[path]; found {
	
		if node.fp == nil {
			fp, err := self.client.Open(path)
			if err != nil {
				fmt.Println(err)
			}
			self.nodes[path].fp = fp		
		}
	
		n, err := node.fp.ReadAt(buff, ofst)
		if nil != err {
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


func (self *S3fs) Opendir(path string) (errc int, fh uint64) {
	//fmt.Printf("Opendir() %s\n", path)
	return 0, 0
}


func (self *S3fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {

	//fmt.Printf("Getattr() %s\n", path)
	//fmt.Printf("%+v\n", self.nodes)
	
	if path == "/" {
		stat.Mode = fuse.S_IFDIR | 0777
		return 0	
	} else if node, found := self.nodes[path]; found {
		
		//fmt.Printf("got node %+v\n", node)
	
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


func (self *S3fs) Readdir(path string,
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
		
			fill(entry.Name, nil, 0)
			
			// add node to Cache for Getattr()
			node := new(Node)
			node.IsDir = entry.IsDir
			node.Size = int(entry.Size)
			if path == "/" {
				node.Path = path + entry.Name
			} else {
				node.Path = path + "/" + entry.Name
			}
			
			//fmt.Printf("%+v\n", node)
			
			self.nodes[node.Path] = node
		}
	}
	

	return 0
}


func (self *S3fs) Release(path string, fh uint64) (errc int) {
	
	fmt.Printf("Release() %s\n", path)
	
	if node, found := self.nodes[path]; found {

		if len(node.mknod.d) > 0 {
		
			fmt.Printf("[+] s3 Creating file \n")
			err := self.client.Create(path, node.mknod.d)
			if err != nil {
				fmt.Println(err)
			} 
		} else {
			self.nodes[path].fp = nil
		}	
	}	
	
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

	s3fs := &S3fs{}
	
	config := S3Config{}
	config.SecretAccessKey = "SecretAccessKey"
	config.AccessKeyId = "AccessKeyId"
	config.Region = "us-west-2"

	s3, err := NewClient("swift2", config)
	if err != nil {
		fmt.Println("unable to create aws session")
	}
	
	
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
