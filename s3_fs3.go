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
	//"errors"
	
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
	
	dpath := fpath[1:]
	fmt.Println(dpath)

	r, err := self.client.GetObject(&aws_s3.GetObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(dpath),
	})

	arr, err := ioutil.ReadAll(r.Body)
    if err != nil {
        return reader, err
    }	
	
	reader = bytes.NewReader(arr)
	return reader, err

}


func (self *S3) Create(fpath string) (*File) {

	dpath := fpath[1:]
	fmt.Println(dpath)

	f := NewFile(self.bucket, dpath, self.client)
	return f
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









// File represents a file in S3.
// It is not threadsafe.
type File struct {
	
	bucket string
	name   string
	s3API  *aws_s3.S3

	// state
	offset int
	closed bool
}


// NewFile initializes an File object.
func NewFile(bucket, name string, s3API *aws_s3.S3) *File {
	return &File{
		bucket: bucket,
		name:   name,
		s3API:  s3API,
		offset: 0,
		closed: false,
	}
}


// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any.
func (f *File) Close() error {
	f.closed = true
	return nil
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (f *File) Read(p []byte) (int, error) {
	if f.closed {
		// mimic os.File's read after close behavior
		fmt.Println("read after close")
	}
	if f.offset != 0 {
		fmt.Println("TODO: non-offset == 0 read")
	}
	if len(p) == 0 {
		return 0, nil
	}
	output, err := f.s3API.GetObject(&aws_s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.name),
	})
	if err != nil {
		return 0, err
	}
	defer output.Body.Close()
	n, err := output.Body.Read(p)
	f.offset += n
	return n, err
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Read(p)
	return
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// The behavior of Seek on a file opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		f.offset = int(offset)
	case 1:
		f.offset += int(offset)
	case 2:
		// can probably do this if we had GetObjectOutput (ContentLength)
		fmt.Println("TODO: whence == 2 seek")
	}
	return int64(f.offset), nil
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(p []byte) (int, error) {
	if f.closed {
		// mimic os.File's write after close behavior
		fmt.Println("write after close")
	}
	if f.offset != 0 {
		fmt.Println("TODO: non-offset == 0 write")
	}
	readSeeker := bytes.NewReader(p)
	size := int(readSeeker.Size())
	if _, err := f.s3API.PutObject(&aws_s3.PutObjectInput{
		Bucket:               aws.String(f.bucket),
		Key:                  aws.String(f.name),
		Body:                 readSeeker,
	}); err != nil {
		fmt.Println("f.s3API.PutObject failed")
		return 0, err
	}
	f.offset += size
	return size, nil
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(p).
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {

	fmt.Println("WriteAt called")

	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Write(p)
	return
}







// Cache
type Node struct {
	
	Path    string
	IsDir   bool
	Size    int
	fp      *bytes.Reader
	mknod   *File
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

	fmt.Printf("Mknod => %s\n", path)
	
	fp := self.client.Create(path)
	
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
			fmt.Println("node.mknod.WriteAt Error")
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
			fmt.Println(self.client.Open)
			fp, _ := self.client.Open(path)
			self.nodes[path].fp = fp		
		}
	
		n, err := node.fp.ReadAt(buff, ofst)
		if nil != err {
			//n = fuseErrc(err)
			fmt.Println("node.fp.ReadAt")
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
	
	if _, found := self.nodes[path]; found {

		//if node.mknod != nil {
		//	node.mknod.Close()
		//}
		//if node.fp != nil {
		//	node.fp.Close()
		//}	
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
