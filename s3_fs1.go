
package main

import(
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"bytes"
)







// File represents a file in S3.
// It is not threadsafe.
type File struct {
	
	bucket string
	name   string
	s3API  *s3.S3

	// state
	offset int
	closed bool
}


// NewFile initializes an File object.
func NewFile(bucket, name string, s3API *s3.S3) *File {
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
		panic("read after close")
	}
	if f.offset != 0 {
		panic("TODO: non-offset == 0 read")
	}
	if len(p) == 0 {
		return 0, nil
	}
	output, err := f.s3API.GetObject(&s3.GetObjectInput{
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
		panic("TODO: whence == 2 seek")
	}
	return int64(f.offset), nil
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(p []byte) (int, error) {
	if f.closed {
		// mimic os.File's write after close behavior
		panic("write after close")
	}
	if f.offset != 0 {
		panic("TODO: non-offset == 0 write")
	}
	readSeeker := bytes.NewReader(p)
	size := int(readSeeker.Size())
	if _, err := f.s3API.PutObject(&s3.PutObjectInput{
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
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Write(p)
	return
}


func main() {

	// You create a session
	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credentials.NewStaticCredentials("key1", "key2", ""),
	})
	
	svc := s3.New(sess)	
	
	f := NewFile("swift2", "file.txt", svc)
	f.WriteAt([]byte("widuu"), 0)
	//f.Close()
	
	
	buf := make([]byte, 4)
	
	m := NewFile("swift2", "file.txt", svc)
	m.ReadAt(buf, 0)
	
	fmt.Println(string(buf))
	
}