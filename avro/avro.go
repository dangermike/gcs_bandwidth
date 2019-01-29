package avro

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/TriggerMail/bigquery2bigtable/schema"
	"github.com/linkedin/goavro"
	log "github.com/sirupsen/logrus"
)

// ReadAvroFiles reads all avro files in a directory (with prefix) into a channel
func ReadAvroFiles(directory string, prefix string, schemaAdapter schema.Adapter, destination chan<- *schema.SourceItem) error {
	localLog := log.WithField("directory", directory).WithField("prefix", prefix)
	finfos, err := ioutil.ReadDir(directory)
	if err != nil {
		localLog.WithError(err).Error("failed to read Avro source directory")
		return err
	}

	for _, finfo := range finfos {
		if finfo.IsDir() || !strings.HasPrefix(finfo.Name(), prefix) {
			continue
		}
		fileLog := localLog.WithField("file", finfo.Name())
		filePath := path.Join(directory, finfo.Name())

		fileLog.Debug("Processing")
		err := ReadAvroFile(filePath, schemaAdapter, destination)
		if err != nil {
			fileLog.WithError(err).Error("Avro read failed")
			return err
		}
	}
	return nil
}

// ReadStorageObjectAvro reads a GCS object in Avro format, converting with the provided adapter
func ReadStorageObjectAvro(ctx context.Context, oa *storage.ObjectAttrs, schemaAdapter schema.Adapter, destination chan<- *schema.SourceItem) error {
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.WithError(err).Fatal("Failed to create storage client")
	}

	rc, err := storageClient.Bucket(oa.Bucket).Object(oa.Name).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	return readAvro(rc, schemaAdapter, destination)
}

// ReadAvroFile reads from the local filesystem in Avro format, converting with the provided adapter
func ReadAvroFile(filePath string, schemaAdapter schema.Adapter, destination chan<- *schema.SourceItem) error {
	llog := log.WithField("filename", filePath)
	avroFile, err := os.Open(filePath)
	if err != nil {
		llog.WithError(err).Error("Failed to open source file")
		return err
	}
	defer avroFile.Close()
	err = readAvro(avroFile, schemaAdapter, destination)
	if err != nil {
		llog.WithError(err).Error("Failed to process avro")
	}
	return err
}

func readAvro(source io.Reader, schemaAdapter schema.Adapter, destination chan<- *schema.SourceItem) error {
	reader, err := goavro.NewOCFReader(source)
	if err != nil {
		log.WithError(err).Error("Failed to initialize avro reader for file")
		return err
	}
	rowNum := 0
	for reader.Scan() {
		item, err := reader.Read()
		if err != nil {
			log.WithField("rownum", rowNum).WithError(err).Error("Failed to read item")
			return err
		}
		message, err := schemaAdapter.Adapt(item)
		if err != nil {
			log.WithField("rownum", rowNum).WithError(err).Error("Failed to convert")
			return err
		}
		destination <- message
		rowNum++
	}
	return reader.Err()
}
