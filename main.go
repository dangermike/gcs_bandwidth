package main

import (
	"bufio"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	goavro "gopkg.in/linkedin/goavro.v2"
)

func benchmark(
	bucket string,
	prefix string,
	filter func(string) bool,
	threads uint,
	bufferSize uint,
) error {
	log.Info("start")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.WithError(err).Fatal("Failed to create storage client")
	}
	it := storageClient.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	foundObjects := []*storage.ObjectAttrs{}
	totalSize := uint64(0)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if filter(attrs.Name) {
			log.WithFields(log.Fields{
				"bucket": attrs.Bucket,
				"name":   attrs.Name,
				"size":   attrs.Size,
			}).Debug("Found file")
			foundObjects = append(foundObjects, attrs)
			totalSize += uint64(attrs.Size)
		} else {
			log.WithFields(log.Fields{
				"bucket": attrs.Bucket,
				"name":   attrs.Name,
			}).Info("Skipping file because of nameFilter")
		}
	}
	log.WithField("totalSize", totalSize).Info("All files found")
	var wg sync.WaitGroup
	ix := int64(-1)

	numFoundObjects := int64(len(foundObjects))

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	linesRead := make([]uint64, threads)
	go func() {
		lastLines := uint64(0)
		lastTime := time.Now()
		ewrLps := 0.0
		for range ticker.C {
			totalLines := linesRead[0]
			for i := 1; i < len(linesRead); i++ {
				totalLines += linesRead[i]
			}

			newLines := totalLines - lastLines
			lps := float64(newLines) / time.Now().Sub(lastTime).Seconds()
			lastTime = time.Now()
			lastLines = totalLines
			if ewrLps == 0.0 {
				ewrLps = lps
			} else {
				ewrLps = (0.9 * ewrLps) + (0.1 * lps)
			}

			log.WithFields(log.Fields{
				"total_lines": totalLines,
				"new_lines":   newLines,
				"lps":         uint(lps),
				"ewr(lps)":    uint(ewrLps),
			}).Info("bytes read")
		}
	}()

	for i := uint(0); i < threads; i++ {
		wg.Add(1)
		go func(tid uint) {
			defer wg.Done()
			llog := log.WithField("thread", tid)
			llog.Info("thread started")
			var buffReader *bufio.Reader
			if bufferSize > 0 {
				buffReader = bufio.NewReaderSize(nil, int(bufferSize))
			}
			for fix := atomic.AddInt64(&ix, 1); fix < numFoundObjects; fix = atomic.AddInt64(&ix, 1) {
				llog.WithFields(log.Fields{
					"bucket": foundObjects[fix].Bucket,
					"name":   foundObjects[fix].Name,
				}).Info("opening")

				rc, err := storageClient.Bucket(foundObjects[fix].Bucket).Object(foundObjects[fix].Name).NewReader(ctx)
				if err != nil {
					llog.WithError(err).Fatal("failed to open file")
				}
				var r io.Reader = rc
				if bufferSize > 0 {
					buffReader.Reset(rc)
					r = buffReader
				}

				avroReader, err := goavro.NewOCFReader(r)
				if err != nil {
					llog.WithError(err).Fatal("failed to read Avro from file")
				}

				for avroReader.Scan() {
					_, err := avroReader.Read()
					if err != nil {
						log.WithError(err).Error("Failed to read item")
						break
					}
					linesRead[tid]++
				}

				rc.Close()
			}
			llog.Info("thread finished")
		}(i)
	}
	wg.Wait()
	return nil
}
