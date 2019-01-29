package main

import (
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/alecthomas/units"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

// Version is displayed by the CLI with --version
const Version = "0.0.1"

// Flags are what the user can pass to us
var Flags = []cli.Flag{
	cli.StringFlag{
		Name:   "bucket",
		EnvVar: "GOOGLE_CLOUD_GCS_BUCKET",
		Usage:  "Google Cloud Storage bucket containing source data",
	},
	cli.StringFlag{
		Name:   "prefix",
		EnvVar: "GOOGLE_CLOUD_GCS_PREFIX",
		Usage:  "Google Cloud Storage prefix for objects containing sorce data",
	},
	cli.StringFlag{
		Name:   "path",
		EnvVar: "GOOGLE_CLOUD_GCS_PATH",
		Usage:  "Google Cloud Storage path for objects containing sorce data",
	},
	cli.StringFlag{
		Name:   "filter",
		EnvVar: "GOOGLE_CLOUD_GCS_FILTER",
		Usage:  "A regex-based filter on the GCS file paths",
	},
	cli.UintFlag{
		Name:  "threads",
		Value: 1,
		Usage: "Number of threads reading from GCS",
	},
	cli.StringFlag{
		Name:  "buffer-size",
		Usage: "Size of the buffer pool when reading from GCS",
	},
	cli.StringFlag{
		Name:   "log-format",
		EnvVar: "LOG_FORMAT",
		Value:  "default",
		Usage:  "Forces the output format of the log. Valid options: default, text, color, json",
	},
	cli.BoolFlag{
		Name:  "verbose, v",
		Usage: "Verbose logging (level=DEBUG)",
	},
	cli.BoolFlag{
		Name:  "quiet, q",
		Usage: "Quiet logging (level=WARNING)",
	},
}

func main() {
	if err := runApp(); err != nil {
		log.Fatal(err)
	}
}

func runApp() error {
	app := cli.NewApp()
	app.Name = "gcs_bandwidth"
	app.Usage = "determine GCS bandwidth"
	app.Action = appMain
	app.Version = Version

	// overridden so -v can be verbose
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print the version",
	}

	app.Flags = Flags

	return app.Run(os.Args)
}

func appMain(c *cli.Context) error {
	if c.Bool("verbose") {
		log.SetLevel(log.DebugLevel)
	} else if c.Bool("quiet") {
		log.SetLevel(log.WarnLevel)
	}

	switch lf := c.String("log-format"); lf {
	case "text":
		formatter := log.TextFormatter{}
		formatter.DisableColors = true
		log.SetFormatter(&formatter)
	case "color":
		formatter := log.TextFormatter{}
		formatter.ForceColors = true
		log.SetFormatter(&formatter)
	case "json":
		formatter := log.JSONFormatter{}
		log.SetFormatter(&formatter)
	case "default":
	default:
		log.WithField("log-format", lf).Error("Invalid log format")
		return errors.New("Invalid options")
	}

	if err := validateInputs(c); err != nil {
		return err
	}

	var bucket string
	var prefix string

	if c.IsSet("path") {
		parts := strings.SplitN(strings.TrimPrefix(c.String("path"), "gs://"), "/", 2)
		bucket = parts[0]
		prefix = parts[1]
	} else {
		bucket = c.String("bucket")
		prefix = c.String("prefix")
	}

	var filter func(string) bool
	if c.IsSet("filter") {
		rx, err := regexp.Compile(c.String("filter"))
		if err != nil {
			log.WithField("expression", c.String("filter")).WithError(err).Fatal("Failed to compile source filter")
		}
		filter = rx.MatchString
	} else {
		filter = func(string) bool { return true }
	}

	bufferSize := uint(0)
	if c.IsSet("buffer-size") {
		buffint, err := strconv.Atoi(c.String("buffer-size"))
		if err == nil {
			bufferSize = uint(buffint)
		} else {
			byteCount, err := units.ParseBase2Bytes(c.String("buffer-size"))
			if err != nil {
				log.WithField("size", c.String("buffer-size")).WithError(err).Fatal("Failed to parse buffer-size")
			}
			bufferSize = uint(byteCount)
		}
	}

	return benchmark(
		bucket,
		prefix,
		filter,
		c.Uint("threads"),
		bufferSize,
	)
}

func validateInputs(c *cli.Context) error {
	ok := true

	if !((c.IsSet("bucket") && c.IsSet("prefix")) || c.IsSet("path")) {
		ok = false
		log.Error("Required parameter bucket and prefix or path must be set")
	}

	if !ok {
		return errors.New("Required parameters missing")
	}
	return nil
}
