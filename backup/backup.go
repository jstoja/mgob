package backup

import (
	"fmt"
	"path/filepath"
	"time"
  "io"

	"github.com/Sirupsen/logrus"
	"github.com/codeskyblue/go-sh"
	"github.com/stefanprodan/mgob/config"
)

func Run(plan config.Plan, tmpPath string, storagePath string) (Result, error) {
  ts := time.Now()

	archivePath := fmt.Sprintf("%v/%v-%v.gz", tmpPath, plan.Name, ts.Unix())
	logPath := fmt.Sprintf("%v/%v-%v.log", tmpPath, plan.Name, ts.Unix())

  archiveStream, logStream, err := backup(plan, archivePath, logPath)

	res := Result{
		Plan:      plan.Name,
		Timestamp: ts.UTC(),
		Status:    500,
	}
	_, res.Name = filepath.Split(archivePath)

  if err != nil {
    return res, err
  }

  if tmpPath != "" {
    dump(archiveStream, logStream)
  }

  file := ""
	if plan.SFTP != nil {
		sftpOutput, err := sftpUpload(file, plan)
		if err != nil {
			return res, err
		} else {
			logrus.WithField("plan", plan.Name).Info(sftpOutput)
		}
	}

	if plan.S3 != nil {
		s3Output, err := s3Upload(file, plan)
		if err != nil {
			return res, err
		} else {
			logrus.WithField("plan", plan.Name).Infof("S3 upload finished %v", s3Output)
		}
	}

	if plan.GCloud != nil {
		gCloudOutput, err := gCloudUpload(file, plan)
		if err != nil {
			return res, err
		} else {
			logrus.WithField("plan", plan.Name).Infof("GCloud upload finished %v", gCloudOutput)
		}
	}

	t2 := time.Now()
	res.Status = 200
	res.Duration = t2.Sub(ts)
	return res, nil
}

func backup(plan config.Plan, archive, log string) (io.Writer, io.Writer, error) {
	dump := fmt.Sprintf("mongodump --archive=%v --gzip --host %v --port %v ",
		archive, plan.Target.Host, plan.Target.Port)
	if plan.Target.Database != "" {
		dump += fmt.Sprintf("--db %v ", plan.Target.Database)
	}
	if plan.Target.Username != "" && plan.Target.Password != "" {
		dump += fmt.Sprintf("-u %v -p %v ", plan.Target.Username, plan.Target.Password)
	}
	if plan.Target.Params != "" {
		dump += fmt.Sprintf("%v", plan.Target.Params)
	}

  cmd := sh.Command("/bin/sh", "-c", dump)
  cmd.SetTimeout(time.Duration(plan.Scheduler.Timeout) * time.Minute)
  cmd.Start()

	return cmd.Stdout, cmd.Stderr, nil
}
