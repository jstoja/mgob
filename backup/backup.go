package backup

import (
	"fmt"
	"time"
  "io"
  "os/exec"

	"github.com/Sirupsen/logrus"
	"github.com/stefanprodan/mgob/config"
)

func Run(plan config.Plan, tmpPath string, storagePath string) (Result, error) {
  ts := time.Now()

	//archivePath := fmt.Sprintf("%v/%v-%v.gz", tmpPath, plan.Name, ts.Unix())
	//logPath := fmt.Sprintf("%v/%v-%v.log", tmpPath, plan.Name, ts.Unix())

  // Add timer here
  archiveStream, logStream, err := backup(plan)

  if err != nil {
    return Result{}, err
  }

  // If there's no API output, just have it
  res := Result{}
  if tmpPath != "" {
    res, err := dump(plan, storagePath, archiveStream, logStream)
    if err != nil {
      return res, err
    }
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

func backup(plan config.Plan) (io.ReadCloser, io.ReadCloser, error) {
	dump := fmt.Sprintf("mongodump --archive --gzip --host %v --port %v ", plan.Target.Host, plan.Target.Port)
	if plan.Target.Database != "" {
		dump += fmt.Sprintf("--db %v ", plan.Target.Database)
	}
	if plan.Target.Username != "" && plan.Target.Password != "" {
		dump += fmt.Sprintf("-u %v -p %v ", plan.Target.Username, plan.Target.Password)
	}
	if plan.Target.Params != "" {
		dump += fmt.Sprintf("%v", plan.Target.Params)
	}

  cmd := exec.Command("/bin/sh", "-c", dump)
  // cmd.SetTimeout(time.Duration(plan.Scheduler.Timeout) * time.Minute)
  cmd.Start()

  out2, err := cmd.StderrPipe()
  if err != nil {
    return nil, out2, err
  }
  out1, err := cmd.StdoutPipe()
  if err != nil {
    return nil, out1, err
  }

	return out1, out2, nil
}
