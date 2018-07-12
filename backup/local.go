package backup

import (
	"fmt"
	"io/ioutil"
  "io"

	"github.com/codeskyblue/go-sh"
	"github.com/pkg/errors"
)

func dump(archive, log io.Writer) (io.Writer, io.Writer, error) {

  // TODO: rework this part
	//res := Result{}
  //err := sh.Command("mkdir", "-p", planDir).Run()
	//if err != nil {
    // return res, errors.Wrapf(err, "creating dir %v in %v failed", plan.Name, storagePath)
	//}

  // fi, err := os.Stat(archive)
	// if err != nil {
	// 	return res, errors.Wrapf(err, "stat file %v failed", archive)
	// }
	// res.Size = fi.Size()

// 	err = sh.Command("mv", archive, planDir).Run()
// 	if err != nil {
// 		return res, errors.Wrapf(err, "moving file from %v to %v failed", archive, planDir)
// 	}
// 
//	err = sh.Command("mv", log, planDir).Run()
//	if err != nil {
//		return res, errors.Wrapf(err, "moving file from %v to %v failed", log, planDir)
//	}

	//if plan.Scheduler.Retention > 0 {
	//	err = applyRetention(planDir, plan.Scheduler.Retention)
	//	if err != nil {
	//		return res, errors.Wrap(err, "retention job failed")
	//	}
	//}

	//file := filepath.Join(planDir, res.Name)
  return nil, nil, nil
}

func logToFile(file string, data []byte) error {
	if len(data) > 0 {
		err := ioutil.WriteFile(file, data, 0644)
		if err != nil {
			return errors.Wrapf(err, "writing log %v failed", file)
		}
	}

	return nil
}

func applyRetention(path string, retention int) error {
	gz := fmt.Sprintf("cd %v && rm -f $(ls -1t *.gz | tail -n +%v)", path, retention+1)
	err := sh.Command("/bin/sh", "-c", gz).Run()
	if err != nil {
		return errors.Wrapf(err, "removing old gz files from %v failed", path)
	}

	log := fmt.Sprintf("cd %v && rm -f $(ls -1t *.log | tail -n +%v)", path, retention+1)
	err = sh.Command("/bin/sh", "-c", log).Run()
	if err != nil {
		return errors.Wrapf(err, "removing old log files from %v failed", path)
	}

	return nil
}

// TmpCleanup remove files older than one day
func TmpCleanup(path string) error {
	rm := fmt.Sprintf("find %v -not -name \"mgob.db\" -mtime +%v -type f -delete", path, 1)
	err := sh.Command("/bin/sh", "-c", rm).Run()
	if err != nil {
		return errors.Wrapf(err, "%v cleanup failed", path)
	}

	return nil
}
