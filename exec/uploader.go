package exec

import (
	"fmt"
	"os"

	"github.com/breathbath/go_utils/v3/pkg/io"
)

type Uploader interface {
	Upload(path string) error
}

type UploaderCfg struct {
	Name              string `json:"name"`
	DeleteAfterUpload bool   `json:"delete_after_upload"`
}

type UploadHelper struct {
}

func (uh UploadHelper) validateConfig(cfg *UploaderCfg, registeredUploaders map[string]Uploader) error {
	if cfg == nil || cfg.Name == "" {
		return nil
	}

	if len(registeredUploaders) == 0 {
		return fmt.Errorf("empty uploaders list for %s", cfg.Name)
	}

	if _, ok := registeredUploaders[cfg.Name]; !ok {
		return fmt.Errorf("unknown uploader name %s", cfg.Name)
	}

	return nil
}

func (uh UploadHelper) uploadIfNeeded(filepath string, cfg *UploaderCfg, registeredUploaders map[string]Uploader) error {
	if cfg == nil || cfg.Name == "" {
		return nil
	}

	err := uh.validateConfig(cfg, registeredUploaders)
	if err != nil {
		return err
	}

	uploader := registeredUploaders[cfg.Name]

	if cfg.DeleteAfterUpload {
		defer func() {
			e := os.Remove(filepath)
			if e != nil {
				io.OutputError(e, "", "Failed to delete %s", filepath)
			} else {
				io.OutputInfo("", "deleted %s", filepath)
			}
		}()
	}

	err = uploader.Upload(filepath)
	if err != nil {
		return err
	}

	return nil
}
