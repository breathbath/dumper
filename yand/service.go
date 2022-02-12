package yand

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/breathbath/go_utils/utils/env"
	io2 "github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
)

const YandexUploader = "yandex"

type UploadConfig struct {
	URL              string
	UserName         string
	Pass             string
	UploadTimeoutRaw string
	UploadTimeout    time.Duration
}

func (mc *UploadConfig) Validate() error {
	fields := []*validation.FieldRules{
		validation.Field(&mc.URL, validation.Required),
		validation.Field(&mc.UserName, validation.Required),
		validation.Field(&mc.Pass, validation.Required),
		validation.Field(&mc.UploadTimeoutRaw, validation.By(func(value interface{}) error {
			valStr := fmt.Sprint(value)
			if valStr != "" {
				_, err := time.ParseDuration(valStr)
				if err != nil {
					return err
				}
			}

			return nil
		})),
	}

	return validation.ValidateStruct(mc, fields...)
}

func NewConfigFromEnvs() *UploadConfig {
	cfg := &UploadConfig{}
	cfg.URL = env.ReadEnv("YAND_UPLOADER_URL", "")
	cfg.UserName = env.ReadEnv("YAND_UPLOADER_LOGIN", "")
	cfg.Pass = env.ReadEnv("YAND_UPLOADER_PASS", "")
	cfg.UploadTimeoutRaw = env.ReadEnv("YAND_UPLOADER_TIMEOUT", "")
	if cfg.UploadTimeoutRaw != "" {
		timeout, err := time.ParseDuration(cfg.UploadTimeoutRaw)
		if err == nil {
			cfg.UploadTimeout = timeout
		}
	}

	return cfg
}

type Service struct {
	cfg *UploadConfig
}

func NewService(cfg *UploadConfig) *Service {
	return &Service{
		cfg: cfg,
	}
}

// see https://yandex.ru/dev/disk/doc/dg/reference/put.html for details
func (s *Service) Upload(path string) error {
	if err := s.cfg.Validate(); err != nil {
		return err
	}

	io2.OutputInfo("", "Will upload file %s to %s for user %s", path, s.cfg.URL, s.cfg.UserName)

	fileName := filepath.Base(path)

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	a := s.cfg.UserName + ":" + s.cfg.Pass
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(a))

	targetPath := join(s.cfg.URL, fileName)

	ctx := context.Background()
	var cancel context.CancelFunc
	if s.cfg.UploadTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), s.cfg.UploadTimeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetPath, file)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)

	cl := &http.Client{}
	resp, err := cl.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call yandex disk api: %v", err)
	}

	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		io2.OutputError(err, "", "Failed to read response body")
	}

	io2.OutputInfo("", "Got yandex response code %d, body: %s", resp.StatusCode, string(bodyBytes))

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to upload file: wrong response code %d from yandex", resp.StatusCode)
	}

	io2.OutputInfo("", "successfully uploaded to %s", targetPath)
	return nil
}

func join(path0, path1 string) string {
	return strings.TrimSuffix(path0, "/") + "/" + strings.TrimPrefix(path1, "/")
}
