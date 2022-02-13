package yand

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/breathbath/go_utils/utils/env"
	io2 "github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
)

const YandexUploader = "yandex"

const uploadURL = "https://cloud-api.yandex.net/v1/disk/resources/upload"

type UploadTarget struct {
	OperationID string `json:"operation_id"`
	Href        string `json:"href"`
	Method      string `json:"method"`
}

type ResponseErr struct {
	Description string `json:"description"`
	Error       string `json:"error"`
}

type UploadConfig struct {
	Token            string
	RemoteFolder     string
	UploadTimeoutRaw string
	UploadTimeout    time.Duration
}

func (mc *UploadConfig) Validate() error {
	fields := []*validation.FieldRules{
		validation.Field(&mc.Token, validation.Required),
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
	cfg.Token = env.ReadEnv("YAND_TOKEN", "")
	cfg.RemoteFolder = env.ReadEnv("YAND_FOLDER", "")
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

func (s *Service) buildAuthURL(fileName string) (*url.URL, error) {
	u, err := url.Parse(uploadURL)
	if err != nil {
		return nil, err
	}

	values := u.Query()
	values.Add("path", join(s.cfg.RemoteFolder, fileName))
	values.Add("overwrite", "true")

	u.RawQuery = values.Encode()

	return u, nil
}

func (s *Service) fetchUploadURL(ctx context.Context, fileName string) (uploadURL, method string, err error) {
	authURL, err := s.buildAuthURL(fileName)
	if err != nil {
		return "", "", err
	}

	io2.OutputInfo("", "Will read upload URL from %s", authURL.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authURL.String(), http.NoBody)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("OAuth %s", s.cfg.Token))

	cl := &http.Client{}
	resp, err := cl.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to call yandex disk api: %v", err)
	}

	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("failed to read response body %v", err)
	}
	jsonDec := json.NewDecoder(bytes.NewBuffer(bodyBytes))

	if resp.StatusCode != http.StatusOK {
		errResp := new(ResponseErr)

		err = jsonDec.Decode(errResp)
		if err != nil {
			io2.OutputError(err, "", "failed to decode resp %q to ResponseErr", string(bodyBytes))
			return "",
				"",
				fmt.Errorf(
					"failed retrieve upload link: wrong response code %d from yandex: %s, %v",
					resp.StatusCode,
					string(bodyBytes),
					err,
				)
		}
		return "",
			"",
			fmt.Errorf(
				"failed retrieve upload link: wrong response code %d from yandex, message %s[%s]",
				resp.StatusCode,
				errResp.Description,
				errResp.Error,
			)
	}

	uploadTarget := new(UploadTarget)
	err = jsonDec.Decode(uploadTarget)

	if err != nil {
		io2.OutputError(err, "", "failed to decode resp %q to UploadTarget", string(bodyBytes))
		return "", "", fmt.Errorf("failed to read upload URL from %q: %v", string(bodyBytes), err)
	}

	io2.OutputInfo("", "got upload url: %s", string(bodyBytes))

	return uploadTarget.Href, uploadTarget.Method, nil
}

// see https://yandex.ru/dev/disk/doc/dg/reference/put.html for details
func (s *Service) Upload(path string) error {
	if err := s.cfg.Validate(); err != nil {
		return err
	}

	fileName := filepath.Base(path)

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	ctx := context.Background()
	var cancel context.CancelFunc
	if s.cfg.UploadTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), s.cfg.UploadTimeout)
		defer cancel()
	}

	tempUploadURL, method, err := s.fetchUploadURL(ctx, fileName)
	if err != nil {
		return err
	}

	err = s.uploadToTempUploadURL(ctx, tempUploadURL, method, file)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) uploadToTempUploadURL(ctx context.Context, tempUploadURL, method string, file *os.File) error {
	io2.OutputInfo("", "Will upload file %s to %q, method %q", file.Name(), uploadURL, method)

	req, err := http.NewRequestWithContext(ctx, method, tempUploadURL, file)
	if err != nil {
		return err
	}

	cl := &http.Client{}
	resp, err := cl.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call upload URL %q, method %q: %v", tempUploadURL, method, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		io2.OutputWarning("", "failed to read response body from %s: %v", tempUploadURL, err)
	}

	io2.OutputInfo("", "response code %d, body %q", resp.StatusCode, string(bodyBytes))

	if resp.StatusCode == http.StatusCreated {
		io2.OutputInfo("", "successfully uploaded file %s to %s", file.Name(), tempUploadURL)
		return nil
	}

	if resp.StatusCode == http.StatusAccepted {
		io2.OutputInfo("", "successfully uploaded file %s to %s, but it's not yet moved to the target localtion", file.Name(), tempUploadURL)
		return nil
	}

	msg := ""
	switch resp.StatusCode {
	case http.StatusPreconditionFailed:
		msg = "invalid range in Content-Range.header"
	case http.StatusRequestEntityTooLarge:
		msg = "file is too big (> 10gb)"
	case http.StatusInternalServerError:
		msg = "internal server error"
	case http.StatusServiceUnavailable:
		msg = "server is not available"
	case http.StatusInsufficientStorage:
		msg = "not enough space on disk"
	default:
		msg = "unknown error"
	}

	return errors.New(msg)
}

func join(path0, path1 string) string {
	return strings.TrimSuffix(path0, "/") + "/" + strings.TrimPrefix(path1, "/")
}
