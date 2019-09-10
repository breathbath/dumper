package exec

import (
	"errors"
	io2 "github.com/breathbath/go_utils/utils/io"
	"io"
)

func NewStdErrorWriter() io.Writer {
	return NewCallbackWriter(func(p []byte) (n int, err error) {
		io2.OutputError(errors.New(string(p)), "", "")
		n = len(p)
		return
	})
}

func NewStdSuccessWriter() io.Writer {
	return NewCallbackWriter(func(p []byte) (n int, err error) {
		io2.OutputInfo("", string(p))
		n = len(p)
		return
	})
}

type CallbackWriter struct {
	Callback func (p []byte) (n int, err error)
}

func NewCallbackWriter(callback func (p []byte) (n int, err error)) CallbackWriter {
	return CallbackWriter{Callback: callback}
}

func (cw CallbackWriter) Write(p []byte) (n int, err error) {
	return cw.Callback(p)
}
