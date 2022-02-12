package cli

import (
	"io"

	io2 "github.com/breathbath/go_utils/utils/io"
)

type CallbackWriter struct {
	Callback func(p []byte) (n int, err error)
}

func NewStdErrorWriter() io.Writer {
	return NewCallbackWriter(func(p []byte) (n int, err error) {
		errTxt := string(p)
		io2.OutputWarning("", errTxt)
		n = len(p)
		return
	})
}

func NewStdSuccessWriter() io.Writer {
	return NewCallbackWriter(func(p []byte) (n int, err error) {
		outputTxt := string(p)
		io2.OutputInfo("", outputTxt)
		n = len(p)
		return
	})
}

func NewCallbackWriter(callback func(p []byte) (n int, err error)) CallbackWriter {
	return CallbackWriter{Callback: callback}
}

func (cw CallbackWriter) Write(p []byte) (n int, err error) {
	return cw.Callback(p)
}
