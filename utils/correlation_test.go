package utils

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestGenCorrImg(t *testing.T) {
	dataArr := [][]float64{
		{0.1, 0.2, 0.01, 0.13, 0.0, -0.05, -0.08, -0.1, 0.1, 0.2},
		{0.01, 0.06, 0.03, 0.03, 0.01, -0.02, -0.01, 0.1, 0.12, 0.04},
		{0.12, 0.03, -0.01, 0.23, 0.14, -0.09, 0.08, -0.01, 0.04, 0.1},
		{0.03, 0.12, 0.04, -0.13, 0.0, -0.01, 0.08, -0.1, 0.06, 0.2},
		{-0.05, 0.1, 0.07, -0.13, 0.06, -0.05, -0.01, -0.06, 0.12, 0.07},
	}
	title := "pearson correlation"
	names := []string{"btc", "eth", "fol", "etc", "eos"}
	corrMat, _, err := CalcCorrMat(len(dataArr[0]), dataArr, false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := GenCorrImg(corrMat, title, names, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, []byte("\x89PNG\r\n\x1a\n")) {
		t.Fatal("generated correlation image is not a PNG")
	}
	path := filepath.Join(t.TempDir(), "correlation.png")
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("generated correlation image is empty")
	}
}
