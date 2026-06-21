//go:build !linux

package fsfilter

func PseudoFilesystemName(_ string) (string, bool, error) {
	return "", false, nil
}
