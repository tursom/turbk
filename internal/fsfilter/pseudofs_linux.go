//go:build linux

package fsfilter

import (
	"fmt"
	"syscall"
)

const (
	procSuperMagic    = 0x9fa0
	sysfsMagic        = 0x62656572
	devptsSuperMagic  = 0x1cd1
	cgroupSuperMagic  = 0x27e0eb
	cgroup2SuperMagic = 0x63677270
	securityfsMagic   = 0x73636673
	debugfsMagic      = 0x64626720
	tracefsMagic      = 0x74726163
	pstorefsMagic     = 0x6165676c
	bpfMagic          = 0xcafe4a11
	binfmtfsMagic     = 0x42494e4d
	configfsMagic     = 0x62656570
	fusectlMagic      = 0x65735543
	mqueueMagic       = 0x19800202
	nsfsMagic         = 0x6e736673
	selinuxMagic      = 0xf97cff8c
	rpcPipefsMagic    = 0x67596969
	hugetlbfsMagic    = 0x958458f6
	efivarfsMagic     = 0xde5e81e4
)

var pseudoFilesystemNames = map[int64]string{
	procSuperMagic:    "proc",
	sysfsMagic:        "sysfs",
	devptsSuperMagic:  "devpts",
	cgroupSuperMagic:  "cgroup",
	cgroup2SuperMagic: "cgroup2",
	securityfsMagic:   "securityfs",
	debugfsMagic:      "debugfs",
	tracefsMagic:      "tracefs",
	pstorefsMagic:     "pstore",
	bpfMagic:          "bpf",
	binfmtfsMagic:     "binfmt_misc",
	configfsMagic:     "configfs",
	fusectlMagic:      "fusectl",
	mqueueMagic:       "mqueue",
	nsfsMagic:         "nsfs",
	selinuxMagic:      "selinuxfs",
	rpcPipefsMagic:    "rpc_pipefs",
	hugetlbfsMagic:    "hugetlbfs",
	efivarfsMagic:     "efivarfs",
}

func PseudoFilesystemName(path string) (string, bool, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return "", false, err
	}
	name, ok := pseudoFilesystemNames[stat.Type]
	if ok {
		return name, true, nil
	}
	return fmt.Sprintf("0x%x", uint64(stat.Type)), false, nil
}
