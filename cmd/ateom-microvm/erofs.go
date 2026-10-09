// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// Container image over EROFS + virtio-pmem + DAX (PROTOTYPE, behind a flag).
//
// Default ("virtiofs"): the host mounts overlay(image, actor upper) and virtiofsd
// serves the merged tree; every file the guest reads is copied into guest RAM
// (its page cache), so into every memory snapshot.
//
// "erofs-pmem": the composed image is packed once per node into an uncompressed
// EROFS file, attached as a read-only virtio-pmem device and mounted in the guest
// with DAX, so its pages stay in the host page cache (shared by every actor on the
// node, not snapshotted). The guest stacks overlay(lower = that EROFS, upper/work =
// the actor's host rootfs-upper dirs served over virtio-fs with --xattr) itself.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/reaper"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
)

const (
	containerLowerVirtiofs  = "virtiofs"
	containerLowerErofsPmem = "erofs-pmem"
)

// defaultContainerLower is overridable at link time
// (-X main.defaultContainerLower=erofs-pmem) so a worker image can flip the
// default without a WorkerPool arg; ATEOM_CONTAINER_LOWER overrides both.
var defaultContainerLower = containerLowerVirtiofs

func containerLower() string {
	if v := os.Getenv("ATEOM_CONTAINER_LOWER"); v != "" {
		return v
	}
	return defaultContainerLower
}

func erofsMode() bool { return containerLower() == containerLowerErofsPmem }

// erofsCacheDir holds the per-image EROFS files, shared by every worker on the
// node (BasePath is the same host dir in every ateom pod).
var erofsCacheDir = filepath.Join(ateompath.BasePath, "erofs-cache")

// pmemAlign is cloud-hypervisor's required virtio-pmem backing size multiple.
const pmemAlign = 2 << 20

// erofsImageKey identifies the composed rootfs a bundle presents: its image
// layers (content-addressed cache dirs), the extra dirs, and the two host-side
// writes that land in the lower before the image is packed (resolv.conf and the
// OCI mountpoints, both identical for every actor of the image on a node).
func erofsImageKey(bundle, bundleRootfs string) (string, error) {
	spec, err := imagecache.ReadSpec(bundle)
	if err != nil {
		return "", err
	}
	resolv, _ := os.ReadFile(filepath.Join(bundleRootfs, "etc", "resolv.conf"))
	h := sha256.New()
	enc := json.NewEncoder(h)
	if err := enc.Encode(struct {
		V      int
		Spec   any
		Resolv string
	}{1, spec, string(resolv)}); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil))[:32], nil
}

// mkfsErofsPath returns an executable mkfs.erofs: the static build shipped in
// the image's kodata (copied once to an exec-able path if ko dropped the mode).
func mkfsErofsPath() (string, error) {
	if p, err := exec.LookPath("mkfs.erofs"); err == nil {
		return p, nil
	}
	src := filepath.Join(os.Getenv("KO_DATA_PATH"), "mkfs.erofs")
	if st, err := os.Stat(src); err == nil && st.Mode()&0o111 != 0 {
		return src, nil
	}
	dst := "/tmp/mkfs.erofs"
	if st, err := os.Stat(dst); err == nil && st.Mode()&0o111 != 0 {
		return dst, nil
	}
	in, err := os.Open(src)
	if err != nil {
		return "", fmt.Errorf("no mkfs.erofs on PATH or in kodata: %w", err)
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	return dst, os.Rename(tmp, dst)
}

// ensureErofsImage returns the node-cached EROFS file for bundleRootfs, building
// it on first use. Builds are serialized per key with a flock (workers on the
// node share the cache) and published with a rename, so a reader only ever sees
// a complete file. The result is byte-for-byte reproducible (fixed UUID, real
// file mtimes, normalized superblock), which restore relies on: the snapshot's
// cloud-hypervisor config maps this path again.
func ensureErofsImage(ctx context.Context, bundle, bundleRootfs string) (string, error) {
	key, err := erofsImageKey(bundle, bundleRootfs)
	if err != nil {
		return "", fmt.Errorf("while keying EROFS image: %w", err)
	}
	if err := os.MkdirAll(erofsCacheDir, 0o755); err != nil {
		return "", err
	}
	img := filepath.Join(erofsCacheDir, key+".erofs")
	if _, err := os.Stat(img); err == nil {
		return img, nil
	}
	lock, err := os.OpenFile(img+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		return "", fmt.Errorf("while locking %q: %w", img, err)
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	if _, err := os.Stat(img); err == nil {
		return img, nil // built by another worker while we waited
	}
	mkfs, err := mkfsErofsPath()
	if err != nil {
		return "", err
	}
	tmp := img + ".tmp"
	_ = os.Remove(tmp)
	t0 := time.Now()
	// Uncompressed (DAX maps file data directly), 4 KiB blocks, no tail packing
	// (inline data would not be block-aligned for DAX). --preserve-mtime keeps the
	// image's file mtimes: a fixed timestamp would make Python treat every shipped
	// .pyc as stale.
	cmd := exec.CommandContext(ctx, mkfs, "--quiet", "-b4096", "-Enoinline_data",
		"-U", uuidFromKey(key), "--preserve-mtime", tmp, bundleRootfs)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := reaper.Run(cmd); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("mkfs.erofs %q: %w (%s)", bundleRootfs, err, strings.TrimSpace(stderr.String()))
	}
	if err := normalizeErofsSuperblock(tmp); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("while normalizing %q: %w", tmp, err)
	}
	st, err := os.Stat(tmp)
	if err != nil {
		return "", err
	}
	if pad := (st.Size() + pmemAlign - 1) / pmemAlign * pmemAlign; pad != st.Size() {
		if err := os.Truncate(tmp, pad); err != nil {
			return "", err
		}
	}
	if err := os.Rename(tmp, img); err != nil {
		return "", err
	}
	slog.InfoContext(ctx, "Built EROFS container image", slog.String("path", img),
		slog.Int64("bytes", st.Size()), slog.Duration("took", time.Since(t0)))
	return img, nil
}

func uuidFromKey(key string) string {
	k := key + strings.Repeat("0", 32)
	return k[0:8] + "-" + k[8:12] + "-" + k[12:16] + "-" + k[16:20] + "-" + k[20:32]
}

// normalizeErofsSuperblock zeroes the superblock build time and recomputes its
// CRC32C, the only bytes two builds of the same tree differ in.
// (SOURCE_DATE_EPOCH would also fix the time but clamps every file's mtime.)
func normalizeErofsSuperblock(path string) error {
	const sbOff = 1024
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	sb := make([]byte, 4096-sbOff)
	if _, err := f.ReadAt(sb, sbOff); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(sb[0:]) != 0xE0F5E1E2 {
		return fmt.Errorf("not an EROFS image")
	}
	compat := binary.LittleEndian.Uint32(sb[8:])
	blksz := 1 << sb[12]
	binary.LittleEndian.PutUint64(sb[24:], 0) // build_time
	binary.LittleEndian.PutUint32(sb[32:], 0) // build_time_nsec
	if compat&0x1 != 0 {                      // EROFS_FEATURE_COMPAT_SB_CHKSUM
		binary.LittleEndian.PutUint32(sb[4:], 0)
		// The kernel verifies crc32c(~0, sb, blksz-1024) without the final xor.
		crc := ^crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), sb[:blksz-sbOff])
		binary.LittleEndian.PutUint32(sb[4:], crc)
	}
	if _, err := f.WriteAt(sb, sbOff); err != nil {
		return err
	}
	return f.Sync()
}
