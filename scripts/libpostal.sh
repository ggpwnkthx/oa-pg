#!/usr/bin/env bash
set -euo pipefail

# Build & install libpostal from source with self-cleanup.
#
# What this script does:
#   1) Installs build deps (autotools, snappy, etc.).
#   2) Clones the official libpostal repo to a temp dir.
#   3) Builds and installs to $PREFIX (default /usr/local).
#   4) Downloads/installs model/data files to $DATADIR.
#   5) Runs ldconfig and verifies install (pkg-config + CLI).
#   6) Removes the cloned source directory on success.
#
# Environment overrides:
#   PREFIX=/usr/local                # install prefix for headers/libs/bin
#   DATADIR=/opt/libpostal           # where data/models live (several GB)
#   REPO=https://github.com/openvenues/libpostal.git
#   BRANCH=master                    # or a tag (e.g., v1.1)
#   JOBS=$(nproc)                    # parallel make jobs
#   MODEL=                           # set to 'senzing' for alternative model
#   DISABLE_SSE2=0                   # set 1 on Apple/ARM if needed
#   KEEP_SOURCE=0                    # set 1 to skip self-cleanup
#
# Usage:
#   bash install_libpostal.sh
#
# Notes:
# - The official README recommends installing deps:
#     curl build-essential autoconf automake libtool pkg-config
#   and running ldconfig after install. :contentReference[oaicite:1]{index=1}
# - Data is stored under --datadir; keep this persistent. :contentReference[oaicite:2]{index=2}

PREFIX="${PREFIX:-/usr/local}"
DATADIR="${DATADIR:-/opt/libpostal}"
REPO="${REPO:-https://github.com/openvenues/libpostal.git}"
BRANCH="${BRANCH:-master}"
JOBS="${JOBS:-$( (command -v nproc >/dev/null 2>&1 && nproc) || echo 2)}"
MODEL="${MODEL:-}"
DISABLE_SSE2="${DISABLE_SSE2:-0}"
KEEP_SOURCE="${KEEP_SOURCE:-0}"

say()  { printf "\033[1;32m==>\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[!]\033[0m %s\n" "$*"; }
die()  { printf "\033[1;31m[x]\033[0m %s\n" "$*"; exit 1; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

sudo_cmd() {
  if [ "$EUID" -ne 0 ]; then
    need_cmd sudo
    echo "sudo"
  else
    echo ""
  fi
}

# Temporary source directory
SRC_DIR="$(mktemp -d -t libpostal-src-XXXXXX)"

cleanup() {
  if [ "${KEEP_SOURCE}" = "1" ]; then
    warn "KEEP_SOURCE=1 set; leaving source at: ${SRC_DIR}"
    return
  fi
  if [ -n "${SRC_DIR:-}" ] && [ -d "${SRC_DIR}" ]; then
    rm -rf "${SRC_DIR}" || true
  fi
}
trap 'warn "Install interrupted."; warn "Source left at: ${SRC_DIR}"' INT TERM
trap 'cleanup' EXIT

say "Installing build dependencies (requires sudo)"
SUDO="$(sudo_cmd)"
$SUDO apt-get update -y
$SUDO apt-get install -y --no-install-recommends \
  git curl ca-certificates \
  build-essential autoconf automake libtool pkg-config \
  libsnappy-dev

say "Creating data directory: ${DATADIR}"
$SUDO mkdir -p "${DATADIR}"
$SUDO chown "$(id -u)":"$(id -g)" "${DATADIR}" || true

say "Cloning libpostal → ${SRC_DIR}"
git clone --depth 1 --branch "${BRANCH}" "${REPO}" "${SRC_DIR}"
cd "${SRC_DIR}"

# Optional: clean if reusing a tree (per README)
if [ -f Makefile ]; then
  make distclean || true
fi

say "Bootstrapping (autotools)"
need_cmd ./bootstrap.sh
./bootstrap.sh

CONFIG_FLAGS=(--prefix="${PREFIX}" --datadir="${DATADIR}")
if [ -n "${MODEL}" ]; then
  # Alternative Senzing model supported by upstream configure. :contentReference[oaicite:3]{index=3}
  CONFIG_FLAGS+=(MODEL="${MODEL}")
fi
if [ "${DISABLE_SSE2}" = "1" ]; then
  # Recommended for Apple/ARM per README; safe on other ARM if needed. :contentReference[oaicite:4]{index=4}
  CONFIG_FLAGS+=(--disable-sse2)
fi

say "Configuring: ./configure ${CONFIG_FLAGS[*]}"
./configure "${CONFIG_FLAGS[@]}"

say "Building (JOBS=${JOBS})"
make -j"${JOBS}"

say "Installing to ${PREFIX} (requires sudo)"
$SUDO make install

# Per README, run ldconfig on Linux so dynamic linker sees the new lib. :contentReference[oaicite:5]{index=5}
if command -v ldconfig >/dev/null 2>&1; then
  say "Running ldconfig"
  $SUDO ldconfig
fi

# ---------- Verification ----------
say "Verifying installation"

# pkg-config should know about libpostal after install (libpostal.pc installed). :contentReference[oaicite:6]{index=6}
if pkg-config --exists libpostal; then
  CFLAGS="$(pkg-config --cflags libpostal || true)"
  LDFLAGS="$(pkg-config --libs libpostal || true)"
  say "pkg-config ok:
    CFLAGS: ${CFLAGS}
    LDFLAGS: ${LDFLAGS}"
else
  warn "pkg-config could not find libpostal. Ensure PKG_CONFIG_PATH includes ${PREFIX}/lib/pkgconfig"
fi

# The CLI programs are installed by 'make install'; common ones include address_parser.
# Upstream shows an interactive "address_parser" example built by make. :contentReference[oaicite:7]{index=7}
BIN_FOUND=0
for candidate in "${PREFIX}/bin/address_parser" "${PREFIX}/bin/expand" "${PREFIX}/bin/parse-address"; do
  if [ -x "${candidate}" ]; then
    say "Found CLI: ${candidate}"
    BIN_FOUND=1
  fi
done
if [ "${BIN_FOUND}" -eq 0 ]; then
  warn "No CLI binaries detected under ${PREFIX}/bin. This can still be OK if you only need the library."
fi

# Quick smoke test if address_parser exists and data present
if [ -x "${PREFIX}/bin/address_parser" ]; then
  # Ensure some .dat exist in DATADIR (models)
  if compgen -G "${DATADIR}/*.dat" >/dev/null; then
    printf "1600 Pennsylvania Ave NW, Washington, DC 20500\n" | "${PREFIX}/bin/address_parser" >/dev/null 2>&1 || true
    say "address_parser ran (smoke test)."
  else
    warn "No *.dat found in ${DATADIR}. If needed, re-run data install:
      (cd ${SRC_DIR} && ${SUDO} make install-data)"
  fi
fi

say "Install complete."

# ---------- Self-cleanup ----------
if [ "${KEEP_SOURCE}" != "1" ]; then
  say "Removing source directory: ${SRC_DIR}"
  # Remove trap to avoid double-message
  trap - EXIT
  rm -rf "${SRC_DIR}" || true
  say "Cleanup done."
fi

say "Done.

If pkg-config wasn't found automatically:
  export PKG_CONFIG_PATH=${PREFIX}/lib/pkgconfig:\${PKG_CONFIG_PATH}
  pkg-config --cflags --libs libpostal
"
