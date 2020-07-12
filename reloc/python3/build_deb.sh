#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh --reloc-pkg build/release/scylla-python3-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}

RELOC_PKG=build/release/scylla-python3-package.tar.gz
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            RELOC_PKG=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ ! -e $RELOC_PKG ]; then
    echo "$RELOC_PKG does not exist."
    echo "Run ./reloc/python3/build_reloc.sh first."
    exit 1
fi
RELOC_PKG=$(readlink -f $RELOC_PKG)
rm -rf build/debian/scylla-python3-package
mkdir -p build/debian/scylla-python3-package
tar -C build/debian/scylla-python3-package -xpf $RELOC_PKG
cd build/debian/scylla-python3-package

PRODUCT=$(cat scylla-python3/SCYLLA-PRODUCT-FILE)
RELOC_PKG_FULLPATH=$(readlink -f $RELOC_PKG)
RELOC_PKG_BASENAME=$(basename $RELOC_PKG)
SCYLLA_VERSION=$(cat scylla-python3/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat scylla-python3/SCYLLA-RELEASE-FILE)

mv scylla-python3/debian debian

PKG_NAME=$(dpkg-parsechangelog --show-field Source)
# XXX: Drop revision number from version string.
#      Since it always '1', this should be okay for now.
PKG_VERSION=$(dpkg-parsechangelog --show-field Version |sed -e 's/-1$//')
ln -fv $RELOC_PKG_FULLPATH ../"$PKG_NAME"_"$PKG_VERSION".orig.tar.gz

debuild -rfakeroot -us -uc
