#!/bin/bash -e

#
# Copyright (C) 2020 ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#

source "$(dirname $0)/util.sh"

echo "Installing Scylla ($MODE) packages on $PRETTY_NAME..."

rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
yum install -y -q deltarpm
yum update -y -q
yum install -y "${SCYLLA_RPMS[@]}"
