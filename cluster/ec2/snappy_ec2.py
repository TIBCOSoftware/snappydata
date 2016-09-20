#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script is taken from 
# https://github.com/amplab/spark-ec2/blob/branch-1.6/spark_ec2.py
# with modifications.

from __future__ import division, print_function, with_statement

import codecs
import hashlib
import itertools
import logging
import os
import os.path
import pipes
import random
import re
import shutil
import string
from stat import S_IRUSR
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import warnings
from datetime import datetime
from optparse import OptionParser
from sys import stderr

if sys.version < "3":
    from urllib2 import urlopen, Request, HTTPError
else:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
    raw_input = input
    xrange = range


SNAPPY_EC2_VERSION = "0.1"
SNAPPY_EC2_DIR = os.path.dirname(os.path.realpath(__file__))
SNAPPY_AWS_CONF_DIR = SNAPPY_EC2_DIR + "/deploy/home/ec2-user/snappydata"
SNAPPYDATA_UI_PORT = ""
LOCATOR_CLIENT_PORT = "1527"

DEFAULT_SNAPPY_VERSION = "LATEST"

# Amazon Linux AMIs 2016.03 for EBS-backed HVM
HVM_AMI_MAP = {
    "ap-northeast-1": "ami-374db956",
    "ap-northeast-2": "ami-2b408b45",
    "ap-south-1": "ami-ffbdd790",
    "ap-southeast-1": "ami-a59b49c6",
    "ap-southeast-2": "ami-dc361ebf",
    "eu-central-1": "ami-ea26ce85",
    "eu-west-1": "ami-f9dd458a",
    "sa-east-1": "ami-6dd04501",
    "us-east-1": "ami-6869aa05",
    "us-west-1": "ami-31490d51",
    "us-west-2": "ami-7172b611"
}


def setup_external_libs(libs):
    """
    Download external libraries from PyPI to SNAPPY_EC2_DIR/lib/ and prepend them to our PATH.
    """
    PYPI_URL_PREFIX = "https://pypi.python.org/packages/source"
    SPARK_EC2_LIB_DIR = os.path.join(SNAPPY_EC2_DIR, "lib")

    if not os.path.exists(SPARK_EC2_LIB_DIR):
        print("Downloading external libraries that snappy-ec2 needs from PyPI to {path}...".format(
            path=SPARK_EC2_LIB_DIR
        ))
        print("This should be a one-time operation.")
        os.mkdir(SPARK_EC2_LIB_DIR)

    for lib in libs:
        versioned_lib_name = "{n}-{v}".format(n=lib["name"], v=lib["version"])
        lib_dir = os.path.join(SPARK_EC2_LIB_DIR, versioned_lib_name)

        if not os.path.isdir(lib_dir):
            tgz_file_path = os.path.join(SPARK_EC2_LIB_DIR, versioned_lib_name + ".tar.gz")
            print(" - Downloading {lib}...".format(lib=lib["name"]))
            download_stream = urlopen(
                "{prefix}/{first_letter}/{lib_name}/{lib_name}-{lib_version}.tar.gz".format(
                    prefix=PYPI_URL_PREFIX,
                    first_letter=lib["name"][:1],
                    lib_name=lib["name"],
                    lib_version=lib["version"]
                )
            )
            with open(tgz_file_path, "wb") as tgz_file:
                tgz_file.write(download_stream.read())
            with open(tgz_file_path, "rb") as tar:
                if hashlib.md5(tar.read()).hexdigest() != lib["md5"]:
                    print("ERROR: Got wrong md5sum for {lib}.".format(lib=lib["name"]), file=stderr)
                    sys.exit(1)
            tar = tarfile.open(tgz_file_path)
            tar.extractall(path=SPARK_EC2_LIB_DIR)
            tar.close()
            os.remove(tgz_file_path)
            print(" - Finished downloading {lib}.".format(lib=lib["name"]))
        sys.path.insert(1, lib_dir)


# Only PyPI libraries are supported.
external_libs = [
    {
        "name": "boto",
        "version": "2.34.0",
        "md5": "5556223d2d0cc4d06dd4829e671dcecd"
    }
]

setup_external_libs(external_libs)

import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2

import webbrowser

class UsageError(Exception):
    pass


# Configure and parse our command-line arguments
def parse_args():
    parser = OptionParser(
        prog="snappy-ec2",
        version="%prog {v}".format(v=SNAPPY_EC2_VERSION),
        usage="%prog [options] <action> <cluster_name>\n\n"
        + "<action> can be: launch, destroy, login, stop, start, get-locator, get-lead, reboot-cluster")

    parser.add_option(
        "-s", "--stores", type="int", default=1,
        help="Number of stores to launch (default: %default)")
    parser.add_option(
        "--locators", type="int", default=1,
        help="Number of locator nodes to launch (default: %default)")
    parser.add_option(
        "--leads", type="int", default=1,
        help="Number of lead nodes to launch (default: %default)")
    parser.add_option(
        "-w", "--wait", type="int",
        help="DEPRECATED (no longer necessary) - Seconds to wait for nodes to start")
    parser.add_option(
        "-k", "--key-pair",
        help="Name of the key pair to use on instances")
    parser.add_option(
        "-i", "--identity-file",
        help="SSH private key file to use for logging into instances")
    parser.add_option(
        "-p", "--profile", default=None,
        help="If you have multiple profiles (AWS or boto config), you can configure " +
             "additional, named profiles by using this option (default: %default)")
    parser.add_option(
        "-t", "--instance-type", default="m3.large",
        help="Type of instance to launch (default: %default). " +
             "WARNING: must be 64-bit; small instances won't work")
    parser.add_option(
        "--locator-instance-type", default="",
        help="Locator instance type (leave empty for same as instance-type)")
    parser.add_option(
        "-r", "--region", default="us-east-1",
        help="EC2 region used to launch instances in, or to find them in (default: %default)")
    parser.add_option(
        "-z", "--zone", default="",
        help="Availability zone to launch instances in, or 'all' to spread " +
             "stores across multiple (an additional $0.01/Gb for bandwidth" +
             "between zones applies) (default: a single zone chosen at random)")
    parser.add_option(
        "-a", "--ami",
        help="Amazon Machine Image ID to use")
    parser.add_option(
        "-v", "--snappydata-version", default=DEFAULT_SNAPPY_VERSION,
        help="Version of SnappyData to use: 'X.Y.Z' (default: %default)")
    parser.add_option(
        "--with-zeppelin", default="embedded",
        help="Launch Apache Zeppelin server with the cluster." + 
             " Use 'embedded' to launch it on lead node and 'non-embedded' to launch it on a separate instance.")
    parser.add_option(
        "--deploy-root-dir",
        default=None,
        help="A directory to copy into / on the first master. " +
             "Must be absolute. Note that a trailing slash is handled as per rsync: " +
             "If you omit it, the last directory of the --deploy-root-dir path will be created " +
             "in / before copying its contents. If you append the trailing slash, " +
             "the directory is not created and its contents are copied directly into /. " +
             "(default: %default).")
    parser.add_option(
        "-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
        help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
             "the given local address (for use with login)")
    parser.add_option(
        "--resume", action="store_true", default=False,
        help="Resume installation on a previously launched cluster " +
             "(for debugging)")
    parser.add_option(
        "--ebs-vol-size", metavar="SIZE", type="int", default=0,
        help="Size (in GB) of each EBS volume.")
    parser.add_option(
        "--ebs-vol-type", default="standard",
        help="EBS volume type (e.g. 'gp2', 'standard').")
    parser.add_option(
        "--ebs-vol-num", type="int", default=1,
        help="Number of EBS volumes to attach to each node as /vol[x]. " +
             "The volumes will be deleted when the instances terminate. " +
             "Only possible on EBS-backed AMIs. " +
             "EBS volumes are only attached if --ebs-vol-size > 0. " +
             "Only support up to 8 EBS volumes.")
    parser.add_option(
        "--placement-group", type="string", default=None,
        help="Which placement group to try and launch " +
             "instances into. Assumes placement group is already " +
             "created.")
    parser.add_option(
        "--spot-price", metavar="PRICE", type="float",
        help="If specified, launch stores as spot instances with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "-u", "--user", default="ec2-user",
        help="The SSH user you want to connect as (default: %default)")
    parser.add_option(
        "--delete-groups", action="store_true", default=False,
        help="When destroying a cluster, delete the security groups that were created")
    parser.add_option(
        "--use-existing-locator", action="store_true", default=False,
        help="Launch fresh stores, but use an existing stopped locator if possible")
    parser.add_option(
        "--user-data", type="string", default="",
        help="Path to a user-data file (most AMIs interpret this as an initialization script)")
    parser.add_option(
        "--authorized-address", type="string", default="0.0.0.0/0",
        help="Address to authorize on created security groups (default: %default)")
    parser.add_option(
        "--additional-security-group", type="string", default="",
        help="Additional security group to place the machines in")
    parser.add_option(
        "--additional-tags", type="string", default="",
        help="Additional tags to set on the machines; tags are comma-separated, while name and " +
             "value are colon separated; ex: \"Task:MySnappyProject,Env:production\"")
    parser.add_option(
        "--copy-aws-credentials", action="store_true", default=False,
        help="Add AWS credentials to hadoop configuration to allow Snappy to access S3")
    parser.add_option(
        "--subnet-id", default=None,
        help="VPC subnet to launch instances in")
    parser.add_option(
        "--vpc-id", default=None,
        help="VPC to launch instances in")
    parser.add_option(
        "--private-ips", action="store_true", default=False,
        help="Use private IPs for instances rather than public if VPC/subnet " +
             "requires that.")
    parser.add_option(
        "--instance-initiated-shutdown-behavior", default="stop",
        choices=["stop", "terminate"],
        help="Whether instances should terminate when shut down or just stop")
    parser.add_option(
        "--instance-profile-name", default=None,
        help="IAM profile name to launch instances under")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    # Boto config check
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    home_dir = os.getenv('HOME')
    if home_dir is None or not os.path.isfile(home_dir + '/.boto'):
        if not os.path.isfile('/etc/boto.cfg'):
            # If there is no boto config, check aws credentials
            if not os.path.isfile(home_dir + '/.aws/credentials'):
                if os.getenv('AWS_ACCESS_KEY_ID') is None:
                    print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set",
                          file=stderr)
                    sys.exit(1)
                if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
                    print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set",
                          file=stderr)
                    sys.exit(1)

    if opts.with_zeppelin is not None:
        print("Option --with-zeppelin specified. The latest SnappyData version will be used.")
        opts.snappydata_version = "LATEST"

    return (opts, action, cluster_name)


# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name, vpc_id):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, "Snappy EC2 group", vpc_id)


# Source: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
# Last Updated: 2015-06-19
# For easy maintainability, please keep this manually-inputted dictionary sorted by key.
EC2_INSTANCE_TYPES = {
    "c1.medium":   "pvm",
    "c1.xlarge":   "pvm",
    "c3.large":    "hvm",
    "c3.xlarge":   "hvm",
    "c3.2xlarge":  "hvm",
    "c3.4xlarge":  "hvm",
    "c3.8xlarge":  "hvm",
    "c4.large":    "hvm",
    "c4.xlarge":   "hvm",
    "c4.2xlarge":  "hvm",
    "c4.4xlarge":  "hvm",
    "c4.8xlarge":  "hvm",
    "cc1.4xlarge": "hvm",
    "cc2.8xlarge": "hvm",
    "cg1.4xlarge": "hvm",
    "cr1.8xlarge": "hvm",
    "d2.xlarge":   "hvm",
    "d2.2xlarge":  "hvm",
    "d2.4xlarge":  "hvm",
    "d2.8xlarge":  "hvm",
    "g2.2xlarge":  "hvm",
    "g2.8xlarge":  "hvm",
    "hi1.4xlarge": "pvm",
    "hs1.8xlarge": "pvm",
    "i2.xlarge":   "hvm",
    "i2.2xlarge":  "hvm",
    "i2.4xlarge":  "hvm",
    "i2.8xlarge":  "hvm",
    "m1.small":    "pvm",
    "m1.medium":   "pvm",
    "m1.large":    "pvm",
    "m1.xlarge":   "pvm",
    "m2.xlarge":   "pvm",
    "m2.2xlarge":  "pvm",
    "m2.4xlarge":  "pvm",
    "m3.medium":   "hvm",
    "m3.large":    "hvm",
    "m3.xlarge":   "hvm",
    "m3.2xlarge":  "hvm",
    "m4.large":    "hvm",
    "m4.xlarge":   "hvm",
    "m4.2xlarge":  "hvm",
    "m4.4xlarge":  "hvm",
    "m4.10xlarge": "hvm",
    "r3.large":    "hvm",
    "r3.xlarge":   "hvm",
    "r3.2xlarge":  "hvm",
    "r3.4xlarge":  "hvm",
    "r3.8xlarge":  "hvm",
    "t1.micro":    "pvm",
    "t2.micro":    "hvm",
    "t2.small":    "hvm",
    "t2.medium":   "hvm",
    "t2.large":    "hvm",
}


# Attempt to resolve an appropriate AMI given the architecture and region of the request.
def get_ami(opts):
    if opts.instance_type in EC2_INSTANCE_TYPES:
        instance_type = EC2_INSTANCE_TYPES[opts.instance_type]
    else:
        instance_type = "hvm"
        print("Don't recognize %s, assuming virtualization type is hvm" % opts.instance_type, file=stderr)

    ami = HVM_AMI_MAP[opts.region]
    print("Found AMI: " + ami)
    return ami


def read_ports_from_conf(fname, patterns):
    global SNAPPYDATA_UI_PORT
    global LOCATOR_CLIENT_PORT
    ports = []

    if os.path.exists(fname):
        with open(fname) as conffile:
            filedata = conffile.read()
            for pattern in patterns:
                m = re.search('(?<=%s)\w+' % pattern, filedata)
                if m is not None:
                    ports.append(m.group(0))
                    if pattern == "-spark.ui.port=":
                        SNAPPYDATA_UI_PORT = m.group(0)
                    if pattern == "-client-port=" and fname == SNAPPY_AWS_CONF_DIR + '/locators':
                        LOCATOR_CLIENT_PORT = m.group(0)
            conffile.close()
    return ports

def retrieve_ports():
    # files = ['servers', 'locators', 'leads'] include snappy-env.sh too?
    # patterns = ['-client-port=', '-peer-discovery-port=', '-thrift-server-port=', '-spark.shuffle.service.port=', '-spark.ui.port=']

    locator_ports = read_ports_from_conf(SNAPPY_AWS_CONF_DIR + '/locators', ['-client-port=', '-thrift-server-port=', '-peer-discovery-port=', '-jmx-manager-http-port=', '-jmx-manager-port='])
    server_ports = read_ports_from_conf(SNAPPY_AWS_CONF_DIR + '/servers', ['-client-port=', '-thrift-server-port='])
    lead_ports = read_ports_from_conf(SNAPPY_AWS_CONF_DIR + '/leads', ['-peer-discovery-port=', '-spark.shuffle.service.port=', '-spark.ui.port='])
    return (locator_ports, server_ports, lead_ports)


# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the master and stores
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    user_data_content = None
    if opts.user_data:
        with open(opts.user_data) as user_data_file:
            user_data_content = user_data_file.read()

    (locator_ports, server_ports, lead_ports) = retrieve_ports()

    print("Setting up security groups...")
    locator_group = get_or_make_group(conn, cluster_name + "-locator", opts.vpc_id)
    lead_group = get_or_make_group(conn, cluster_name + "-lead", opts.vpc_id)
    store_group = get_or_make_group(conn, cluster_name + "-stores", opts.vpc_id)
    if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
        zeppelin_group = get_or_make_group(conn, cluster_name + "-zeppelin", opts.vpc_id)
    authorized_address = opts.authorized_address
    if locator_group.rules == []:  # Group was just now created
        if opts.vpc_id is None:
            locator_group.authorize(src_group=locator_group)
            locator_group.authorize(src_group=lead_group)
            locator_group.authorize(src_group=store_group)
            if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
                locator_group.authorize(src_group=zeppelin_group)
        else:
            locator_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=locator_group)
            locator_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=locator_group)
            locator_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=locator_group)
            locator_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=lead_group)
            locator_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=lead_group)
            locator_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=lead_group)
            locator_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=store_group)
            locator_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=store_group)
            locator_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=store_group)
            if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
                locator_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                       src_group=zeppelin_group)
                locator_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                       src_group=zeppelin_group)
                locator_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                       src_group=zeppelin_group)
        locator_group.authorize('tcp', 22, 22, authorized_address)
        locator_group.authorize('tcp', 8080, 8081, authorized_address)
        locator_group.authorize('tcp', 18080, 18080, authorized_address)
        locator_group.authorize('tcp', 19999, 19999, authorized_address)
        locator_group.authorize('tcp', 50030, 50030, authorized_address)
        locator_group.authorize('tcp', 50070, 50070, authorized_address)
        locator_group.authorize('tcp', 60070, 60070, authorized_address)
        locator_group.authorize('tcp', 4040, 4045, authorized_address)
        # HDFS NFS gateway requires 111,2049,4242 for tcp & udp
        locator_group.authorize('tcp', 111, 111, authorized_address)
        locator_group.authorize('udp', 111, 111, authorized_address)
        locator_group.authorize('tcp', 2049, 2049, authorized_address)
        locator_group.authorize('udp', 2049, 2049, authorized_address)
        locator_group.authorize('tcp', 4242, 4242, authorized_address)
        locator_group.authorize('udp', 4242, 4242, authorized_address)
        # RM in YARN mode uses 8088
        locator_group.authorize('tcp', 8088, 8088, authorized_address)
        # SnappyData netserver uses this port to listen to clients by default
        locator_group.authorize('tcp', 1527, 1527, authorized_address)
        # Port used by Pulse UI
        locator_group.authorize('tcp', 7070, 7070, authorized_address)
        # JMX manager port
        locator_group.authorize('tcp', 1099, 1099, authorized_address)
        # Default locator port for peer discovery
        locator_group.authorize('tcp', 10334, 10334, authorized_address)
        for port in locator_ports:
            locator_group.authorize('tcp', int(port), int(port), authorized_address)
    # TODO Existing groups are not changed. Document it.
    if lead_group.rules == []:  # Group was just now created
        if opts.vpc_id is None:
            lead_group.authorize(src_group=lead_group)
            lead_group.authorize(src_group=locator_group)
            lead_group.authorize(src_group=store_group)
            if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
                lead_group.authorize(src_group=zeppelin_group)
        else:
            lead_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=lead_group)
            lead_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=lead_group)
            lead_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=lead_group)
            lead_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=locator_group)
            lead_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=locator_group)
            lead_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=locator_group)
            lead_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=store_group)
            lead_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=store_group)
            lead_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=store_group)
            if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
                lead_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                       src_group=zeppelin_group)
                lead_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                       src_group=zeppelin_group)
                lead_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                       src_group=zeppelin_group)
        lead_group.authorize('tcp', 22, 22, authorized_address)
        lead_group.authorize('tcp', 8080, 8081, authorized_address)
        lead_group.authorize('tcp', 18080, 18080, authorized_address)
        lead_group.authorize('tcp', 19999, 19999, authorized_address)
        lead_group.authorize('tcp', 50030, 50030, authorized_address)
        lead_group.authorize('tcp', 50070, 50070, authorized_address)
        lead_group.authorize('tcp', 60070, 60070, authorized_address)
        lead_group.authorize('tcp', 4040, 4045, authorized_address)
        # HDFS NFS gateway requires 111,2049,4242 for tcp & udp
        lead_group.authorize('tcp', 111, 111, authorized_address)
        lead_group.authorize('udp', 111, 111, authorized_address)
        lead_group.authorize('tcp', 2049, 2049, authorized_address)
        lead_group.authorize('udp', 2049, 2049, authorized_address)
        lead_group.authorize('tcp', 4242, 4242, authorized_address)
        lead_group.authorize('udp', 4242, 4242, authorized_address)
        # RM in YARN mode uses 8088
        lead_group.authorize('tcp', 8088, 8088, authorized_address)
        # SnappyData netserver uses this port to listen to clients by default
        lead_group.authorize('tcp', 1527, 1527, authorized_address)
        # Default port of Spark JobServer
        lead_group.authorize('tcp', 8090, 8090, authorized_address)
        for port in lead_ports:
            lead_group.authorize('tcp', int(port), int(port), authorized_address)
    if store_group.rules == []:  # Group was just now created
        if opts.vpc_id is None:
            store_group.authorize(src_group=locator_group)
            store_group.authorize(src_group=store_group)
            store_group.authorize(src_group=lead_group)
            if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
                store_group.authorize(src_group=zeppelin_group)
        else:
            store_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                  src_group=locator_group)
            store_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                  src_group=locator_group)
            store_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                  src_group=locator_group)
            store_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                  src_group=store_group)
            store_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                  src_group=store_group)
            store_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                  src_group=store_group)
            store_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                  src_group=lead_group)
            store_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                  src_group=lead_group)
            store_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                  src_group=lead_group)
            if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
                store_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                      src_group=zeppelin_group)
                store_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                      src_group=zeppelin_group)
                store_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                      src_group=zeppelin_group)
        store_group.authorize('tcp', 22, 22, authorized_address)
        store_group.authorize('tcp', 8080, 8081, authorized_address)
        store_group.authorize('tcp', 50060, 50060, authorized_address)
        store_group.authorize('tcp', 50075, 50075, authorized_address)
        store_group.authorize('tcp', 60060, 60060, authorized_address)
        store_group.authorize('tcp', 60075, 60075, authorized_address)
        # SnappyData netserver uses this port to listen to clients by default
        store_group.authorize('tcp', 1527, 1527, authorized_address)
        for port in server_ports:
            store_group.authorize('tcp', int(port), int(port), authorized_address)
    if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
        if zeppelin_group.rules == []:  # Group was just now created
            if opts.vpc_id is None:
                zeppelin_group.authorize(src_group=locator_group)
                zeppelin_group.authorize(src_group=lead_group)
                zeppelin_group.authorize(src_group=store_group)
                zeppelin_group.authorize(src_group=zeppelin_group)
            else:
                zeppelin_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                       src_group=zeppelin_group)
                zeppelin_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                       src_group=zeppelin_group)
                zeppelin_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                       src_group=zeppelin_group)
                zeppelin_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                       src_group=locator_group)
                zeppelin_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                       src_group=locator_group)
                zeppelin_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                       src_group=locator_group)
                zeppelin_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                       src_group=lead_group)
                zeppelin_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                       src_group=lead_group)
                zeppelin_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                       src_group=lead_group)
                zeppelin_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                       src_group=store_group)
                zeppelin_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                       src_group=store_group)
                zeppelin_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                       src_group=store_group)
            zeppelin_group.authorize('tcp', 22, 22, authorized_address)
            zeppelin_group.authorize('tcp', 8080, 8080, authorized_address)

    # Check if instances are already running in our groups
    existing_locators, existing_leads, existing_stores, existing_zeppelin = get_existing_cluster(conn,
                                                             opts, cluster_name,
                                                             die_on_error=False)
    if existing_leads or existing_stores or existing_zeppelin or (existing_locators and not opts.use_existing_locator):
        zp_name = ""
        if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
            zp_name = zeppelin_group.name
        print("ERROR: There are already instances running in group %s, %s, %s or %s" %
              (locator_group.name, lead_group.name, zp_name, store_group.name), file=stderr)
        sys.exit(1)

    # Figure out the AMI
    # TODO User provided AMI may not work.
    if opts.ami is None:
        opts.ami = get_ami(opts)

    # we use group ids to work around https://github.com/boto/boto/issues/350
    additional_group_ids = []
    if opts.additional_security_group:
        additional_group_ids = [sg.id
                                for sg in conn.get_all_security_groups()
                                if opts.additional_security_group in (sg.name, sg.id)]
    print("Launching instances...")

    try:
        image = conn.get_all_images(image_ids=[opts.ami])[0]
    except:
        print("Could not find AMI " + opts.ami, file=stderr)
        sys.exit(1)

    # Create block device mapping so that we can add EBS volumes if asked to.
    # The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
    block_map = BlockDeviceMapping()
    if opts.ebs_vol_size > 0:
        for i in range(opts.ebs_vol_num):
            device = EBSBlockDeviceType()
            device.size = opts.ebs_vol_size
            device.volume_type = opts.ebs_vol_type
            device.delete_on_termination = True
            block_map["/dev/sd" + chr(ord('s') + i)] = device

    # AWS ignores the AMI-specified block device mapping for M3 (see SPARK-3342).
    if opts.instance_type.startswith('m3.'):
        for i in range(get_num_disks(opts.instance_type)):
            dev = BlockDeviceType()
            dev.ephemeral_name = 'ephemeral%d' % i
            # The first ephemeral drive is /dev/sdb.
            name = '/dev/sd' + string.ascii_letters[i + 1]
            block_map[name] = dev

    server_nodes = []
    # Launch servers
    if opts.spot_price is not None:
        # Launch spot instances with the requested price
        print("Requesting %d stores as spot instances with price $%.3f" %
              (opts.stores, opts.spot_price))
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        my_req_ids = []
        for zone in zones:
            num_stores_this_zone = get_partition(opts.stores, num_zones, i)
            store_reqs = conn.request_spot_instances(
                price=opts.spot_price,
                image_id=opts.ami,
                launch_group="launch-group-%s" % cluster_name,
                placement=zone,
                count=num_stores_this_zone,
                key_name=opts.key_pair,
                security_group_ids=[store_group.id] + additional_group_ids,
                instance_type=opts.instance_type,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                user_data=user_data_content,
                instance_profile_name=opts.instance_profile_name)
            my_req_ids += [req.id for req in store_reqs]
            i += 1

        print("Waiting for spot instances to be granted...")
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                active_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        active_instance_ids.append(id_to_req[i].instance_id)
                if len(active_instance_ids) == opts.stores:
                    print("All %d stores granted" % opts.stores)
                    reservations = conn.get_all_reservations(active_instance_ids)
                    for r in reservations:
                        server_nodes += r.instances
                    break
                else:
                    print("%d of %d stores granted, waiting longer" % (
                        len(active_instance_ids), opts.stores))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            running = len(locator_nodes) + len(server_nodes) + len(lead_nodes) + len(zeppelin_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
    else:
        # Launch non-spot instances
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        for zone in zones:
            num_stores_this_zone = get_partition(opts.stores, num_zones, i)
            if num_stores_this_zone > 0:
                store_res = image.run(
                    key_name=opts.key_pair,
                    security_group_ids=[store_group.id] + additional_group_ids,
                    instance_type=opts.instance_type,
                    placement=zone,
                    min_count=num_stores_this_zone,
                    max_count=num_stores_this_zone,
                    block_device_map=block_map,
                    subnet_id=opts.subnet_id,
                    placement_group=opts.placement_group,
                    user_data=user_data_content,
                    instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                    instance_profile_name=opts.instance_profile_name)
                server_nodes += store_res.instances
                print("Launched {s} store{plural_s} in {z}, regid = {r}".format(
                      s=num_stores_this_zone,
                      plural_s=('' if num_stores_this_zone == 1 else 's'),
                      z=zone,
                      r=store_res.id))
            i += 1

    # Launch or resume locators
    if existing_locators:
        print("Starting locators...")
        for inst in existing_locators:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        locator_nodes = existing_locators
    else:
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        locator_nodes = []
        for zone in zones:
            num_locators_this_zone = get_partition(opts.locators, num_zones, i)
            master_type = opts.locator_instance_type
            if master_type == "":
                master_type = opts.instance_type
            if num_locators_this_zone > 0:
                master_res = image.run(
                    key_name=opts.key_pair,
                    security_group_ids=[locator_group.id] + additional_group_ids,
                    instance_type=master_type,
                    placement=zone,
                    min_count=num_locators_this_zone,
                    max_count=num_locators_this_zone,
                    block_device_map=block_map,
                    subnet_id=opts.subnet_id,
                    placement_group=opts.placement_group,
                    user_data=user_data_content,
                    instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                    instance_profile_name=opts.instance_profile_name)
                locator_nodes += master_res.instances
                print("Launched {s} locator{plural_s} in {z}, regid = {r}".format(
                      s=num_locators_this_zone,
                      plural_s=('' if num_locators_this_zone == 1 else 's'),
                      z=zone,
                      r=master_res.id))
            i += 1

    # Launch leads
    zones = get_zones(conn, opts)
    num_zones = len(zones)
    i = 0
    lead_nodes = []
    for zone in zones:
        num_leads_this_zone = get_partition(opts.leads, num_zones, i)
        lead_res = image.run(
            key_name=opts.key_pair,
            security_group_ids=[lead_group.id] + additional_group_ids,
            instance_type=opts.instance_type,
            placement=zone,
            min_count=num_leads_this_zone,
            max_count=num_leads_this_zone,
            block_device_map=block_map,
            subnet_id=opts.subnet_id,
            placement_group=opts.placement_group,
            user_data=user_data_content,
            instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
            instance_profile_name=opts.instance_profile_name)
        lead_nodes += lead_res.instances
        print("Launched {s} lead{plural_s} in {z}, regid = {r}".format(
              s=num_leads_this_zone,
              plural_s=('' if num_leads_this_zone == 1 else 's'),
              z=zone,
              r=lead_res.id))
        i += 1

    zeppelin_nodes = []
    if opts.with_zeppelin is not None and opts.with_zeppelin == "non-embedded":
         # Launch zeppelin server
         zeppelin_res = image.run(
             key_name=opts.key_pair,
             security_group_ids=[zeppelin_group.id] + additional_group_ids,
             instance_type=opts.instance_type,
             placement=zones[0],
             min_count=1,
             max_count=1,
             block_device_map=block_map,
             subnet_id=opts.subnet_id,
             placement_group=opts.placement_group,
             user_data=user_data_content,
             instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
             instance_profile_name=opts.instance_profile_name)
         zeppelin_nodes += zeppelin_res.instances
         print("Launched zeppelin server in {z}, regid = {r}".format(
               z=zones[0],
               r=zeppelin_res.id))

    # This wait time corresponds to SPARK-4983
    print("Waiting for AWS to propagate instance metadata...")
    time.sleep(15)

    # Give the instances descriptive names and set additional tags
    additional_tags = {}
    if opts.additional_tags.strip():
        additional_tags = dict(
            map(str.strip, tag.split(':', 1)) for tag in opts.additional_tags.split(',')
        )

    for master in locator_nodes:
        master.add_tags(
            dict(additional_tags, Name='{cn}-locator-{iid}'.format(cn=cluster_name, iid=master.id))
        )

    for lead in lead_nodes:
        lead.add_tags(
            dict(additional_tags, Name='{cn}-lead-{iid}'.format(cn=cluster_name, iid=lead.id))
        )

    for store in server_nodes:
        store.add_tags(
            dict(additional_tags, Name='{cn}-store-{iid}'.format(cn=cluster_name, iid=store.id))
        )

    for zp in zeppelin_nodes:
        zp.add_tags(
            dict(additional_tags, Name='{cn}-zeppelin-{iid}'.format(cn=cluster_name, iid=zp.id))
        )

    # Return all the instances
    return (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes)


def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and stores.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
          c=cluster_name, r=opts.region))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.

        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        reservations = conn.get_all_reservations(
            filters={"instance.group-name": group_names})
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    locator_instances = get_instances([cluster_name + "-locator"])
    lead_instances = get_instances([cluster_name + "-lead"])
    store_instances = get_instances([cluster_name + "-stores"])
    zeppelin_instances = get_instances([cluster_name + "-zeppelin"])

    if any((locator_instances, lead_instances, store_instances, zeppelin_instances)):
        print("Found {l} locator{plural_l}, {m} lead{plural_m}, {s} store{plural_s}, {z} zeppelin instance.".format(
              l=len(locator_instances),
              plural_l=('' if len(locator_instances) == 1 else 's'),
              m=len(lead_instances),
              plural_m=('' if len(lead_instances) == 1 else 's'),
              s=len(store_instances),
              plural_s=('' if len(store_instances) == 1 else 's'),
              z=len(zeppelin_instances)))

    if not locator_instances and die_on_error:
        print("ERROR: Could not find a locator for cluster {c} in region {r}.".format(
              c=cluster_name, r=opts.region), file=sys.stderr)
        sys.exit(1)

    return (locator_instances, lead_instances, store_instances, zeppelin_instances)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, locator_nodes, lead_nodes, server_nodes, zeppelin_nodes, opts, deploy_ssh_key):
    locator = get_dns_name(locator_nodes[0], opts.private_ips)
    # Read the UI port from conf/leads, if available.
    read_ports_from_conf(SNAPPY_AWS_CONF_DIR + '/leads', ['-spark.ui.port='])
    read_ports_from_conf(SNAPPY_AWS_CONF_DIR + '/locators', ['-client-port='])

    if deploy_ssh_key:
        print("Generating cluster's SSH key on locator...")
        key_setup = """
          [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(locator, opts, key_setup)
        dot_ssh_tar = ssh_read(locator, opts, ['tar', 'c', '.ssh'])
        if len(locator_nodes) > 1:
            print("Transferring cluster's SSH key to other locators...")
        for index in range(len(locator_nodes)):
            if index > 0:
                locator_address = get_dns_name(locator_nodes[index], opts.private_ips)
                print(locator_address)
                ssh_write(locator_address, opts, ['tar', 'x'], dot_ssh_tar)
        print("Transferring cluster's SSH key to leads...")
        for lead in lead_nodes:
            lead_address = get_dns_name(lead, opts.private_ips)
            print(lead_address)
            ssh_write(lead_address, opts, ['tar', 'x'], dot_ssh_tar)
        print("Transferring cluster's SSH key to stores...")
        for store in server_nodes:
            store_address = get_dns_name(store, opts.private_ips)
            print(store_address)
            ssh_write(store_address, opts, ['tar', 'x'], dot_ssh_tar)
        for zp in zeppelin_nodes:
            print("Transferring cluster's SSH key to zeppelin server...")
            zeppelin_address = get_dns_name(zp, opts.private_ips)
            print(zeppelin_address)
            ssh_write(zeppelin_address, opts, ['tar', 'x'], dot_ssh_tar)

    # TODO Download aws-setup.sh, etc from public repo instead of pushing them from this machine.

    print("Deploying files to locator...")
    deploy_files(
        conn=conn,
        root_dir=SNAPPY_EC2_DIR + "/" + "deploy",
        opts=opts,
        locator_nodes=locator_nodes,
        lead_nodes=lead_nodes,
        server_nodes=server_nodes,
        zeppelin_nodes=zeppelin_nodes
    )

    if opts.deploy_root_dir is not None:
        print("Deploying {s} to locator...".format(s=opts.deploy_root_dir))
        deploy_user_files(
            root_dir=opts.deploy_root_dir,
            opts=opts,
            locator_nodes=locator_nodes
        )

    print("Running aws-setup on locator...")
    setup_snappy_cluster(locator, opts)
    # TODO If the cluster does not start fully, print so.
    print("Done!")


def setup_snappy_cluster(master, opts):
    ssh(master, opts, "chmod u+x snappydata/aws-setup.sh")
    ssh(master, opts, "snappydata/aws-setup.sh")


def shutdown_snappy_cluster(master, opts):
    ssh(master, opts, "chmod u+x snappydata/aws-shutdown.sh")
    ssh(master, opts, "snappydata/aws-shutdown.sh")


def is_ssh_available(host, opts, print_ssh_output=True):
    """
    Check if SSH is available on a host.
    """
    s = subprocess.Popen(
        ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                             '%s@%s' % (opts.user, host), stringify_command('true')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT  # we pipe stderr through stdout to preserve output order
    )
    cmd_output = s.communicate()[0]  # [1] is stderr, which we redirected to stdout

    if s.returncode != 0 and print_ssh_output:
        # extra leading newline is for spacing in wait_for_cluster_state()
        print(textwrap.dedent("""\n
            Warning: SSH connection error. (This could be temporary.)
            Host: {h}
            SSH return code: {r}
            SSH output: {o}
        """).format(
            h=host,
            r=s.returncode,
            o=cmd_output.strip()
        ))

    return s.returncode == 0


def is_cluster_ssh_available(cluster_instances, opts):
    """
    Check if SSH is available on all the instances in a cluster.
    """
    for i in cluster_instances:
        dns_name = get_dns_name(i, opts.private_ips)
        if not is_ssh_available(host=dns_name, opts=opts):
            return False
    else:
        return True


def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.

    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for cluster to enter '{s}' state.".format(s=cluster_state)
    )
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        for i in cluster_instances:
            i.update()

        max_batch = 100
        statuses = []
        for j in xrange(0, len(cluster_instances), max_batch):
            batch = [i.id for i in cluster_instances[j:j + max_batch]]
            statuses.extend(conn.get_all_instance_status(instance_ids=batch))

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
               all(s.system_status.status == 'ok' for s in statuses) and \
               all(s.instance_status.status == 'ok' for s in statuses) and \
               is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print("Cluster is now in '{s}' state. Waited {t} seconds.".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    ))


# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Last Updated: 2015-06-19
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    disks_by_instance = {
        "c1.medium":   1,
        "c1.xlarge":   4,
        "c3.large":    2,
        "c3.xlarge":   2,
        "c3.2xlarge":  2,
        "c3.4xlarge":  2,
        "c3.8xlarge":  2,
        "c4.large":    0,
        "c4.xlarge":   0,
        "c4.2xlarge":  0,
        "c4.4xlarge":  0,
        "c4.8xlarge":  0,
        "cc1.4xlarge": 2,
        "cc2.8xlarge": 4,
        "cg1.4xlarge": 2,
        "cr1.8xlarge": 2,
        "d2.xlarge":   3,
        "d2.2xlarge":  6,
        "d2.4xlarge":  12,
        "d2.8xlarge":  24,
        "g2.2xlarge":  1,
        "g2.8xlarge":  2,
        "hi1.4xlarge": 2,
        "hs1.8xlarge": 24,
        "i2.xlarge":   1,
        "i2.2xlarge":  2,
        "i2.4xlarge":  4,
        "i2.8xlarge":  8,
        "m1.small":    1,
        "m1.medium":   1,
        "m1.large":    2,
        "m1.xlarge":   4,
        "m2.xlarge":   1,
        "m2.2xlarge":  1,
        "m2.4xlarge":  2,
        "m3.medium":   1,
        "m3.large":    1,
        "m3.xlarge":   2,
        "m3.2xlarge":  2,
        "m4.large":    0,
        "m4.xlarge":   0,
        "m4.2xlarge":  0,
        "m4.4xlarge":  0,
        "m4.10xlarge": 0,
        "r3.large":    1,
        "r3.xlarge":   1,
        "r3.2xlarge":  1,
        "r3.4xlarge":  1,
        "r3.8xlarge":  2,
        "t1.micro":    0,
        "t2.micro":    0,
        "t2.small":    0,
        "t2.medium":   0,
        "t2.large":    0,
    }
    if instance_type in disks_by_instance:
        return disks_by_instance[instance_type]
    else:
        print("WARNING: Don't know number of disks on instance type %s; assuming 1"
              % instance_type, file=stderr)
        return 1


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of locators, leads and stores). Files are only deployed to
# the first lead instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(conn, root_dir, opts, locator_nodes, lead_nodes, server_nodes, zeppelin_nodes):
    active_locator = get_dns_name(locator_nodes[0], opts.private_ips)

    num_disks = get_num_disks(opts.instance_type)
    hdfs_data_dirs = "/mnt/ephemeral-hdfs/data"
    mapred_local_dirs = "/mnt/hadoop/mrlocal"
    spark_local_dirs = "/mnt/spark"
    if num_disks > 1:
        for i in range(2, num_disks + 1):
            hdfs_data_dirs += ",/mnt%d/ephemeral-hdfs/data" % i
            mapred_local_dirs += ",/mnt%d/hadoop/mrlocal" % i
            spark_local_dirs += ",/mnt%d/spark" % i

    cluster_url = "%s:7077" % active_locator

    locator_addresses = [get_dns_name(i, opts.private_ips) for i in locator_nodes]
    lead_addresses = [get_dns_name(i, opts.private_ips) for i in lead_nodes]
    server_addresses = [get_dns_name(i, opts.private_ips) for i in server_nodes]

    zp_mode = "EMBEDDED"
    zeppelin_address = "zeppelin_server"
    if opts.with_zeppelin is not None:
        if opts.with_zeppelin == "non-embedded":
            zp_mode = "NON-EMBEDDED"
            zeppelin_addresses = [get_dns_name(i, opts.private_ips) for i in zeppelin_nodes]
            zeppelin_address = zeppelin_addresses[0]
        else: 
            zeppelin_address = lead_addresses[0]

    template_vars = {
        "locator_list": '\n'.join(locator_addresses),
        "lead_list": '\n'.join(lead_addresses),
        "server_list": '\n'.join(server_addresses),
        "zeppelin_server": zeppelin_address
    }

    if opts.copy_aws_credentials:
        template_vars["aws_access_key_id"] = conn.aws_access_key_id
        template_vars["aws_secret_access_key"] = conn.aws_secret_access_key
    else:
        template_vars["aws_access_key_id"] = ""
        template_vars["aws_secret_access_key"] = ""

    template_vars["locator_client_port"] = LOCATOR_CLIENT_PORT

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitue template parameters in them
    tmp_dir = tempfile.mkdtemp()
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            for idx in range(len(locator_nodes)):
                                text = text.replace("{{LOCATOR_" + str(idx) + "}}", locator_addresses[idx])
                            for idx in range(len(lead_nodes)):
                                text = text.replace("{{LEAD_" + str(idx) + "}}", lead_addresses[idx])
                            for idx in range(len(server_nodes)):
                                text = text.replace("{{SERVER_" + str(idx) + "}}", server_addresses[idx])
                            text = text.replace("{{snappydata_version}}", opts.snappydata_version)
                            text = text.replace("{{EMBEDDED}}", zp_mode)
                            dest.write(text)
                            dest.close()
    # rsync the whole directory over to the locator instance
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/" % (opts.user, active_locator)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


# Deploy a given local directory to a cluster, WITHOUT parameter substitution.
# Note that unlike deploy_files, this works for binary files.
# Also, it is up to the user to add (or not) the trailing slash in root_dir.
# Files are only deployed to the first master instance in the cluster.
#
# root_dir should be an absolute path.
def deploy_user_files(root_dir, opts, master_nodes):
    active_master = get_dns_name(master_nodes[0], opts.private_ips)
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s" % root_dir,
        "%s@%s:/" % (opts.user, active_master)
    ]
    subprocess.check_call(command)


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts


def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)


# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file and "
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)])


def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print("Error {0} while executing remote command, retrying after 30 seconds".
                  format(status), file=stderr)
            time.sleep(30)
            tries = tries + 1


# Gets a list of zones to launch instances in
def get_zones(conn, opts):
    if opts.zone == 'all':
        zones = [z.name for z in conn.get_all_zones()]
    else:
        zones = [opts.zone]
    return zones


# Gets the number of items in a partition
def get_partition(total, num_partitions, current_partitions):
    num_stores_this_zone = total // num_partitions
    if (total % num_partitions) - current_partitions > 0:
        num_stores_this_zone += 1
    return num_stores_this_zone


# Gets the IP address, taking into account the --private-ips flag
def get_ip_address(instance, private_ips=False):
    ip = instance.ip_address if not private_ips else \
        instance.private_ip_address
    return ip


# Gets the DNS name, taking into account the --private-ips flag
def get_dns_name(instance, private_ips=False):
    dns = instance.public_dns_name if not private_ips else \
        instance.private_ip_address
    return dns


def real_main():
    global SNAPPYDATA_UI_PORT
    (opts, action, cluster_name) = parse_args()

    # Input parameter validation

    if opts.wait is not None:
        # NOTE: DeprecationWarnings are silent in 2.7+ by default.
        #       To show them, run Python with the -Wdefault switch.
        # See: https://docs.python.org/3.5/whatsnew/2.7.html
        warnings.warn(
            "This option is deprecated and has no effect. "
            "snappy-ec2 automatically waits as long as necessary for clusters to start up.",
            DeprecationWarning
        )

    if opts.identity_file is not None:
        if not os.path.exists(opts.identity_file):
            print("ERROR: The identity file '{f}' doesn't exist.".format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

        file_mode = os.stat(opts.identity_file).st_mode
        if not (file_mode & S_IRUSR) or not oct(file_mode)[-2:] == '00':
            print("ERROR: The identity file must be accessible only by you.", file=stderr)
            print('You can fix this with: chmod 400 "{f}"'.format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

    if opts.instance_type not in EC2_INSTANCE_TYPES:
        print("Warning: Unrecognized EC2 instance type for instance-type: {t}".format(
              t=opts.instance_type), file=stderr)

    if opts.locator_instance_type != "":
        if opts.locator_instance_type not in EC2_INSTANCE_TYPES:
            print("Warning: Unrecognized EC2 instance type for locator-instance-type: {t}".format(
                  t=opts.locator_instance_type), file=stderr)
        # Since we try instance types even if we can't resolve them, we check if they resolve first
        # and, if they do, see if they resolve to the same virtualization type.
        if opts.instance_type in EC2_INSTANCE_TYPES and \
           opts.locator_instance_type in EC2_INSTANCE_TYPES:
            if EC2_INSTANCE_TYPES[opts.instance_type] != \
               EC2_INSTANCE_TYPES[opts.locator_instance_type]:
                print("Error: snappy-ec2 currently does not support having locators and stores "
                      "with different AMI virtualization types.", file=stderr)
                print("locator instance virtualization type: {t}".format(
                      t=EC2_INSTANCE_TYPES[opts.locator_instance_type]), file=stderr)
                print("store instance virtualization type: {t}".format(
                      t=EC2_INSTANCE_TYPES[opts.instance_type]), file=stderr)
                sys.exit(1)

    # TODO Why the restriction?
    if opts.ebs_vol_num > 8:
        print("ebs-vol-num cannot be greater than 8", file=stderr)
        sys.exit(1)

    if not (opts.deploy_root_dir is None or
            (os.path.isabs(opts.deploy_root_dir) and
             os.path.isdir(opts.deploy_root_dir) and
             os.path.exists(opts.deploy_root_dir))):
        print("--deploy-root-dir must be an absolute path to a directory that exists "
              "on the local file system", file=stderr)
        sys.exit(1)

    try:
        if opts.profile is None:
            conn = ec2.connect_to_region(opts.region)
        else:
            conn = ec2.connect_to_region(opts.region, profile_name=opts.profile)
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)

    # Select an AZ at random if it was not specified.
    if opts.zone == "":
        opts.zone = random.choice(conn.get_all_zones()).name

    if action == "launch":
        if opts.locators <= 0 or opts.stores <= 0 or opts.leads <= 0:
            print("ERROR: You have to start at least one instance of locator, lead and store each.", file=sys.stderr)
            sys.exit(1)
        zeppelin_nodes = []
        if opts.resume:
            (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(conn, opts, cluster_name)
        else:
            (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = launch_cluster(conn, opts, cluster_name)
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(locator_nodes + lead_nodes + server_nodes + zeppelin_nodes),
            cluster_state='ssh-ready'
        )
        setup_cluster(conn, locator_nodes, lead_nodes, server_nodes, zeppelin_nodes, opts, True)
        lead = get_dns_name(lead_nodes[0], opts.private_ips)
        if SNAPPYDATA_UI_PORT == "":
            SNAPPYDATA_UI_PORT = '4040'
        url = "http://%s:%s" % (lead, SNAPPYDATA_UI_PORT)
        print("SnappyData Unified cluster started at %s" % url)
        if opts.with_zeppelin is not None:
            if len(zeppelin_nodes) == 1:
                zp = get_dns_name(zeppelin_nodes[0], opts.private_ips)
            else:
                zp = lead
            url = "http://%s:8080" % zp
            print("Apache Zeppelin server started at %s" % url)
            time.sleep(2)
            webbrowser.open_new_tab(url)

    elif action == "destroy":
        (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(
            conn, opts, cluster_name, die_on_error=False)

        if any(locator_nodes + lead_nodes + server_nodes + zeppelin_nodes):
            print("The following instances will be terminated:")
            for inst in locator_nodes + lead_nodes + server_nodes + zeppelin_nodes:
                print("> %s" % get_dns_name(inst, opts.private_ips))
            print("ALL DATA ON ALL NODES WILL BE LOST!!")

        msg = "Are you sure you want to destroy the cluster {c}? (y/N) ".format(c=cluster_name)
        response = raw_input(msg)
        if response == "y":
            print("Terminating locator instance(s)...")
            for inst in locator_nodes:
                inst.terminate()
            print("Terminating lead instance(s)...")
            for inst in lead_nodes:
                inst.terminate()
            print("Terminating store instance(s)...")
            for inst in server_nodes:
                inst.terminate()
            for inst in zeppelin_nodes:
                print("Terminating Apache Zeppelin instance...")
                inst.terminate()

            # Delete security groups as well
            if opts.delete_groups:
                group_names = [cluster_name + "-locator", cluster_name + "-lead", cluster_name + "-stores"]
                wait_for_cluster_state(
                    conn=conn,
                    opts=opts,
                    cluster_instances=(locator_nodes + lead_nodes + server_nodes + zeppelin_nodes),
                    cluster_state='terminated'
                )
                print("Deleting security groups (this will take some time)...")
                attempt = 1
                while attempt <= 3:
                    print("Attempt %d" % attempt)
                    groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
                    success = True
                    # Delete individual rules in all groups before deleting groups to
                    # remove dependencies between them
                    for group in groups:
                        print("Deleting rules in security group " + group.name)
                        for rule in group.rules:
                            for grant in rule.grants:
                                success &= group.revoke(ip_protocol=rule.ip_protocol,
                                                        from_port=rule.from_port,
                                                        to_port=rule.to_port,
                                                        src_group=grant)

                    # Sleep for AWS eventual-consistency to catch up, and for instances
                    # to terminate
                    time.sleep(30)  # Yes, it does have to be this long :-(
                    for group in groups:
                        try:
                            # It is needed to use group_id to make it work with VPC
                            conn.delete_security_group(group_id=group.id)
                            print("Deleted security group %s" % group.name)
                        except boto.exception.EC2ResponseError:
                            success = False
                            print("Failed to delete security group %s" % group.name)

                    # Unfortunately, group.revoke() returns True even if a rule was not
                    # deleted, so this needs to be rerun if something fails
                    if success:
                        break

                    attempt += 1

                if not success:
                    print("Failed to delete all security groups after 3 tries.")
                    print("Try re-running in a few minutes.")

    elif action == "login":
        (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(conn, opts, cluster_name)
        if not lead_nodes[0].public_dns_name and not opts.private_ips:
            print("Lead has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            lead = get_dns_name(lead_nodes[0], opts.private_ips)
            print("Logging into lead " + lead + "...")
            proxy_opt = []
            if opts.proxy_port is not None:
                proxy_opt = ['-D', opts.proxy_port]
            subprocess.check_call(
                ssh_command(opts) + proxy_opt + ['-t', '-t', "%s@%s" % (opts.user, lead)])

    elif action == "reboot-cluster":
        response = raw_input(
            "Are you sure you want to reboot the cluster " +
            cluster_name + "?\n" +
            "Reboot cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            shutdown_snappy_cluster(get_dns_name(locator_nodes[0], opts.private_ips), opts)
            print("Rebooting cluster instances...")
            cluster_instances=(locator_nodes + lead_nodes + server_nodes + zeppelin_nodes)
            for inst in cluster_instances:
                if inst.state not in ["shutting-down", "terminated"]:
                    print("  Rebooting " + inst.id)
                    inst.reboot()
            # Wait for ssh-ready
            wait_for_cluster_state(
                conn=conn,
                opts=opts,
                cluster_instances=cluster_instances,
                cluster_state='ssh-ready'
            )
            # setup_snappy_cluster(get_dns_name(locator_nodes[0], opts.private_ips), opts)
            setup_cluster(conn, locator_nodes, lead_nodes, server_nodes, zeppelin_nodes, opts, False)

    # TODO
    elif action == "get-locator":
        (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(conn, opts, cluster_name)
        if not locator_nodes[0].public_dns_name and not opts.private_ips:
            print("Locator has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            print(get_dns_name(locator_nodes[0], opts.private_ips))

    # TODO Get the active lead
    elif action == "get-lead":
        (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(conn, opts, cluster_name)
        if not lead_nodes[0].public_dns_name and not opts.private_ips:
            print("Lead has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            print(get_dns_name(lead_nodes[0], opts.private_ips))

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
            "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
            "All data on spot-instance stores will be lost.\n" +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print("Stopping the snappydata processes...")
            shutdown_snappy_cluster(get_dns_name(locator_nodes[0], opts.private_ips), opts)

            print("Stopping store instance(s)...")
            for inst in server_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()
            print("Stopping lead instance(s)...")
            for inst in lead_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.stop()
            print("Stopping locator instance(s)...")
            for inst in locator_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.stop()
            for inst in zeppelin_nodes:
                print("Stopping Apache Zeppelin instance...")
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.stop()

    elif action == "start":
        (locator_nodes, lead_nodes, server_nodes, zeppelin_nodes) = get_existing_cluster(conn, opts, cluster_name)
        print("Starting locator instance...")
        for inst in locator_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print("Starting lead instance(s)...")
        for inst in lead_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print("Starting store instance(s)...")
        for inst in server_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        all_instances=(locator_nodes + lead_nodes + server_nodes)
        if opts.with_zeppelin is not None:
            all_instances=(all_instances + zeppelin_nodes)
            for inst in zeppelin_nodes:
                print("Starting Apache Zeppelin instance...")
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.start()
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=all_instances,
            cluster_state='ssh-ready'
        )

        # Determine types of running instances
        existing_locator_type = locator_nodes[0].instance_type
        existing_store_type = server_nodes[0].instance_type
        # Setting opts.locator_instance_type to the empty string indicates we
        # have the same instance type for the locator and the stores
        if existing_locator_type == existing_store_type:
            existing_locator_type = ""
        opts.locator_instance_type = existing_locator_type
        opts.instance_type = existing_store_type

        setup_cluster(conn, locator_nodes, lead_nodes, server_nodes, zeppelin_nodes, opts, False)

    else:
        print("Invalid action: %s" % action, file=stderr)
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        print("\n\t\t** PLEASE STOP/TERMINATE THE EC2 INSTANCES OF THIS CLUSTER IF ANY OF THEM ARE LAUNCHED BEFORE THIS ERROR **\n", e, file=stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
