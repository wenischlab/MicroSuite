#!/usr/bin/python
import os
import subprocess
import sys

def InstallGrpc():
    subprocess.call(["sudo", "apt-get", "install", "build-essential", "autoconf", "libtool", "curl", "cmake", "git", "pkg-config"])
    os.system("git clone -b $(curl -L http://grpc.io/release) https://github.com/grpc/grpc")
    subprocess.Popen(["git", "submodule", "update", "--init"], cwd="grpc/")
    subprocess.Popen(["make"], cwd="grpc/")
    subprocess.Popen(["sudo", "make", "install"], cwd="grpc/")

def InstallProtobuf():
    subprocess.call(["wget", "https://github.com/google/protobuf/releases/download/v3.2.0/protobuf-cpp-3.2.0.tar.gz"])
    subprocess.call(["tar", "-xzvf", "protobuf-cpp-3.2.0.tar.gz"]) 
    subprocess.Popen(["./configure"], cwd="protobuf-3.2.0")
    subprocess.Popen(["make"], cwd="protobuf-3.2.0")
    subprocess.Popen(["make", "check"], cwd="protobuf-3.2.0")
    subprocess.Popen(["sudo", "make", "install"], cwd="protobuf-3.2.0")
    subprocess.Popen(["sudo", "ldconfig"], cwd="protobuf-3.2.0")

def InstallHDS():
    subprocess.Popen(["cd", "HDSearch"], cwd=".")
    subprocess.Popen(["mkdir", "build"], cwd="HDSearch/mid_tier_service")
    subprocess.Popen(["cmake", ".."], cwd="HDSearch/mid_tier_service/build")
    subprocess.Popen(["sudo", "make", "install"], cwd="HDSearch/mid_tier_service/build")
    subprocess.Popen(["make"], cwd="HDSearch/mid_tier_service/build")

    subprocess.Popen(["cd", "protoc_files"], cwd="HDSearch/protoc_files")
    subprocess.Popen(["make"], cwd="HDSearch/protoc_files")

    subprocess.Popen(["cd", "bucket_service/service"], cwd="HDSearch/bucket_service/service")
    subprocess.Popen(["make"], cwd="HDSearch/bucket_service/service")



def InstallOpenSSL():
    subprocess.call(["sudo", "apt-get", "install", "openssl"])
    subprocess.call(["sudo", "apt-get", "install", "libssl-dev"])

def InstallMKL():
    subprocess.call(["wget", "http://registrationcenter-download.intel.com/akdlm/irc_nas/tec/11306/l_mkl_2017.2.174.tgz"])
    subprocess.call(["tar", "xzvf", "l_mkl_2017.2.174.tgz"])
    subprocess.call(["./install.sh"], cwd="l_mkl_2017.2.174")

def main():
    print "Installing gRPC....\n"
    InstallGrpc()
    print "Installed gRPC....\n"
    print "Installing Protobuf....\n"
    InstallProtobuf()
    print "Installed Protobuf....\n"
    InstallOpenSSL()
    InstallMKL()
    print "Installing HDSearch....\n"
    InstallHDS()
    print "Installed HDSearch....\n"
    InstallOpenSSL()
    InstallMKL()

main()
