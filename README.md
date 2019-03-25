# MicroSuite
µSuite: A Benchmark Suite for Microservices

µSuite is a suite of OLDI services that are each composed of front-end, mid-tier, and leaf microservice tiers. μSuite includes four OLDI services that incorporate open-source software: a content-based high dimensional search for image similarity — HDSearch, a replication-based protocol router for scaling fault-tolerant key-value stores — Router, a service for performing set algebra on posting lists for document retrieval — Set Algebra, and a user-based item recommender system for predicting user ratings — Recommend.
µSuite was originally written to evaluate OS and network overheads faced by microservices. You can find more details about µSuite in our IISWC paper (http://akshithasriraman.eecs.umich.edu/pubs/IISWC2018-%CE%BCSuite-preprint.pdf).

# License & Copyright
µSuite is free software; you can redistribute it and/or modify it under the terms of the BSD License as published by the Open Source Initiative, revised version.

µSuite was originally written by Akshitha Sriraman at the University of Michigan, and per the the University of Michigan policy, the copyright of this original code remains with the Trustees of the University of Michigan.

If you use this software in your work, we request that you cite the µSuite paper ("μSuite: A Benchmark Suite for Microservices", Akshitha Sriraman and Thomas F. Wenisch, IEEE International Symposium on Workload Characterization, September 2018), and that you send us a citation of your work.

# Installation
To install µSuite, please follow these steps (works on Debian):

(1) Install GRPC:

sudo apt-get install build-essential autoconf libtool curl cmake git pkg-config

git clone -b $(curl -L http://grpc.io/release) https://github.com/grpc/grpc

cd grpc

git submodule update --init

make

sudo make install

Step out of the GRPC directory.

If you have any issues installing GRPC, refer to: https://github.com/grpc/grpc/blob/master/INSTALL.md)

(2) Install Protobuf 3.0.0 or higher:

wget https://github.com/google/protobuf/releases/download/v3.2.0/protobuf-cpp-3.2.0.tar.gz

tar -xzvf protobuf-cpp-3.2.0.tar.gz

cd protobuf-3.2.0

./configure

make

make check

sudo make install

sudo ldconfig

Step out of the protobuf directiry.

If you have any issues installing Protobuf, refer to: https://github.com/protocolbuffers/protobuf/releases


(3) Install OpenSSL and Intel's MKL:

sudo apt-get install openssl

sudo apt-get install libssl-dev

wget http://registrationcenter-download.intel.com/akdlm/irc_nas/tec/11306/l_mkl_2017.2.174.tgz

tar xzvf l_mkl_2017.2.174.tgz

cd l_mkl_*

./install.sh   -> Follow the prompts that appear to install MKL.

Step back into the MicroSuite directory.


(4) Install FLANN:

cd src/HDSearch/mid_tier_service

mkdir build

cd build

cmake ..

sudo make install

make

If you have any issues installing FLANN, please refer to: http://www.cs.ubc.ca/research/flann/uploads/FLANN/flann_manual-1.8.4.pdf and https://github.com/mariusmuja/flann/issues

(5) Install MLPACK (MicroSuite uses MLPACK version 2.2.5):

sudo apt-get install libmlpack-dev

If you have any issues installing or running MLPACK, please refer to: https://github.com/mlpack/mlpack

(6) Build HDSearch:

cd src/HDSearch/protoc_files

make  ---> It's fine if you have errors, just make sure that the "*.grpc.*" and "*pb.*" files get created.

cd ../bucket_service/service

make

cd ../../mid_tier_service/service/

make

cd ../../load_generator/

make  --> Open loop load generators are used for measuring latency and closed-loop load generators help measure throughput.

Step back to the MicroSuite parent directory.

(7) Build Router:

cd src/Router/protoc_files

make  ---> It's fine if you have errors, just make sure that the "*.grpc.*" and "*pb.*" files get created.

cd ../lookup_service/service

make

cd ../../mid_tier_service/service/

make

cd ../../load_generator/

make

(8) Build Set Algebra:

















# Issues
If you have any other issues with installation or running benchmarks, please raise an issue in this github repository or email akshitha@umich.edu.

If you have issues with any of the third party software that MicroSuite uses, you will have to look up issues pertaining to that software; I may not be fully qualified to answer those questions.

# Maintenance
Frequent code or data pushes to this repository are likely. Please pull from this repository for the latest update.
MicroSuite is developed and maintained by Akshitha Sriraman
