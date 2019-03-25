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
Install GRPC pre-requisites: _sudo apt-get install build-essential autoconf libtool curl cmake git pkg-config_
_git clone -b $(curl -L http://grpc.io/release) https://github.com/grpc/grpc_
cd grpc
git submodule update --init
make
sudo make install
Step out of the GRPC directory. 
If you have any issues installing GRPC, refer to: https://github.com/grpc/grpc/blob/master/INSTALL.md)

(2) Install Protobuf 3.0.0 or higher:



# Issues
If you have any other issues with installation or running benchmarks, please raise an issue in this github repository or email akshitha@umich.edu.

# Maintenance
Frequent code or data pushes to this repository are likely. Please pull from this repository for the latest update.
MicroSuite is developed and maintained by Akshitha Sriraman
