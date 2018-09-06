Dataset directory:
--Contains dataset and query files

Service directory:
--Install grpc and protoc 
--Paths to "grpc/libs/opt/pkgconfig" and "grpc/libs/opt" must be exported as environment variables: PKG_CONFIG_PATH and LD_LIBRARY_PATH in the ~/.bashrc file
--Makefile compiles bucket.proto to create ".grpc.pb.cc" and ".pb.cc" files
--bucket_server and bucket_client use classes defined in src/ for distance calculations

Src directory:
--Contains all the necessary files to perform distance calculations between dataset and query points

Test directory:
--Unit tests for distance calculations

