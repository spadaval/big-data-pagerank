#!/bin/sh
#preliminary tasks
cd edgecount
./build.sh
cd ..

cd vertexcount
./build.sh
cd ..

#create the matrix A
cd matrixbuilder
./build.sh
cd ..
#multiply the matrix A and vector V
cd matrixmultiplier
./build.sh
cd ..

cd pr
./build.sh
cd ..
