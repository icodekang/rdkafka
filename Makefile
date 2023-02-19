ROOT = $(abspath .)
SRC_PATH := ${ROOT}/src
INC_PATH := ${ROOT}/include
LIB_PATH := ${ROOT}/lib
LIB_NAME := rdkafka

TAR := rdkafka
OBJ := $(patsubst ${ROOT}/%.cc, ${ROOT}/%.o, ${SRC_PATH}/*.cc) 
SRC := $(wildcard ${SRC_PATH}/*.cc)
INC := ${foreach path, ${INC_PATH}, -I${path}}
LIB := ${foreach lib, ${LIB_PATH}, -L${lib}}
LIB += ${foreach lib, ${LIB_NAME}, -l${lib}}
CC  := g++

$(TAR):$(OBJ)
	$(CC) $(OBJ) -o $(TAR) $(LIB)

%.o:%.cc
	$(CC) -c $(SRC) -o $(OBJ) $(INC)

.PHONY:
clean:
	rm $(OBJ) $(TAR)