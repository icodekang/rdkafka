TAR = rdkafka
OBJ = ./src/rdkafka.o
SRC = ./src/rdkafka.cc
CC := g++

$(TAR):$(OBJ)
	$(CC) $(OBJ) -o $(TAR) -L./lib -lrdkafka

%.o:%.cc
	$(CC) -c $(SRC) -o $(OBJ) -I./include

.PHONY:
clean_all:
	rm $(OBJ)