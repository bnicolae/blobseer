CC = g++
OPTIONS = $(MAKE_OPTIONS)
INCLUDE = -I $(HOME)/deploy/include -I ..
LINKER = -L $(HOME)/deploy/lib -lboost_thread-$(GCCV)-mt -lboost_serialization-$(GCCV)-mt -lboost_system-$(GCCV)-mt -lconfig++
WARN = -Wall -Wextra
OUTPUT = vmanager
RPC = ../rpc
COMMON = ../common

$(OUTPUT): vmanagement.o main.o
	$(CC) $(OPTIONS) $(INCLUDE) $(LINKER) -o $(OUTPUT) vmanagement.o main.o

vmanagement.o: vmanagement.cpp vmanagement.hpp 
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c vmanagement.cpp

main.o: main.cpp $(RPC)/rpc_server.hpp
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c main.cpp

#doc: doxygen.cfg
#	doxygen doxygen.cfg

clean:
	rm -f *.o $(OUTPUT) *~ 
