CC = g++
OPTIONS = $(MAKE_OPTIONS)
INCLUDE = -I $(HOME)/deploy/include -I ..
LINKER = -L $(HOME)/deploy/lib -lboost_thread-gcc41-mt -lboost_serialization-gcc41-mt -lboost_system-gcc41-mt -lconfig++
WARN = -Wall -Wextra
OUTPUT = sdht
RPC = ../rpc
COMMON = ../common

$(OUTPUT): page_manager.o sdht.o
	$(CC) $(OPTIONS) $(INCLUDE) $(LINKER) -o $(OUTPUT) page_manager.o sdht.o

page_manager.o: page_manager.cpp page_manager.hpp 
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c page_manager.cpp

sdht.o: sdht.cpp $(RPC)/rpc_server.hpp
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c sdht.cpp

#doc: doxygen.cfg
#	doxygen doxygen.cfg

clean:
	rm -f *.o $(OUTPUT) *~ 
