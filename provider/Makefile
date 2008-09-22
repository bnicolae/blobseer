CC = g++
OPTIONS = $(MAKE_OPTIONS)
INCLUDE = -I $(HOME)/deploy/include -I ..
LINKER = -L $(HOME)/deploy/lib -lboost_thread-gcc41-mt -lboost_serialization-gcc41-mt -lboost_system-gcc41-mt -lconfig++
WARN = -Wall -Wextra
OUTPUT = provider
RPC = ../rpc
COMMON = ../common

$(OUTPUT): page_manager.o provider.o
	$(CC) $(OPTIONS) $(INCLUDE) $(LINKER) -o $(OUTPUT) page_manager.o provider.o

page_manager.o: page_manager.cpp page_manager.hpp 
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c page_manager.cpp

provider.o: provider.cpp $(RPC)/rpc_server.hpp $(RPC)/rpc_client.hpp
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c provider.cpp

#doc: doxygen.cfg
#	doxygen doxygen.cfg

clean:
	rm -f *.o $(OUTPUT) *~ 
