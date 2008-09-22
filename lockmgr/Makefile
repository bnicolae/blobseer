CC = g++
OPTIONS = $(MAKE_OPTIONS)
INCLUDE = -I $(HOME)/deploy/include -I ..
LINKER = -L $(HOME)/deploy/lib -lboost_thread-gcc41-mt -lboost_serialization-gcc41-mt -lboost_system-gcc41-mt -lconfig++
WARN = -Wall -Wextra
OUTPUT = lock_mgr
RPC = ../rpc
COMMON = ../common

$(OUTPUT): lock_management.o lockmgr.o
	$(CC) $(OPTIONS) $(INCLUDE) $(LINKER) -o $(OUTPUT) lock_management.o lockmgr.o

lock_management.o: lock_management.cpp lock_management.hpp 
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c lock_management.cpp

lockmgr.o: lockmgr.cpp $(RPC)/rpc_server.hpp
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c lockmgr.cpp

#doc: doxygen.cfg
#	doxygen doxygen.cfg

clean:
	rm -f *.o $(OUTPUT) *~ 
