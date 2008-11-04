CC = g++
OPTIONS = $(MAKE_OPTIONS)
INCLUDE = -I $(HOME)/deploy/include -I ..
LINKER = -L $(HOME)/deploy/lib -lboost_thread-$(GCCV)-mt -lboost_serialization-$(GCCV)-mt -lboost_system-$(GCCV)-mt -lconfig++
WARN = -Wall -Wextra
OUTPUT = pmanager
RPC = ../rpc
COMMON = ../common

$(OUTPUT): adv_manager.o publisher.o
	$(CC) $(OPTIONS) $(INCLUDE) $(LINKER) -o $(OUTPUT) adv_manager.o publisher.o

adv_manager.o: adv_manager.cpp adv_manager.hpp 
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c adv_manager.cpp

publisher.o: publisher.cpp $(RPC)/rpc_server.hpp
	$(CC) $(WARN) $(INCLUDE) $(OPTIONS) -c publisher.cpp

#doc: doxygen.cfg
#	doxygen doxygen.cfg

clean:
	rm -f *.o $(OUTPUT) *~ 
