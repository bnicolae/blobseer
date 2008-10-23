CC = g++
OPTIONS = $(MAKE_OPTIONS) -fPIC
LINKER = -L $(HOME)/deploy/lib -lcrypto -lboost_thread-gcc41-mt -lboost_serialization-gcc41-mt -lboost_system-gcc41-mt -lconfig++ -ldl -shared
COMMON = ../common
RPC = ../rpc
CLIENT = ../client
INCLUDE = -I $(HOME)/deploy/include -I ..
WARN = -Wall -Wextra
OUTPUT = blobseer

$(OUTPUT).so: $(OUTPUT).o range_query.o object_handler.o
	$(CC) $(OPTIONS) $(LINKER) -o $(OUTPUT).so $(OUTPUT).o range_query.o object_handler.o

object_handler.o: $(CLIENT)/object_handler.cpp $(CLIENT)/object_handler.hpp $(RPC)/rpc_client.hpp
	$(CC) $(INCLUDE) $(WARN) $(OPTIONS) -c $(CLIENT)/object_handler.cpp

range_query.o: $(CLIENT)/range_query.cpp $(CLIENT)/range_query.hpp $(RPC)/rpc_client.hpp
	$(CC) $(INCLUDE) $(WARN) $(OPTIONS) -c $(CLIENT)/range_query.cpp

$(OUTPUT).o: $(OUTPUT).cpp $(OUTPUT).h $(RPC)/rpc_client.hpp
	$(CC) $(INCLUDE) $(WARN) $(OPTIONS) -c $(OUTPUT).cpp

install: $(OUTPUT).so
	mv $(OUTPUT).so $(HOME)/deploy/lib/lib$(OUTPUT).so

clean:
	rm -f *.o $(OUTPUT).so *~
