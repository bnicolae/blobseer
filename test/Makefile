CC = g++
OPTIONS = $(MAKE_OPTIONS)
LINKER = -L $(HOME)/deploy/lib -lcrypto -lboost_thread-$(GCCV)-mt -lboost_serialization-$(GCCV)-mt -lboost_system-$(GCCV)-mt -lconfig++
COMMON = ../common
RPC = ../rpc
CLIENT = ../client
INCLUDE = -I $(HOME)/deploy/include -I ..
WARN = -Wall -Wextra
#OUTPUT = create_blob
OUTPUT = multiple_writers
GEN_SOURCE = gateway_prot.h gateway_prot_clnt.c gateway_prot_xdr.c


$(OUTPUT): $(OUTPUT).o gateway_prot_clnt.o gateway_prot_xdr.o range_query.o object_handler.o
	$(CC) $(OPTIONS) $(LINKER) -o $(OUTPUT) $(OUTPUT).o gateway_prot_clnt.o gateway_prot_xdr.o range_query.o object_handler.o

object_handler.o: $(CLIENT)/object_handler.cpp $(CLIENT)/object_handler.hpp $(RPC)/rpc_client.hpp
	$(CC) $(INCLUDE) $(WARN) $(OPTIONS) -c $(CLIENT)/object_handler.cpp

range_query.o: $(CLIENT)/range_query.cpp $(CLIENT)/range_query.hpp $(RPC)/rpc_client.hpp
	$(CC) $(INCLUDE) $(WARN) $(OPTIONS) -c $(CLIENT)/range_query.cpp

gateway_prot_clnt.o: gateway_prot_clnt.c gateway_prot.h
	$(CC) $(OPTIONS) -c gateway_prot_clnt.c

gateway_prot_xdr.o: gateway_prot_xdr.c gateway_prot.h
	$(CC) $(OPTIONS) -c gateway_prot_xdr.c

$(OUTPUT).o: $(OUTPUT).cpp gateway_prot.h
	$(CC) $(INCLUDE) $(WARN) $(OPTIONS) -c $(OUTPUT).cpp

gateway_prot.h: $(COMMON)/gateway_prot.x
	rpcgen -M -h $(COMMON)/gateway_prot.x > gateway_prot.h

gateway_prot_clnt.c: $(COMMON)/gateway_prot.x
	rpcgen -M -l $(COMMON)/gateway_prot.x > gateway_prot_clnt.c

gateway_prot_xdr.c: $(COMMON)/gateway_prot.x
	rpcgen -M -c $(COMMON)/gateway_prot.x > gateway_prot_xdr.c

clean:
	rm -f *.o $(OUTPUT) $(GEN_SOURCE) *~ 
