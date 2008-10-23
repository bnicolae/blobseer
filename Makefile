MAKE_DEFAULT = -g -D __INFO -D SOCK_TYPE=tcp
MAKE_PROVIDER=$(MAKE_DEFAULT)
MAKE_SDHT=$(MAKE_DEFAULT)
MAKE_VMGR=$(MAKE_DEFAULT)
MAKE_PMGR=$(MAKE_DEFAULT)
#MAKE_TEST=$(MAKE_DEFAULT)
MAKE_LIB=$(MAKE_DEFAULT)

all:
	export MAKE_OPTIONS="$(MAKE_PMGR)"; cd pmanager && make
	export MAKE_OPTIONS="$(MAKE_PROVIDER)"; cd provider && make
	export MAKE_OPTIONS="$(MAKE_VMGR)"; cd vmanager && make
#	export MAKE_OPTIONS="$(MAKE_TEST)"; cd test && make
	export MAKE_OPTIONS="$(MAKE_SDHT)"; cd sdht && make
	export MAKE_OPTIONS="$(MAKE_LIB)"; cd lib && make

doc: doxygen.cfg
	doxygen doxygen.cfg

clean:
	rm -r -f ./doc
	cd pmanager && make clean
	cd provider && make clean
	cd vmanager && make clean
#	cd test && make clean
	cd sdht && make clean
	cd lib && make clean

