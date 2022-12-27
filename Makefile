CC=g++
CFLAGS=-std=c++17 -g -Wall -pthread -I./ -I/home/betrfs/tweezer/rocksdb-6.14.5-unmodified/include
LDFLAGS= -lpthread -L/home/betrfs/tweezer/rocksdb-6.14.5-unmodified -L../libs  -lrocksdb -lz -lsnappy -ldl
SUBDIRS=core db
SUBCPPSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
SUBCSRCS=$(wildcard core/*.c) $(wildcard db/*.c)
OBJECTS=$(SUBCPPSRCS:.cc=.o) $(SUBCSRCS:.c=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

