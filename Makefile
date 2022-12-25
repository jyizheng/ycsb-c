CC=g++
CFLAGS=-std=c++17 -g -Wall -pthread -I./ -I/mnt/splinterdb/include
LDFLAGS= -lpthread -lsplinterdb -L/mnt/splinterdb/build/lib
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

