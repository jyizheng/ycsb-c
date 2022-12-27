CC=g++
CFLAGS=-std=c++11 -c -g -O3 -Wall -fgnu-tm -D SPLINTERDB_PLATFORM_DIR=platform_linux
INCLUDES=-I../ -I/home/betrfs/tweezer/rocksdb-6.14.5-unmodified/include
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

