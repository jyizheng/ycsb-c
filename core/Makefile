CC=g++
CFLAGS=-std=c++11 -c -g -O3 -Wall
CPPSOURCES=$(wildcard *.cc)
CSOURCES=$(wildcard *.c)
OBJECTS=$(CPPSOURCES:.cc=.o) $(CSOURCES:.c=.o)

all: $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $< -o $@

.c.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
