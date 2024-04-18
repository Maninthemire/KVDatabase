CXX = g++
CFLAGS = -std=c++14 -O2 -Wall -g 

TARGET = main
OBJS = main.cpp

all: $(OBJS)
	$(CXX) $(CFLAGS) $(OBJS) -o bin/$(TARGET) 

clean:
	rm -rf bin/$(OBJS) $(TARGET)
