C = gcc
CXXFLAGS = -pthread -march=native
CXX = g++
CXXFLAGS = -pthread -std=c++11 -march=native
#OBJS = build/sqlite.o
INCLUDES = -I./src/sqib

opt: CXXFLAGS += -O3 -funroll-loops -DNDEBUG
opt: test_sqlite_blaster

debug: CXXFLAGS += -g -O0 -fno-inline
# -fsanitize=address -static-libsan
debug: test_sqlite_blaster

test_sqlite_blaster: test_sqlite_blaster.cpp src/sqib/*.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o test_sqlite_blaster test_sqlite_blaster.cpp

clean:
	rm test_sqlite_blaster
	rm -f tests_out/*.db
	rm -f tests_out/*.txt

#build/.o: src/imain.cpp src/*.h
#   $(CXX) $(CXXFLAGS) $(INCLUDES) -c src/imain.cpp -o build/imain.o
