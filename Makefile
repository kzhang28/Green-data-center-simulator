CC=clang++ 
CFLAGS=-O3 -Wall -shared -std=c++17
INC=-I/home/kz181/parasol-rl-sched-simulator/new-2020-simulator/cpp/spdlog/include -I. 

simulator: simulator.cpp pprint.h power_cooling.h common.h dispach_algorithm.h dispach_algorithm.h
	$(CC) $(CFLAGS) $(INC) -fPIC `python3.7 -m pybind11 --includes` simulator.cpp -o simulator`python3.7-config --extension-suffix`
	cp simulator.cpython-37m-x86_64-linux-gnu.so /home/kz181/python37_venv/lib/python3.7/site-packages
	rm simulator.cpython-37m-x86_64-linux-gnu.so
	
clean:
	rm simulator`python3.7-config --extension-suffix`
