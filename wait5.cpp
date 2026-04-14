// Note: Uses an atomic to decrement nprod.
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
using namespace std;

class ProducerConsumer {
    static queue<int> q;
    static condition_variable q_cond;
    static mutex q_sync, print;
    static atomic_size_t nprod;
    static ofstream output;
public:
    static const size_t nprods = 3, ncons = 5;

    static void consume() {
        for (;;) {
            // Get lock for sync mutex
            unique_lock<mutex> qlck(q_sync);

            // Wait for queue to have something to process
            q_cond.wait(qlck,[](){return !q.empty() || !nprod;});
            if (q.empty()) {
                assert(!nprod);
                break;
            }
            // BUG! assert(nprod);
            auto x = q.front();
            q.pop();
            qlck.unlock();

            // Print trace of consumption
            lock_guard<mutex> plck(print);
            output << x << " consumed" << endl;
        }
    }

    static void produce(int i) {
        // Generate 10000 random ints
        srand(time(nullptr)+i*(i+1));
        for (int i = 0; i < 1000; ++i) {
            int n = rand();     // Get random int

            // Get lock for queue; push int
            unique_lock<mutex> qlck(q_sync);
            q.push(n);
            qlck.unlock();
            q_cond.notify_one();

            // Get lock for print mutex
            lock_guard<mutex> plck(print);
            output << n << " produced" << endl;
        }

        // Notify consumers that a producer has shut down
        --nprod;
        q_cond.notify_all();			
    }
};

// Define static class data members
queue<int> ProducerConsumer::q;
condition_variable ProducerConsumer::q_cond;
mutex ProducerConsumer::q_sync, ProducerConsumer::print;
ofstream ProducerConsumer::output("wait5.out");
atomic_size_t ProducerConsumer::nprod(nprods);

int main() {
    vector<thread> prods, cons;
    for (size_t i = 0; i < ProducerConsumer::ncons; ++i)
        cons.push_back(thread(&ProducerConsumer::consume));
    for (size_t i = 0; i < ProducerConsumer::nprods; ++i)
        prods.push_back(thread(&ProducerConsumer::produce,i));

    // Join all threads
    for (auto &p: prods)
        p.join();
    for (auto &c: cons)
        c.join();
}
