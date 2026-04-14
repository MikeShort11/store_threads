// Michael Short - Pipelined eCommerce Order Processing System
#include <iostream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <stop_token>
#include <fstream>

struct Order
{
  int orderId;
  double orderAmount;
  int regionCode;
};

void ProducerThread(std::stop_token stopToken, std::vector<Order> &orderQueue, std::mutex &queueMutex, std::condition_variable &queueCV)
{
  while (!stopToken.stop_requested())
  {
    Order newOrder{rand() % 10000, 10.0 + static_cast<double>(rand() % 491), rand() % 5};
    {
      std::lock_guard<std::mutex> lock(queueMutex);
      orderQueue.push_back(newOrder);
    }
    queueCV.notify_one();
  }
}

void filterThread(std::stop_token stopToken, std::vector<Order> &orderQueue, std::vector<Order> &validatedQueue, std::mutex &queueMutex, std::condition_variable &queueCV)
{
  while (!stopToken.stop_requested())
  {
    std::unique_lock<std::mutex> lock(queueMutex);
    queueCV.wait(lock, [&orderQueue]
                 { return !orderQueue.empty(); });

    if (!orderQueue.empty())
    {
      Order order = orderQueue.back();
      orderQueue.pop_back();
      if (order.orderAmount > 50.0 && order.orderId >= 0) // Example filter condition
      {
        validatedQueue.push_back(order);
      }

      lock.unlock();
      queueCV.notify_one();
    }
  }
}

void routerThread(std::stop_token stopToken, std::vector<Order> &validatedQueue, int assignedRegion, std::string filePath, std::mutex &queueMutex, std::condition_variable &queueCV)
{
  while (!stopToken.stop_requested())
  {
    std::unique_lock<std::mutex> lock(queueMutex);
    queueCV.wait(lock, [&validatedQueue]
                 { return !validatedQueue.empty(); });

    if (!validatedQueue.empty())
    {
      Order order = validatedQueue.back();
      if (order.regionCode == assignedRegion)
      {
        std::ofstream outFile(filePath, std::ios::app);
        if (outFile.is_open())
        {
          outFile << "Order ID: " << order.orderId << ", Amount: " << order.orderAmount << ", Region: " << order.regionCode << std::endl;
          outFile.close();
        }
      }
      lock.unlock();
      queueCV.notify_one();
    }
  }
}

int main()
{
  std::vector<Order> orderQueue;
  std::mutex queueMutex;
  std::condition_variable queueCV;

  std::jthread producer(ProducerThread, std::ref(orderQueue), std::ref(queueMutex), std::ref(queueCV));

  // Let the producer run for a bit

  // Stop the producer
  producer.request_stop();
  printf(orderQueue.size() > 0 ? "Produced %zu orders.\n" : "No orders produced.\n", orderQueue.size());

  return 0;
}
