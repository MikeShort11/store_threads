#include <iostream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <stop_token>
#include <fstream>
#include <random>
#include <atomic>
#include <iomanip>

struct Order
{
  int orderId;
  double orderAmount;
  int regionCode;
};

class OrderPipeline
{
private:
  static std::queue<Order> rawQueue;
  static std::queue<Order> validatedQueue;
  static std::mutex rawMtx;
  static std::mutex validMtx;
  static std::condition_variable rawCV;
  static std::condition_variable validCV;

  static std::atomic<int> totalOrders;
  static std::atomic<int> validCount;
  static std::atomic<int> invalidCount;
  static std::atomic<double> regionalRevenue[5];
  static std::atomic<int> regionalOrderCount[5];

public:
  static void producerWork(std::stop_token st)
  {
    while (!st.stop_requested())
    {
      if (totalOrders.fetch_add(1) >= 1000)
        break;
      Order newOrder{rand() % 10000, 10.0 + static_cast<double>(rand() % 491), rand() % 5};
      {
        std::lock_guard<std::mutex> lock(rawMtx);
        rawQueue.push(newOrder);
      }
    }
  }

  static void filterWork(std::stop_token st)
  {
    while (!st.stop_requested())
    {
      std::unique_lock<std::mutex> lock(rawMtx);
      rawCV.wait(lock, [st]
                { return st.stop_requested() || !rawQueue.empty(); });

      if (!rawQueue.empty())
      {
        Order order = rawQueue.front();
        rawQueue.pop();

        lock.unlock(); // Unlock early for better concurrency [cite: 42]

        if (order.orderAmount >= 50.0 && order.orderId >= 0)
        {
          {
            std::lock_guard<std::mutex> validLock(validMtx);
            validatedQueue.push(order);
            validCount++;
            regionalRevenue[order.regionCode] += order.orderAmount;
            regionalOrderCount[order.regionCode]++;
          }
          validCV.notify_one();
        }
        else
        {
          invalidCount++;
        }
      }
    }
  }

  static void routerWork(std::stop_token st, int regionId)
  {
    while (!st.stop_requested())
    {
      std::unique_lock<std::mutex> lock(validMtx);
      validCV.wait(lock, [st, regionId]
                  { return st.stop_requested() || (!validatedQueue.empty() && validatedQueue.front().regionCode == regionId); });

      if (!validatedQueue.empty() && validatedQueue.front().regionCode == regionId)
      {
        Order order = validatedQueue.front();
        validatedQueue.pop();

        lock.unlock(); // Unlock early for better concurrency [cite: 42]

        try
        {
          std::ofstream outFile("region_" + std::to_string(regionId) + "_orders.txt", std::ios::app);
          if (outFile.is_open())
          {
            outFile << "Order ID: " << order.orderId << ", Amount: " << order.orderAmount << ", Region: " << order.regionCode << std::endl;
          }
          else
          {
            std::cerr << "Failed to open file for region " << regionId << std::endl;
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << "Error writing to file for region " << regionId << ": " << e.what() << std::endl;
        }
      }
    }
  }

  static void run()
  {
    std::vector<std::jthread> producers;
    for (int i = 0; i < 5; ++i)
      producers.emplace_back(producerWork);

    std::vector<std::jthread> filters;
    for (int i = 0; i < 4; ++i)
      filters.emplace_back(filterWork);

    std::vector<std::jthread> routers;
    for (int i = 0; i < 5; ++i)
      routers.emplace_back(routerWork, i);

    producers.clear(); // jthreads join automatically here [cite: 53]

    for (auto &f : filters)
      f.request_stop();
    rawCV.notify_all();
    filters.clear();

    for (auto &r : routers)
      r.request_stop();
    validCV.notify_all();
    routers.clear();

    static const char *regionNames[5] = {"West", "Central", "East", "North", "South"};
    double totalRevenue = 0;
    int totalValid = 0;
    int totalInvalid = invalidCount;
    std::cout << "Order Processing Pipeline Results\n";
    std::cout << "==================================\n";
    for (int i = 0; i < 5; ++i)
    {
      std::cout << "Region " << i << " (" << regionNames[i] << "): "
                << regionalOrderCount[i] << " orders, Total Revenue: $"
                << std::fixed << std::setprecision(2) << regionalRevenue[i] << std::endl;
      totalRevenue += regionalRevenue[i];
      totalValid += regionalOrderCount[i];
    }
    std::cout << "Total Valid Orders: " << totalValid << std::endl;
    std::cout << "Total Invalid Orders: " << totalInvalid << std::endl;
    std::cout << "Pipeline processing completed successfully.\n";
  }
};

std::queue<Order> OrderPipeline::rawQueue;
std::queue<Order> OrderPipeline::validatedQueue;
std::mutex OrderPipeline::rawMtx;
std::mutex OrderPipeline::validMtx;
std::condition_variable OrderPipeline::rawCV;
std::condition_variable OrderPipeline::validCV;
std::atomic<int> OrderPipeline::totalOrders{0};
std::atomic<int> OrderPipeline::validCount{0};
std::atomic<int> OrderPipeline::invalidCount{0};
std::atomic<double> OrderPipeline::regionalRevenue[5] = {0, 0, 0, 0, 0};
std::atomic<int> OrderPipeline::regionalOrderCount[5] = {0, 0, 0, 0, 0};

int main()
{
  OrderPipeline::run();
  return 0;
}
