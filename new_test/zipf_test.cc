#include "zipf.h"
#include <cstddef>
#include <vector>

int main() {
  std::vector<int> counter(101, 0);
  Zipf zipf(100, 0x1231, 2);
  for (int i = 0; i < 1000; i++) {
    int num = zipf.Next();
    printf("Zipf %d\n", num);
    counter[num]++;
  }
  for (size_t i = 1; i < counter.size(); i++) {
    printf("num %zu, frequency %d\n", i, counter[i]);
  }
}