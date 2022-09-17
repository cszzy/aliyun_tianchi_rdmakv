#include "bitmap.h"
#include <assert.h>
#include <iostream>

using namespace std;

int main() {
    kv::bitmap *b = kv::create_bitmap(192000000ul);
    for (int i = 0; i < 10000000; i++) {
        int slot = kv::get_free(b);
        assert(i == slot);
        if (i % 100000 == 0)
            std::cout << i << std::endl;
    }
    return 0;
}