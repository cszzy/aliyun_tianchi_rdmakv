#include <cstddef>
#include "kv_engine.h"

using namespace kv;

int main() {
  RemoteEngine *engine = new RemoteEngine();
  engine->start("", "23627");
  while (engine->alive())
    ;
  engine->stop();
  delete  engine;
  return 0;
}
