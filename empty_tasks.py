import ray
import time

ray.init()

# empty task
@ray.remote
def empty_fn():
    return

# empty actor
@ray.remote
class EmptyActor:
    def __init__(self, fns):
        self.returns = [fn.remote() for fn in fns]
    def result(self):
        return returns

# antipattern
# s = time.time()
# returns = []
# for i in range(100):
#     returns.append(ray.get(empty_fn.remote()))
# print("Antipattern: " + str(time.time() - s))

# total number of empty tasks that take a little more than 1s
n = 18000
# n = 2000

# empty tasks
s = time.time()
refs = []
for i in range(n):
    refs.append(empty_fn.remote())
returns = ray.get(refs)
print("Empty tasks (" + str(n) + "): "+ str(time.time() - s))


# empty actors
s = time.time()

fns = [empty_fn for _ in range(n)]
# results = []
BATCH_SIZE = 500
for i in range(0, len(fns), BATCH_SIZE):
    batch = fns[i : i + BATCH_SIZE]
    actor = EmptyActor.remote(batch)
    ray.get(actor.result.remote())

# ray.get(returns)


print("Actors with empty tasks (" + str(n) + "): "+ str(time.time() - s))
