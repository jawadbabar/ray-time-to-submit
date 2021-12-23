import ray
import time

ray.init()

@ray.remote
def empty_fn():
    return

# antipattern
# s = time.time()
# returns = []
# for i in range(100):
#     returns.append(ray.get(empty_fn.remote()))
# print("Antipattern: " + str(time.time() - s))

# better approach
ns = [10000, 12000, 14000, 16000, 18000, 20000]
for n in ns:
    s = time.time()
    refs = []
    for i in range(n):
        refs.append(empty_fn.remote())

    returns = ray.get(refs)
    print("Empty tasks (" + str(n) + "): "+ str(time.time() - s))
