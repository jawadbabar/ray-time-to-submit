import ray
import time
import logging

ray.init(num_cpus=64, log_to_driver=False, logging_level=logging.FATAL) # removing warnings

# empty task
@ray.remote
def empty_fn():
    return

# empty actor
@ray.remote
class EmptyActor:
    def work(self, fns):
        return [fn.remote() for fn in fns]

# total number of empty tasks that take a little more than 1s
n = 2**14

# empty tasks
start = time.time()
[empty_fn.remote() for _ in range(n)]
end = time.time()
print("Empty tasks (# tasks: " + str(n) + "): "+ str(end - start))


# empty actors
fns = [empty_fn for _ in range(n)]
for a in [1, 2, 4, 8, 16, 32, 64]:

    batch_size = n // a

    actors = []
    for i in range(0, len(fns), batch_size):
        batch = fns[i : i + batch_size]
        actor = EmptyActor.remote()
        actors.append(actor)

    start = time.time()
    refs = []

    for actor in actors:
        refs.append(actor.work.remote(batch))

    ray.get(refs)

    end = time.time()
    print("Actors with empty tasks (# tasks: " + str(n) + ", batch size: " + str(batch_size) + "): " + str(end - start))
