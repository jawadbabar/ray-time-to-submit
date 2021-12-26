import ray
import time
import logging

ray.init(log_to_driver=False, logging_level=logging.FATAL) # removing warnings

# empty task
@ray.remote
def empty_fn():
    return

# empty actor
@ray.remote
class EmptyActor:
    def work(self, fns):
        self.refs = [fn.remote() for fn in fns]
        return

# total number of empty tasks that take a little more than 1s
n = 18000
# n = 2000

# empty tasks
start = time.time()
ray.get([empty_fn.remote() for _ in range(n)])
end = time.time()
print("Empty tasks (# tasks: " + str(n) + "): "+ str(end - start))


# empty actors
for b in range(4000, 8000, 100):

    start = time.time()

    BATCH_SIZE = b
    fns = [empty_fn for _ in range(n)]
    refs = []

    for i in range(0, len(fns), BATCH_SIZE):
        batch = fns[i : i + BATCH_SIZE]
        actor = EmptyActor.remote()
        refs.append(actor.work.remote(batch))

    # ray.get(refs)

    unfinished = refs
    while unfinished:
        finished, unfinished = ray.wait(unfinished, num_returns=1)
        ray.get(finished)

    end = time.time()
    print("Actors with empty tasks (# tasks: " + str(n) + ", batch size: " + str(b) + "): " + str(end - start))
