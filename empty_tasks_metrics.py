import ray
import time
import logging
from statistics import mean
from statistics import stdev

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

def empty_task():
    start = time.time()
    ray.get([empty_fn.remote() for _ in range(EMPTY_TASKS)])
    end = time.time()
    return end - start

TOTAL_EXPERIMENTS = 128
EMPTY_TASKS = 2**14
number_of_actors = [1, 2, 4, 8, 16]

# empty tasks
empty_tasks_times = [0] * TOTAL_EXPERIMENTS
for i in range(TOTAL_EXPERIMENTS):
    start = time.time()
    ray.get([empty_fn.remote() for _ in range(EMPTY_TASKS)])
    end = time.time()
    empty_tasks_times[i] = end - start

# print("Empty tasks (# tasks: " + str(EMPTY_TASKS) + "): "+ str(end - start))


# empty actors
empty_actors_times = [[0] * TOTAL_EXPERIMENTS] * len(number_of_actors)
for i, actors in enumerate(number_of_actors):
    for j in range(TOTAL_EXPERIMENTS):


        start = time.time()
        BATCH_SIZE = EMPTY_TASKS / actors
        fns = [empty_fn for _ in range(EMPTY_TASKS)]
        refs = []

        for i in range(0, len(fns), BATCH_SIZE):
            batch = fns[i : i + BATCH_SIZE]
            actor = EmptyActor.remote()
            refs.append(actor.work.remote(batch))

        unfinished = refs
        while unfinished:
            finished, unfinished = ray.wait(unfinished, num_returns=1)
            ray.get(finished)

        end = time.time()

        empty_actors_times[i][j] = end - start



for b in range(4000, 8000, 100):

    start = time.time()
    BATCH_SIZE = b
    fns = [empty_fn for _ in range(n)]
    refs = []

    for i in range(0, len(fns), BATCH_SIZE):
        batch = fns[i : i + BATCH_SIZE]
        actor = EmptyActor.remote()
        refs.append(actor.work.remote(batch))

    unfinished = refs
    while unfinished:
        finished, unfinished = ray.wait(unfinished, num_returns=1)
        ray.get(finished)

    end = time.time()
    print("Actors with empty tasks (# tasks: " + str(n) + ", batch size: " + str(b) + "): " + str(end - start))
