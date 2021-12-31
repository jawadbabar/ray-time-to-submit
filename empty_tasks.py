import ray
import logging

from time import time
from statistics import mean
from statistics import stdev
from math import log

TOTAL_EXPERIMENTS = 100
TOTAL_EMPTY_TASKS = 2**14
NUMBER_OF_ACTORS = [1, 2, 4, 8, 16]

# empty task
@ray.remote
def empty_fn():
    return

# empty actor
@ray.remote
class EmptyActor:
    def work(self, fns):
        return [fn.remote() for fn in fns]

# helper function to run an empty task experiment
def empty_task_experiment():
    start = time()
    [empty_fn.remote() for _ in range(TOTAL_EMPTY_TASKS)]
    end = time()
    return end - start

# helper function to run an empty actor experiment
def empty_actor_experiment(fns, total_actors, batch_size):
    tasks_per_actor = TOTAL_EMPTY_TASKS // total_actors
    batches_per_actor = tasks_per_actor // batch_size
    actors = [EmptyActor.remote() for _ in range(total_actors)]

    start = time()
    refs = []
    for i, actor in enumerate(actors):
        batch_per_actor = fns[i * tasks_per_actor : (i+1) * tasks_per_actor]
        for j in range(batches_per_actor):
            batch = batch_per_actor[j * batch_size : (j+1) * batch_size]
            refs.append(actor.work.remote(batch))
    ray.get(refs)
    end = time()

    return end - start


if __name__ == "__main__":

    ray.init(num_cpus=64) # removing warnings

    print("Total number of empty tasks: " + str(TOTAL_EMPTY_TASKS))
    print("Remote function test:")

    # empty tasks
    empty_tasks_times = [empty_task_experiment() for _ in range(TOTAL_EXPERIMENTS)]
    print("mean: " + str(mean(empty_tasks_times)) + "; std: " + str(stdev(empty_tasks_times)))
    print("Remote actor test:")

    # empty actors
    fns = [empty_fn for _ in range(TOTAL_EMPTY_TASKS)]
    for i, actors in enumerate(NUMBER_OF_ACTORS):
        for j in range(7, int(log(TOTAL_EMPTY_TASKS // actors, 2)) + 1):
            batch = 2 ** j
            empty_actors_times = [empty_actor_experiment(fns, actors, batch) for _ in range(TOTAL_EXPERIMENTS)]
            print("mean: " + str(mean(empty_actors_times)) + "; std: " + str(stdev(empty_actors_times))
            + " (actors: " + str(actors) + "; batch size: " + str(batch) + ")")

    print("Total number of experiments conducted for calculating mean/std: " + str(TOTAL_EXPERIMENTS))
