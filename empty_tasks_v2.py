import ray
import logging

from time import time
from statistics import mean
from statistics import stdev


TOTAL_EXPERIMENTS = 128
TOTAL_EMPTY_TASKS = 2**14
NUMBER_OF_ACTORS = [1, 2, 4, 8, 16, 32]

ray.init(num_cpus=32, log_to_driver=False, logging_level=logging.FATAL) # removing warnings

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
def empty_actor_experiment(fns, total_actors):
    batch_size = TOTAL_EMPTY_TASKS // total_actors
    actors = [EmptyActor.remote() for _ in range(total_actors)]

    start = time()
    refs = []
    for i, actor in enumerate(actors):
        batch = fns[i * batch_size : (i+1) * batch_size]
        refs.append(actor.work.remote(batch))
    ray.get(refs)
    end = time()

    return end - start

if __name__ == "__main__":
    print("Total number of empty tasks: " + str(TOTAL_EMPTY_TASKS))
    print("Remote function test:")

    # empty tasks
    empty_tasks_times = [empty_task_experiment() for _ in range(TOTAL_EXPERIMENTS)]
    print("mean: " + str(mean(empty_tasks_times)) + "; std: " + str(stdev(empty_tasks_times)))
    print("Remote actor test:")

    # empty actors
    fns = [empty_fn for _ in range(TOTAL_EMPTY_TASKS)]
    empty_actors_times = [[0] * TOTAL_EXPERIMENTS] * len(NUMBER_OF_ACTORS)
    for i, actors in enumerate(NUMBER_OF_ACTORS):
        empty_actors_times[i] = [empty_actor_experiment(fns, actors) for _ in range(TOTAL_EXPERIMENTS)]
        print("mean: " + str(mean(empty_actors_times[i])) + "; std: " + str(stdev(empty_actors_times[i]))
        + " (actors: " + str(actors) + "; batch size: " + str(TOTAL_EMPTY_TASKS // actors) + ")")

    print("Total number of experiments conducted for calculating mean/std: " + str(TOTAL_EXPERIMENTS))
