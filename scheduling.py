import json
import collections
import heapq
import time

class ProcessingFunctions():
   processing_times = {"start": 0, "vii": 20.0788, "blur": 5.9768, "night": 23.5906, "onnx": 3.3892, "emboss": 2.2084, "muse": 15.7553, "wave": 11.7784}
   
   def get_function_time(self, task):
      return self.processing_times[task]

   def get_task_time(self, task):
      return self.get_function_time(task.split('_')[0])
   
   def sum_task_time(self, task_set):
      return sum([self.get_task_time(task) for task in task_set])

class Workflow(): 
   EDGE_SET = 'edge_set'
   START = 'start'

   def __init__(self, input_file='input.json', start='muse_3', workflow='workflow_0') -> None:
      with open(input_file) as f:
         input_data = json.load(f)

         # Maps which tasks (value) require a task (key)
         causation_map = collections.defaultdict(list)
         # Maps which tasks (value) is required to begin the task (key)
         dependency_map = collections.defaultdict(list)

         '''
         Dependency map is for all intends and purposes and 
         inverse of the causation map, however is implemented here as 
         an optimisation 
         '''

         dependency_map[self.START] = [start]
         node_set = set()
         for edge in input_data[workflow][self.EDGE_SET]:
            causation_map[edge[0]].append(edge[1])
            dependency_map[edge[1]].append(edge[0])
            
            node_set.add(edge[0])
            node_set.add(edge[1])
         
         self.causation_map = causation_map
         self.dependency_map = dependency_map
         self.node_set = node_set
         self.due_dates = input_data[workflow]["due_dates"]
         print(node_set)
         f.close()
   
   '''
   This function returns the new tasks available given that the last task
   was just completed
   '''
   def get_new_tasks(self, completed_tasks):
      recently_completed_task = completed_tasks[0]
      dependencies = self.get_dependency_map()[recently_completed_task]
      new_tasks = []
      for task in dependencies:
         causation_tasks = self.get_causation_map()[task]
         causations_met = True
         for causation in causation_tasks:
            if causation not in completed_tasks:
               print('Dependency:', causation, 'not met for task:', task)
               causations_met = False
               break
         if causations_met:
            new_tasks.append(task)
      print('Task', recently_completed_task, 'enabled', new_tasks)
      return new_tasks
   
   def get_causation_map(self):
      return self.causation_map
   
   def get_dependency_map(self):
      return self.dependency_map

   def get_node_set(self):
      return self.node_set

   def get_due_dates(self):
      return self.due_dates


class Schedule():
   def __init__(self, workflow: Workflow, max_iterations: int=30000) -> None:
      self.workflow = workflow
      self.max_iterations=max_iterations
      self.total_num_tasks=32

   def sum_task_time(self):
      return ProcessingFunctions().sum_task_time(self.workflow.get_node_set())

   def schedule(self):
      total_sum = self.sum_task_time()

      print(f"total sum: {total_sum}")
      min_heap = [(0, total_sum, ["start"], [])]
      lowest_solution = (float("inf"), ["start"])
      largest_iteration=float("-inf")
      largest_min_heap_size=float("-inf")
      iterations = 0
      while min_heap and iterations <= self.max_iterations:
         lower_bound, sum_so_far, functions_called, possible_paths = heapq.heappop(min_heap)
         lowest_solution = (lower_bound, functions_called)
         largest_iteration = max(largest_iteration, len(functions_called))
         largest_min_heap_size=max(largest_min_heap_size, len(min_heap))
         
         print(f"Iteration: {iterations}")
         print(f"\tCurrent Lower Bound Score: {lower_bound}")
         print(f"\tEnd Time of Current Iteration Node {functions_called[0]} is {sum_so_far}")
         print(f"\tCurrent Best End Schedule: {functions_called}")

         possible_paths = possible_paths + self.workflow.get_new_tasks(functions_called)
         for path in possible_paths:
            new_called = [path] + functions_called
            new_lower_bound = lower_bound + max(0, sum_so_far - self.workflow.get_due_dates()[path])
            new_paths = possible_paths.copy()
            new_paths.remove(path)
            heapq.heappush(min_heap, (new_lower_bound, sum_so_far - ProcessingFunctions().get_task_time(functions_called[0]), new_called, new_paths))
         iterations += 1

      #either we found full solution, or we have to fill in the rest
      #we can fill in the rest according to remainder functions due dates
      print(f"iterations {iterations}")
      print(f"largest schedule found {largest_iteration}")
      print(f"largest minimum heap size found {largest_min_heap_size}")
      if min_heap:
         lower_bound, sum_so_far, functions_called, possible_paths = heapq.heappop(min_heap)
         print(f"most lower bound solution found {functions_called} with lower bound score {lower_bound}")
         complete_schedule = self.complete(functions_called, possible_paths)
         self.get_total_tardiness(complete_schedule)
         return complete_schedule
      else:
         return lowest_solution

   def get_total_tardiness(self, schedule):
      total_tardiness = 0
      total_processing = 0
      for function in schedule[:-1]:
         total_processing += ProcessingFunctions().get_task_time(function)
         tardiness = max(0, total_processing - self.workflow.get_due_dates()[function])
         total_tardiness += tardiness
      print(f"Total Processing of Complete Schedule: {total_processing}")
      print(f"Total Tardiness of Complete Schedule: {total_tardiness}")
      return total_tardiness


   def complete(self, functions, possible):
      print(f"Before Completing {functions}")
      while len(functions) != self.total_num_tasks:
         possible = possible + self.workflow.get_new_tasks(functions)
         get_min_due = min(possible, key=lambda x: self.workflow.due_dates[x])
         functions.insert(0, get_min_due)
         possible.remove(get_min_due)
      print(f"Afrer Completing: {functions}")
      return functions


if __name__ == "__main__":
   workflow = Workflow()
   start_time = time.perf_counter()
   optimal_schedule = Schedule(workflow).schedule()
   print(f"Scheduling took {time.perf_counter() - start_time} seconds")
   print(f"Schedule found: {optimal_schedule}")