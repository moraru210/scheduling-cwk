import json
import collections
import heapq

class ProcessingFunctions():
   processing_times = {"vii": 20.0788, "blur": 5.9768, "night": 23.5906, "onnx": 3.3892, "emboss": 2.2084, "muse": 15.7553, "wave": 11.7784}
   
   def get_function_time(self, task):
      return self.processing_times[task]

   def get_task_time(self, task):
      return self.get_function_time(task.split('_')[0])
   
   def sum_task_time(self, task_set):
      return sum([self.get_task_time(task) for task in task_set])

class Workflow(): 
   EDGE_SET = 'edge_set'
   START = 'start'

   def __init__(self, input_file='input.json', start='emboss_8', workflow='workflow_0') -> None:
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

         causation_map[self.START] = [start]
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
      # The task we just completed is last
      recently_completed_task = completed_tasks[-1]
      affected_tasks = self.get_causation_map()[recently_completed_task]
      new_tasks = []
      for task in affected_tasks:
         dependencies = self.get_dependency_map()[task]
         dependencies_met = True
         for dependency in dependencies:
            if dependency not in completed_tasks:
               print('Dependency:', dependency, 'not met for task:', task)
               dependencies_met = False
               break
         if dependencies_met:
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
   def __init__(self, workflow: Workflow, max_iterations: int=100000) -> None:
      self.workflow = workflow
      self.max_iterations=max_iterations

   def sum_task_time(self):
      return ProcessingFunctions().sum_task_time(self.workflow.get_node_set())

   def schedule(self):
      total_sum = self.sum_task_time()

      print(f"total sum: {total_sum}")
      min_heap = [(0, total_sum, ["start"], [])]
      prev_solution = (0, ["start"])
      iterations = 0
      while min_heap and iterations <= self.max_iterations:
         lower_bound, sum_so_far, functions_called, possible_paths = heapq.heappop(min_heap)
         prev_solution = (lower_bound, functions_called)

         possible_paths = possible_paths + self.workflow.get_new_tasks(functions_called)
         for path in possible_paths:
            new_called = functions_called + [path]
            new_lower_bound = lower_bound + max(0, sum_so_far - self.workflow.get_due_dates()[path])
            new_paths = possible_paths.copy()
            new_paths.remove(path)
            heapq.heappush(min_heap, (new_lower_bound, sum_so_far - ProcessingFunctions().get_task_time(path), new_called, new_paths))
         iterations += 1

      #either we found full solution, or we have to fill in the rest
      #we can fill in the rest according to remainder functions due dates
      print(f"iterations {iterations}")
      if min_heap:
         return heapq.heappop(min_heap)
      else:
         return prev_solution

if __name__ == "__main__":
   workflow = Workflow()
   optimal_schedule = Schedule(workflow).schedule()
   print(optimal_schedule)





# def load_graph(N):
#    G = [[0 for _ in range(1)] for _ in range(N)]
#    G[0,30]=1
#    G[1,0]=1
#    G[2,7]=1
#    G[3,2]=1
#    G[4,1]=1
#    G[5,15]=1
#    G[6,5]=1
#    G[7,6]=1
#    G[8,7]=1
#    G[9,8]=1
#    G[10,4]=1
#    G[11,4]=1
#    G[12,11]=1
#    G[13,12]=1
#    G[14,10]=1
#    G[15,14]=1
#    G[16,15]=1
#    G[17,16]=1
#    G[18,17]=1
   # G[19,18]=1
   # G[20,17]=1
   # G[21,20]=1
   # G[22,21]=1
   # G[23,4]=1
   # G[24,23]=1
   # G[25,24]=1
   # G[26,25]=1
   # G[27,25]=1
   # G[28,27]=1
   # G[29,3]=1
   # G[29,9]=1
   # G[29,13]=1
   # G[29,19]=1
   # G[29,22]=1
   # G[29,26]=1
   # G[29,28]=1