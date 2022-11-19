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



class Schedule():
   def __init__(self, task_set, max_iterations=4000) -> None:
      self.node_set = task_set
      self.max_iterations=max_iterations
   
   def sum_task_time(self):
      return ProcessingFunctions().sum_task_time(self.node_set)

   def schedule(self):
      total_sum = self.sum_task_time()

      print(f"total sum: {total_sum}")
      min_heap = [(0, total_sum, ["start"])]
      prev_solution = (0, ["start"])
      iterations = 0
      while min_heap and iterations <= self.max_iterations:
         lower_bound, sum_so_far, functions_called = heapq.heappop(min_heap)
         prev_solution = (lower_bound, functions_called)
         possible_paths = adj_list[functions_called[-1]]
         for path in possible_paths:
            new_called = functions_called + [path]
            new_lower_bound = lower_bound + max(0, sum_so_far - due_dates[path])
            heapq.heappush(min_heap, (new_lower_bound, sum_so_far - ProcessingFunctions().get_task_time(path), new_called))
         iterations += 1

      #either we found full solution, or we have to fill in the rest
      #we can fill in the rest according to remainder functions due dates
      print(f"iterations {iterations}")
      if min_heap:
         return heapq.heappop(min_heap)
      else:
         return prev_solution

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


    
if __name__ == "__main__":
   f = open('input.json')
   input_data = json.load(f)

   adj_list = collections.defaultdict(list)
   adj_list["start"] = ["emboss_8"]
   node_set = set()
   for edge in input_data["workflow_0"]["edge_set"]:
      adj_list[edge[0]].append(edge[1])
      node_set.add(edge[0])
      node_set.add(edge[1])

   due_dates = input_data["workflow_0"]["due_dates"]

   optimal_schedule = Schedule(node_set).schedule()
   print(optimal_schedule)