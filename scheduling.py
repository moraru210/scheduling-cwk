import json
import collections
import heapq

def calculate_total_sum():
   sum = float(0)
   for task in node_set:
      type = task.split("_")[0]
      sum += processing_times[type]
   return sum


def schedule():
   total_sum = calculate_total_sum()
   print(f"total sum: {total_sum}")
   min_heap = [(0, total_sum, ["start"])]
   prev_solution = (0, ["start"])
   iterations = 0
   while min_heap and iterations <= 4000:
      lower_bound, sum_so_far, functions_called = heapq.heappop(min_heap)
      prev_solution = (lower_bound, functions_called)
      possible_paths = adj_list[functions_called[-1]]
      for path in possible_paths:
         path_func = path.split("_")[0]
         new_called = functions_called + [path]
         new_lower_bound = lower_bound + max(0, sum_so_far - due_dates[path])
         heapq.heappush(min_heap, (new_lower_bound, sum_so_far - processing_times[path_func], new_called))
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
   processing_times = {"vii": 20.0788, "blur": 5.9768, "night": 23.5906, "onnx": 3.3892, "emboss": 2.2084, "muse": 15.7553, "wave": 11.7784}
   
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

   optimal_schedule = schedule()
   print(optimal_schedule)