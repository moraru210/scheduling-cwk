import json
import collections

def schedule():
   print("hello world!")


if __name__ == "__main__":
   processing_times = {"vii": 20.0788, "blur": 5.9768, "night": 23.5906, "onnx": 3.3892, "emboss": 2.2084, "muse": 15.7553, "wave": 11.7784}
   
   f = open('input.json')
   input_data = json.load(f)

   adj_list = collections.defaultdict(list)
   for edge in input_data["workflow_0"]["edge_set"]:
      adj_list[edge[0]].append(edge[1])

   due_dates = input_data["workflow_0"]["due_dates"]
   
   schedule()