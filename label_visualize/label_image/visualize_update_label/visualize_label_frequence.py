import os, os.path
#from os.path import splitext, basename, join
import csv
import numpy as np
import networkx as nx
from itertools import combinations
from random import random

# path cua file content
file_='C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/data_2017-06-23 04_32_17 PM out.csv'
list_json = []
lable_unique = []
frequence = []
relationship = []
edge_data = []
i = 0
with open (file_, 'r') as csvfile:
    #reader=json.load(file_json)
    #reader=csv.reader(csvfile , delimiter=',', quoting=csv.QUOTE_NONE)
    reader=csv.reader(csvfile)
    for row in reader:
        # Deleted the end element (frequence)
        row_content = row[:len(row) - 1]
        list_json.append(row)
        # Create lable_unique
        for value in row_content:
            if value not in lable_unique:
                lable_unique.append(value)
        if len(row_content) > 1:
            # Create list all edge in row
            edge_row = list(combinations(row_content, 2))
            edge_data.append(edge_row)

# # Create all label unique
# list_label_unique='C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/list_label_unique.csv'
# with open (list_label_unique,'w') as f:
#     json.dump(lable_unique,f)

# list_label_unique='C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/list_label_unique.csv'
# with open(list_label_unique, 'w', newline="") as f:
#     wr = csv.writer(f, quoting=csv.QUOTE_ALL)
#     wr.writerows(lable_unique)

# Count frequence of each node
frequence = [0] * len(lable_unique)
for i in range(len(lable_unique)):
    for row in list_json:
            freq = row.count(lable_unique[i])
            frequence[i] = frequence[i] + freq * (int)(row[len(row) - 1])

################################ OPTION #########################
# Set node size, scale with node_size = freq / scale
scale = 10.0
# Conditions for the show node. frequence > freq
freq = 0.5
# Show label, font_size label
show_label = True
font_size = 8

frequence = [(frequence[i] / scale) for i in range(len(lable_unique))]




# Create a graph
graph = nx.Graph()
# Add list node
graph.add_nodes_from(lable_unique)
# Add list edge
for list_edge in edge_data:
    graph.add_edges_from(list_edge)


# Delete node have frequence < freq
n = 0
for i in range(len(lable_unique) - 1, -1, -1):
    if frequence[i] < freq:
        del frequence[i]
        graph.remove_node(lable_unique[i])
        n = n + 1
print ("Numbers node is deleted/Total : %d/%d" %(n, len(lable_unique)))

# Create color for each node
colors = [(random()) for _i in range(len(frequence))]

nx.draw_spring(graph, node_color=colors, node_size=frequence, node_shape='o', weight=1, with_labels=show_label, font_size=font_size, font_family='sans-serif')


import matplotlib.pyplot as plt
plt.show()
