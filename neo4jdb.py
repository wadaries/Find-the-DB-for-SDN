from py2neo import neo4j,node, rel, cypher
from sets import Set

def create_switch(graph_db,node_lst):
	for i in range(1, 7):
		s = graph_db.create({"switch_id": i, "border":"no"})
		node_lst.append(s[0])
		s[0].add_labels("switch")	



def set_borders(node_lst, border_set):
	for i in range(1, 7):
		if i in border_set:
			node_lst[i-1].set_properties({"switch_id":i, "border":"yes"})


def create_topology(graph_db, node_lst):
	graph_db.create(
	rel(node_lst[0], "GOTO", node_lst[1], {'max_flow': 1000}),
	rel(node_lst[1], "GOTO", node_lst[0], {'max_flow': 1000}),
	rel(node_lst[1], "GOTO", node_lst[2], {'max_flow': 1000}),
	rel(node_lst[2], "GOTO", node_lst[1], {'max_flow': 1000}),
	rel(node_lst[3], "GOTO", node_lst[2], {'max_flow': 1000}),
	rel(node_lst[2], "GOTO", node_lst[3], {'max_flow': 1000}),
	rel(node_lst[3], "GOTO", node_lst[4], {'max_flow': 1000}),
	rel(node_lst[4], "GOTO", node_lst[3], {'max_flow': 1000}),
	rel(node_lst[4], "GOTO", node_lst[5], {'max_flow': 1000}),
	rel(node_lst[5], "GOTO", node_lst[4], {'max_flow': 1000}),
	rel(node_lst[3], "GOTO", node_lst[5], {'max_flow': 1000}),
	rel(node_lst[5], "GOTO", node_lst[3], {'max_flow': 1000}),
	rel(node_lst[0], "GOTO", node_lst[3], {'max_flow': 1000}),
	rel(node_lst[3], "GOTO", node_lst[0], {'max_flow': 1000}),
	)

def set_configuration(graph_db, node_lst):
	graph_db.create(
	rel(node_lst[0], "CONF_TO", node_lst[1], {'flow_id': 1, 'flow_size': 1}),
	rel(node_lst[1], "CONF_TO", node_lst[2], {'flow_id': 1, 'flow_size': 1}),
	rel(node_lst[0], "CONF_TO", node_lst[3], {'flow_id': 2, 'flow_size': 1}),
	rel(node_lst[3], "CONF_TO", node_lst[4], {'flow_id': 2, 'flow_size': 1}),
	rel(node_lst[4], "CONF_TO", node_lst[5], {'flow_id': 2, 'flow_size': 1}),
	rel(node_lst[0], "CONF_TO", node_lst[3], {'flow_id': 3, 'flow_size': 1}),
	rel(node_lst[3], "CONF_TO", node_lst[5], {'flow_id': 3, 'flow_size': 1}),
	rel(node_lst[2], "CONF_TO", node_lst[4], {'flow_id': 4, 'flow_size': 1}),
	rel(node_lst[4], "CONF_TO", node_lst[5], {'flow_id': 4, 'flow_size': 1}),
	)

def dijkstra(graph_db, visited, path):
	nodes = list(graph_db.find('switch'))
	visited[nodes[0]] = 0
	while nodes:
		min_node = None
		for node in nodes:
			if node in visited:
				if min_node is None:
					min_node = node
				elif visited[node] < visited[min_node]:
					min_node = node
		 
		if min_node is None:
			break
		 
		nodes.remove(min_node)
		current_weight = visited[min_node]
		edges = list(graph_db.match(start_node=min_node, rel_type="GOTO"))
		for edge in edges:
			end_node = edge.end_node
			weight = current_weight + 1
			if end_node not in visited or weight < visited[end_node]:
				visited[end_node] = weight
				path[end_node] = min_node

def create_reachability(graph_db):
	for i in range(1, 5):
		#find longest path for a certain flow id
		query = neo4j.CypherQuery(graph_db, 
		"MATCH p =(a:switch)-[:CONF_TO*2.. { flow_id:"+str(i)+" }]->(b:switch)" + 
		"WHERE not(b-[:CONF_TO {flow_id:"+str(i)+"}]->()) and not(()-[:CONF_TO {flow_id:"+str(i)+"}]->a) RETURN p")
		for record in query.stream():
			pv_lst = []
			for node in record[0].nodes:
				pv_lst.append(node["switch_id"])
			#print pv_lst
			graph_db.create(
			rel(record[0].nodes[0], "CAN_REACH", record[0].nodes[-1], 
			{'flow_id':i, 'hop_num': len(pv_lst),'pv': pv_lst})
			)	

def create_shortest_path_view(graph_db):
	query = neo4j.CypherQuery(graph_db, "MATCH p = (a)-[:CAN_REACH]->(b) RETURN p")
	for record in query.stream():
		for r in record[0].relationships:
			flow_id = r['flow_id']
			end_id = str(r.end_node['switch_id'])
			start_id = str(r.start_node['switch_id'])
			sp = neo4j.CypherQuery(graph_db, 
			"MATCH (a:switch{switch_id:"+start_id+"}),(b:switch {switch_id:"+end_id+"}),"+
			"p = shortestPath((a)-[:GOTO*..]->(b)) RETURN p")
			for result in sp.stream():
				pv_lst = []
				for node in result[0].nodes:
					pv_lst.append(node["switch_id"])
				print "shotest path calculated"
				print pv_lst
				graph_db.create(
				rel(result[0].nodes[0], "SHORTEST_PATH", result[0].nodes[-1], 
				{'flow_id':flow_id, 'hop_num': len(pv_lst),'pv': pv_lst})
				)

	
def construct_db(graph_db):
	node_lst = []
	border_set = Set([1,3,6])
	create_switch(graph_db, node_lst)
	set_borders(node_lst, border_set)
	create_topology(graph_db, node_lst)
	set_configuration(graph_db, node_lst)
	create_reachability(graph_db)
	create_shortest_path_view(graph_db)

if __name__ == '__main__':
	graph_db = neo4j.GraphDatabaseService()
	construct_db(graph_db)
	del_flag = raw_input ('Clean the added database (y/n): ')
	if del_flag == 'y':
		graph_db.clear()












