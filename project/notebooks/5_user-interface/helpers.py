import pandas as pd
import datetime as dt
import numpy as np
from tqdm import tqdm
import warnings
from IPython.display import display
warnings.filterwarnings('ignore')


# +
def Id_from_name(name, locations):
    return locations[locations['stops_filtered.stop_name'] == name]['stops_filtered.stop_id'].values[0]

def name_from_Id(Id, locations):
    return locations[locations['stops_filtered.stop_id'] == Id]['stops_filtered.stop_name'].values[0]


# +
from scipy.sparse import hstack
import pickle

# loading models
with open(f'../../models/transport_type_ohe.pkl','rb') as f:
    transport_type_ohe = pickle.load(f)

with open(f'../../models/stop_id_ohe.pkl','rb') as f:
    stop_id_ohe = pickle.load(f)

with open(f'../../models/scaler.pkl','rb') as f:
    scaler = pickle.load(f)

with open(f'../../models/log.pkl','rb') as f:
    log = pickle.load(f)

# Give the bin for the delay
def label_bin(x):
    bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30]
    bins_id = range(len(bins) + 1)
    for i, b in enumerate(bins):
        if x <= b:
            return bins_id[i]
    return bins_id[-1]

def delay_proba(delay_max, arrival_time_shedule, transport_type, stop_id):
        
    try:
        df = pd.DataFrame(columns=['arrival_time_shedule', 'transport_type', 'stop_id'])
        df.loc[0] = [arrival_time_shedule, str(transport_type), str(stop_id)]

        # Compute the predictors
        df["minute"] = df.arrival_time_shedule.dt.minute
        df["hour"] = df.arrival_time_shedule.dt.hour
        df["month"] = df.arrival_time_shedule.dt.month
        df["year"] = df.arrival_time_shedule.dt.year
        df["week_of_year"] = df.arrival_time_shedule.dt.isocalendar().week
        df["day_of_year"] = df.arrival_time_shedule.dt.day_of_year
        df["day_of_week"] = df.arrival_time_shedule.dt.day_of_week

        inputCols = ["minute", "hour", "month", "year", "week_of_year", "day_of_year", "day_of_week"]
        ohe_cols = ["transport_type", "stop_id"]
        X = df[inputCols].to_numpy(dtype=int)
        
        # Scale data
        X = scaler.transform(X)
        
        # One hot encode transport type
        transformed = transport_type_ohe.transform(df[[ohe_cols[0]]])
        X = hstack((X, transformed), format='csr')

        # One hot encode stop id
        transformed = stop_id_ohe.transform(df[[ohe_cols[1]]])
        X = hstack((X, transformed), format='csr')

        # Prediect the probability distribution
        prob = log.predict_proba(X)[0]

        # Get the delay bin
        delay_bin = label_bin(delay_max)

        # The probability of arrival time below delay_max is the sum of the prediected probability for delay <= delay_max
        total_proba = sum(prob[:delay_bin + 1])

        return total_proba
    
    # If the model fail for some raison
    except:
        return min(0.99 ** (10 - delay_max), 1)


# -

def get_tables(TRAVEL_DAY, ARRIVAL_TIME):
    
    print('Please wait, data is downloading.')
    
    # Get the table for network
    dtype_graph = { 'src_stop_id': str,
                    'trip_id': str,
                    'src_departure_time': str,
                    'vehicule': str,
                    'monday': np.int64,
                    'tuesday': np.int64,
                    'wednesday': np.int64,
                    'thursday': np.int64,
                    'friday': np.int64,                    
                    'dst_stop_id': str,
                    'dst_arrival_time': str,
                    'duration': np.int64}
    
    df_graph = pd.read_csv('../../5km_data/all_edges.csv', dtype = dtype_graph)
    # To use a small dataset for testing (1km radius around Zürich HB): df_graph = pd.read_csv('../../small_data/all_edges.csv', dtype = dtype_graph)
    # To use a small dataset for testing (5km radius around Zürich HB): df_graph = pd.read_csv('../../5km_data/all_edges.csv', dtype = dtype_graph)
    
    
    df_graph['dst_arrival_time'] = df_graph['dst_arrival_time'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df_graph['src_departure_time'] = df_graph['src_departure_time'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
     
    df_graph = df_graph.loc[(df_graph[TRAVEL_DAY]==1) & (df_graph['dst_arrival_time']<=ARRIVAL_TIME)].copy() 
    df_graph.drop(columns=['monday', 'tuesday', 'wednesday', 'thursday', 'friday'], inplace=True)
    
    print('Data loaded')    
    
    return df_graph


def routing(df_graph, arrival_stop_id, arrival_time, proba_threshold):
    
    df_graph.sort_values(by=['src_departure_time'], ascending=False, inplace=True) # ordering the edges in descending order according to their starting time
    
    transfer_time = dt.timedelta(minutes=2) # minimal transfer time between two different transports at the same location
    distinct_stops = pd.concat([df_graph['src_stop_id'],df_graph['dst_stop_id']]).unique() # retrieving the nodes of the graph from the edges
    path = dict()  # dict that takes as key a departure stop_id and as value a list [departure_time, arrival_time, arrival stop_id, transport_ID, proba_sucess, transport mode]
    
    ### 1. Put default list for each stop ###
    
    for stop_id in distinct_stops:
        
        if (stop_id == arrival_stop_id):
            path[stop_id] = [arrival_time, arrival_time, arrival_stop_id, None, 1, None]
       
        else:
            path[stop_id] = [dt.datetime.strptime('00:00:01', '%H:%M:%S'), dt.datetime.strptime('00:00:01', '%H:%M:%S'), None, None, None, None]   
            
    ### 2. Scans each edge to update the list of each stop ###
    
    progress_bar = tqdm(total=len(df_graph), desc='Processing', unit='iteration')
    for i, edge in df_graph.iterrows():
        
        u = edge['src_stop_id']  # stop_id of departure
        v = edge['dst_stop_id']  # stop_id of arrival
        t = edge['src_departure_time']  # instant of departure
        delta = dt.timedelta(minutes=edge['duration'])  # duration of the journey
        trip_id = edge['trip_id']  # trip_id
        vehicule = edge['vehicule']  # vehicule
        transfer_time = dt.timedelta(minutes=2) #redefined here since it might become 0 if we walk or we do not change transport type cf. what follows
        
        if ((trip_id == path[v][3]) or (vehicule=='foot')):  # checking if the transport ID of the edge (connection) is the same as the one we are taking afterwards (from v) 
            transfer_time = dt.timedelta(minutes=0)  # no transfer time between two same transport id
            if ((t + delta) <= path[v][0]):  # checking if the connection makes us arrive at stop v before the next transport departs from stop v
                if (t > path[u][0]):  # updating the path only if the connection makes us depart from u later (we want the latest departure time)
                    path[u][0] = t 
                    path[u][1] = t + delta
                    path[u][2] = v
                    path[u][3] = trip_id
                    path[u][4] = path[v][4]
                    path[u][5] = vehicule

        else :  # changing transport ID
            if v == arrival_stop_id: # no need to add transfer time if we arrive at the final destination
                transfer_time = dt.timedelta(minutes=0)
     
            if ((t + delta + transfer_time) <= path[v][0]): # same logic as before but making sure that we keep some time (transfer_time) between the two connections
                if (t > path[u][0]):
                    delay_max = path[v][0] - (t + delta + transfer_time)  # maximum delay time
                    proba_delay = delay_proba(delay_max.seconds // 60 % 60, t + delta, vehicule, v) # computing the probability to make the connection based on this maximum delay time
                    proba_success = 1 - proba_delay
                    if (proba_success*path[v][4] > proba_threshold/100): #updating the path if the probability is sufficiently high
                        path[u][0] = t
                        path[u][1] = t + delta
                        path[u][2] = v
                        path[u][3] = trip_id
                        path[u][4] = proba_success*path[v][4]
                        path[u][5] = vehicule
        
        progress_bar.update(1)
        
    progress_bar.close()
    return path


def recover_path(output, departure_stop_id, arrival_stop_id, locations):
    cur_node = departure_stop_id
    path = []
    if output[cur_node][2] is not None: # Check if path from depart to arrival stops exist
        while cur_node != arrival_stop_id:
            next_node = output[cur_node][2]
            output[cur_node][2] = name_from_Id(output[cur_node][2],locations)
            output[cur_node].append(name_from_Id(cur_node,locations))
            path.append(output[cur_node])
            if next_node is not None:
                cur_node = next_node
    return path


def print_path(path):
    if path == []:
        print('No possible path was found')
    else:
        proba_success = []
        for p in path:
            proba_success.append(p[4]*100)

        table = pd.DataFrame(path)
        table.columns = ['Latest departure', 'Arrival time', 'To', 'Transport_ID', 'Proba', 'By', 'From']
        table['Proba'] = table['Proba'].round(2)
        table['Latest departure'] = table['Latest departure'].apply(lambda x: x.time())
        table['Arrival time'] = table['Arrival time'].apply(lambda x: x.time())

        desired_order = ['Transport_ID', 'By', 'From', 'Latest departure', 'To', 'Arrival time', 'Proba']
        table = table.reindex(columns=desired_order)

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', None)
        display(table.sort_values(by=['Latest departure']))

        print( f'\nProbability success of this path: {"{:.2f}".format(proba_success[0])}% .')
