# Import
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML
from datetime import time
from helpers import * 

# +
# upload data
locations = pd.read_csv('../../5km_data/stops.csv')
# To use a small dataset for testing (1km radius around Zürich HB): locations = pd.read_csv('../../small_data/stops.csv')
# To use a small dataset for testing (5km radius around Zürich HB): locations = pd.read_csv('../../5km_data/stops.csv')

locations["stops_filtered.parent_station"] = locations["stops_filtered.parent_station"].fillna("")
# Remove rows where "stops_filtered.parent_station" column contains "Parent"
locations = locations[~locations["stops_filtered.parent_station"].str.contains("Parent")]
# Remove rows where "stops_filtered.stop_id" column contains "Parent"
locations = locations[~locations["stops_filtered.stop_id"].str.contains("Parent")]
# Remove letters from "stops_filtered.stop_id" column
locations["stops_filtered.stop_id"] = locations["stops_filtered.stop_id"].astype(str).str.replace(r'\D', '')


# -

def get_locations():
    names = [loc for loc in locations['stops_filtered.stop_name']]
    return names


def get_input():
    locations = get_locations()
    day = widgets.Dropdown(
        options=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
        description='Pick a day: ',
        style = {'description_width': 'initial'},
        disabled=False,
    )
    display(day)
    
    arrival_time = widgets.TimePicker(
        description='Pick an arrival time: ',
        style = {'description_width': 'initial'},
        min = time(5,0,0),
        max = time(23,0,0),
        disabled=False
    )
    display(arrival_time)
    
    departure_location = widgets.Combobox(
        placeholder='Choose departure location',
        options= locations,
        description='Departure location: ',
        style = {'description_width': 'initial'},
        ensure_option=True,
        disabled=False
    )
    display(departure_location)

    arrival_location = widgets.Combobox(
        placeholder='Choose arrival location',
        options= locations,
        description='Arrival location: ',
        style = {'description_width': 'initial'},
        ensure_option=True,
        disabled=False
    )
    display(arrival_location)
    
    confidence = widgets.IntSlider(
        min=0,
        max=100,
        step=1,
        value = 90,
        description='Confidence tolerance [%]: ',
        style = {'description_width': 'initial'},
        disabled=False,
        continuous_update=False,
    )
    display(confidence)
    
    return day, arrival_time, departure_location, arrival_location, confidence


# Validates button when values are selected
def validate_values(day, arrival_time, departure_location, arrival_location, confidence):
    return (arrival_time.value is None or departure_location.value == '' or arrival_location.value == '')


def get_output(day, arrival_time, departure_location, arrival_location, confidence):
    button = widgets.Button(
        description='Search travel path',
        disabled= validate_values(day, arrival_time, departure_location, arrival_location, confidence),
        button_style=''
    )
    output = widgets.Output()
    display(button, output)
    

    def run_algorithm(b):
        with output:
            departure_id = Id_from_name(departure_location.value, locations)
            arrival_id = Id_from_name(arrival_location.value, locations)
            arrival_time_value = dt.datetime.strptime(str(arrival_time.value), '%H:%M:%S')
            
            network = get_tables(day.value.lower(), arrival_time_value)
            path_output = routing(network, arrival_id, arrival_time_value, confidence.value)

            path = recover_path(path_output, departure_id, arrival_id, locations)
            print_path(path)

    button.on_click(run_algorithm)
    
    ## Handles changes
    def validate_and_update_button(change):
        button.disabled = validate_values(day, arrival_time, departure_location, arrival_location, confidence)

    day.observe(validate_and_update_button, 'value')
    arrival_time.observe(validate_and_update_button, 'value')
    departure_location.observe(validate_and_update_button, 'value')
    arrival_location.observe(validate_and_update_button, 'value')
    confidence.observe(validate_and_update_button, 'value')


def run():
    day, arrival_time, departure_location, arrival_location, confidence = get_input()
    
    display(HTML("<br><div> After selecting the departure location, the arrival location, the day, the arrival time and the confidence tolerance, click on the button to compute the fastest and surest route.</div>"))
    get_output(day, arrival_time, departure_location, arrival_location, confidence)

