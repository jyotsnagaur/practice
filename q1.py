import pandas as pd
import json

# Define the JSON inputs
cars_data = {
    "id": "Cars",
    "type": [
        {"name": "Tesla", "min": "100", "max": "500"},
        {"name": "Toyota", "min": "50", "max": "400"}
    ]
}

owner_data = {
    "id": "Owner",
    "persons": [
        {"name": "Amit", "owns": ["Tesla"]},
        {"name": "Bodh", "owns": ["Tesla", "Toyota"]}
    ]
}

# Normalize the cars data into a DataFrame
df_cars = pd.DataFrame(cars_data['type'])

# Convert owner data into a DataFrame with an expanded 'owns' column
owners_list = []
for person in owner_data['persons']:
    for car in person['owns']:
        owners_list.append({'name': person['name'], 'owns': car})
df_owners = pd.DataFrame(owners_list)

# Merge the owners DataFrame with the cars DataFrame to get car details for each owner
merged_df = pd.merge(df_owners, df_cars, left_on='owns', right_on='name', how='left')

# Group by owner to aggregate car details and count
aggregated_data = merged_df.groupby('name_x').apply(lambda x: pd.Series({
    'count': x['name_y'].count(),
    'owns': x[['name_y', 'min', 'max']].to_dict('records')
})).reset_index().rename(columns={'name_x': 'name'})

# Prepare the final output structure
result_json = {
    "id": "Owner_cars",
    "persons": []
}

for _, row in aggregated_data.iterrows():
    person_cars = [{'name': car['name_y'], 'min': car['min'], 'max': car['max']} for car in row['owns']]
    result_json['persons'].append({
        'name': row['name'],
        'count': row['count'],
        'owns': person_cars
    })

# Write the result to a JSON file
output_path = "C:/Users/arjyo/PycharmProjects/practice/owner_cars_corrected.json"
with open(output_path, 'w') as outfile:
    json.dump(result_json, outfile)
