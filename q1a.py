import pandas as pd
import json
import unittest


# Function to normalize and merge data
def normalize_and_merge(cars_data, owner_data):
    try:
        df_cars = pd.DataFrame(cars_data['type'])
        owners_list = []
        for person in owner_data['persons']:
            for car in person['owns']:
                owners_list.append({'name': person['name'], 'owns': car})
        df_owners = pd.DataFrame(owners_list)
        merged_df = pd.merge(df_owners, df_cars, left_on='owns', right_on='name', how='left')

        aggregated_data = merged_df.groupby('name_x').apply(lambda x: pd.Series({
            'count': x['name_y'].count(),
            'owns': x[['name_y', 'min', 'max']].to_dict('records')
        })).reset_index().rename(columns={'name_x': 'name'})

        result_json = {"id": "Owner_cars", "persons": []}
        for _, row in aggregated_data.iterrows():
            person_cars = [{'name': car['name_y'], 'min': car['min'], 'max': car['max']} for car in row['owns']]
            result_json['persons'].append({
                'name': row['name'],
                'count': row['count'],
                'owns': person_cars
            })
        return result_json

    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle or re-raise the exception as needed
        raise


# Function to write JSON to file
def write_json_to_file(data, output_path):
    try:
        with open(output_path, 'w') as outfile:
            json.dump(data, outfile)
    except Exception as e:
        print(f"Error writing file: {e}")
        # Handle or re-raise the exception as needed
        raise


# Unit tests for the functionality
class TestDataProcessing(unittest.TestCase):
    def test_normalize_and_merge(self):
        # Example test case
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

        result = normalize_and_merge(cars_data, owner_data)
        self.assertTrue('persons' in result)
        self.assertEqual(len(result['persons']), 2)


if __name__ == '__main__':
    # Run unit tests
    unittest.main()
