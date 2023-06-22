#importing all the necessary libraries
import requests
import json
import pandas as pd
import html
import geojson
from datetime import datetime
import ast
import geopandas as gpd
import folium
import plotly.express as px

#API endpoints and authentication variable
osmcha_base_url = "https://osmcha.org/api/v1/changesets/"
changeset_base_url="https://s3.amazonaws.com/mapbox/real-changesets/production"
headers = {"Authorization": "Token d8616b9f387e0904c6df338a54a435cce714cca8"}

#retrieving today's date
end_date = datetime.today().strftime('%Y-%m-%d')

#query parameter to fetch all the tag_changes made for key==place for Nepal till date
query_parameter="page=1&page_size=500&date__lte={end_date}&tag_changes=place%3D*&geometry=%7B%22type%22%3A%22Polygon%22%2C%22coordinates%22%3A%5B%5B%5B80.057081%2C28.908481%5D%2C%5B80.518402%2C28.550701%5D%2C%5B80.567844%2C28.688262%5D%2C%5B81.210932%2C28.360793%5D%2C%5B81.318137%2C28.133657%5D%2C%5B81.884959%2C27.85697%5D%2C%5B82.708454%2C27.722606%5D%2C%5B82.735571%2C27.502349%5D%2C%5B83.317191%2C27.330165%5D%2C%5B83.388914%2C27.479763%5D%2C%5B83.855779%2C27.351867%5D%2C%5B84.145552%2C27.518703%5D%2C%5B84.62269%2C27.335921%5D%2C%5B84.643298%2C27.046168%5D%2C%5B85.212006%2C26.75818%5D%2C%5B85.634409%2C26.872193%5D%2C%5B85.850365%2C26.568401%5D%2C%5B86.333064%2C26.619121%5D%2C%5B86.730881%2C26.422438%5D%2C%5B87.071158%2C26.586016%5D%2C%5B87.340938%2C26.347758%5D%2C%5B87.888119%2C26.486951%5D%2C%5B88.007209%2C26.361033%5D%2C%5B88.163684%2C26.646132%5D%2C%5B87.98729%2C27.119507%5D%2C%5B88.195583%2C27.854728%5D%2C%5B87.831664%2C27.953049%5D%2C%5B87.725211%2C27.805471%5D%2C%5B87.171857%2C27.821334%5D%2C%5B86.576171%2C28.113427%5D%2C%5B86.411344%2C27.90684%5D%2C%5B86.186388%2C28.173806%5D%2C%5B86.000154%2C27.911588%5D%2C%5B85.711978%2C28.385833%5D%2C%5B85.608295%2C28.256302%5D%2C%5B85.119594%2C28.336352%5D%2C%5B85.187019%2C28.642005%5D%2C%5B84.856808%2C28.570342%5D%2C%5B84.485721%2C28.736595%5D%2C%5B84.226238%2C28.89306%5D%2C%5B84.094248%2C29.292947%5D%2C%5B83.578888%2C29.178893%5D%2C%5B83.277872%2C29.569912%5D%2C%5B82.171975%2C30.065456%5D%2C%5B82.08167%2C30.362086%5D%2C%5B81.40836%2C30.421039%5D%2C%5B81.218554%2C30.00737%5D%2C%5B80.540919%2C30.449022%5D%2C%5B80.877847%2C30.128313%5D%2C%5B80.365555%2C29.747803%5D%2C%5B80.297729%2C29.205258%5D%2C%5B80.057081%2C28.908481%5D%5D%5D%7D"
query_parameter = query_parameter.replace("{end_date}", end_date)

#required url with query parameter
url = f"{osmcha_base_url}?{html.unescape(query_parameter)}"

#getting response from the osmcha api endpoint in json format 
response = requests.get(url, headers=headers)
osmcha_data= response.json()

#keeping all the response in osmcha_responses
osmcha_responses=[osmcha_data]
while 'next' in response.json() and response.json()['next'] is not None:
    next_url = response.json()['next']
    response = requests.get(next_url, headers=headers)
    osmcha_responses.append(response.json())

#changing the list into dictionary 
data = {
    'type': 'FeatureCollection',
    'count': 0,
    'features': []
}
for response in osmcha_responses:
    for feature in response['features']:
            data['features'].append(feature)
            data['count'] += 1


#Getting all the required information 
changes=[]
ids=[]
changes_list = []
for feature in data['features']:
    changeset_id = str(feature['id'])
    changeset_url = f"{changeset_base_url}/{changeset_id}.json"

    response = requests.get(changeset_url)
    changeset_data = response.json()

    for x in changeset_data['elements']:
        if x['changeset'] == changeset_id:
            feature_id = x['id']
            if 'tags' in x:
                n_tags = x['tags']
                if isinstance(n_tags, str):
                    try:
                        n_tags = ast.literal_eval(n_tags.replace("'", '"'))
                    except (ValueError, SyntaxError):
                        n_tags = {}
            else:
                n_tags = {}

            coordinates=[]
            if 'lat' in x and 'lon' in x:
                coordinates = [x['lat'], x['lon']]

            if any(key == 'place' for key, value in n_tags.items()):
                changees = {
                    'changeset_id': x['changeset'],
                    'feature_id': feature_id,
                    'place': x['tags']['place'],
                    'type' : x['type'],
                    'coordinates' : coordinates
                }
                changes_list.append(changees)


#changing into dataframe and handling redundancy
df=pd.DataFrame(changes_list)
a=df.sort_values('changeset_id', ascending=False)
b=a.drop_duplicates('feature_id')
mask=b['type']=='node'
c=b[mask]

#splitting coordinates into longitude and latitude
c['latitude'] = c['coordinates'].apply(lambda x: x[0])
c['longitude'] = c['coordinates'].apply(lambda x: x[1])

#Reading .geojson file from the directory and merging it with our dataframe
wards = gpd.read_file("/home/umesh/Desktop/KLL/choropleth/wards.geojson")
gdf= gpd.GeoDataFrame(c, geometry=gpd.points_from_xy(c["longitude"], c["latitude"]))
join = gpd.sjoin(wards, gdf, op='contains')
count_by_ward = join.groupby('VDC_NAME').size()
count_df = pd.DataFrame({'count': count_by_ward})
all_districts = wards['VDC_NAME'].unique()
count_df = count_df.reindex(all_districts, fill_value=0)
wards_with_counts = wards.merge(count_df, left_on='VDC_NAME', right_index=True, how='left')

#plotting the choropleth map 
fig = px.choropleth_mapbox(wards_with_counts, geojson=wards_with_counts.geometry, locations=wards_with_counts.index,
                           color='count', color_continuous_scale='YlOrRd',
                           mapbox_style='carto-positron',
                           zoom=5, center={'lat': 27.35, 'lon': 85.65},
                           opacity=0.8,
                           labels={'count': 'counts of places per municipality'},
                           hover_data={'VDC_NAME': True, 'DISTRICT': True, 'count': True})

#saving it in .html format
fig.update_layout(margin={'r':0, 't':0, 'l':0, 'b':0})
fig.write_html('choropleth_VDC.html')