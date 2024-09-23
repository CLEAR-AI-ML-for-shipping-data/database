import psycopg2
import geopandas as gpd

# Database connection parameters

POSTGRES_DB="gis"
POSTGRES_USER="clear"
POSTGRES_PASSWORD="clear"
POSTGRES_PORT=5432
POSTGRES_HOST = "localhost"
database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

conn_params = {
    'dbname': POSTGRES_DB,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'host': 'localhost',
    'port': POSTGRES_PORT
}

# Establish connection
conn = psycopg2.connect(**conn_params)

# Query to extract all nautical-related features from planet_osm_line and planet_osm_point
query_line = """
SELECT way, name, waterway, harbour
FROM planet_osm_line
WHERE waterway IS NOT NULL;
"""

query_point = """
SELECT way, name, harbour, wetland, waterway
FROM planet_osm_point
WHERE harbour IS NOT NULL;
"""

# Read data into GeoDataFrame
gdf_line = gpd.read_postgis(query_line, conn, geom_col='way')
gdf_point = gpd.read_postgis(query_point, conn, geom_col='way')

# Close the database connection
conn.close()


import folium

# Initialize a Folium map centered around a specific location
m = folium.Map(location=[48.8566, 2.3522], zoom_start=10)  # Center on Paris for example

# Plot lines (e.g., waterways)
for _, row in gdf_line.iterrows():
    sim_geo = gpd.GeoSeries(row['harbour']).simplify(tolerance=0.001)
    geo_j = sim_geo.to_json()
    geo_j = folium.GeoJson(data=geo_j, style_function=lambda x: {'color': 'black'},name='lines')
    geo_j.add_to(m)

# Plot points (e.g., seamarks)
# for _, row in gdf_point.iterrows():
#     folium.Marker(
#         location=[row['way'].y, row['way'].x],
#         popup=row['name'] if row['name'] else 'Unnamed',
#         icon=folium.Icon(color='red', icon='info-sign'), name="points"
#     ).add_to(m)

folium.LayerControl().add_to(m)

# Save or display the map
m.save('openseamap_features.html')

