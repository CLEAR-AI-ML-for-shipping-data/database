import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2

def load_data_into_postgis():
    # Step 1: Load the Natural Earth GeoPackage into GeoPandas
    gdf = gpd.read_file("data/ne_10m_ports/ne_10m_ports.shp")  

    gdf_depth_0 = gpd.read_file("data/ne_10m_bathymetry_all/ne_10m_bathymetry_L_0.shp")
    gdf_depth_200 = gpd.read_file("data/ne_10m_bathymetry_all/ne_10m_bathymetry_K_200.shp")
    gdf_depth_1000 = gpd.read_file("data/ne_10m_bathymetry_all/ne_10m_bathymetry_J_1000.shp")


    # Step 2: Connect to PostGIS Database
    # Replace with your actual PostgreSQL/PostGIS credentials
    POSTGRES_DB="gis"
    POSTGRES_USER="clear"
    POSTGRES_PASSWORD="clear"
    POSTGRES_PORT=5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    engine = create_engine(database_url)

    # Step 3: Insert Data into PostGIS
    gdf.to_postgis(name="ne_ports", con=engine, if_exists="replace", index=False)

    gdf_depth_0.to_postgis(name="ne_depth_0", con=engine, if_exists="replace", index=False)
    gdf_depth_200.to_postgis(name="ne_depth_200", con=engine, if_exists="replace", index=False)
    gdf_depth_1000.to_postgis(name="ne_depth_1000", con=engine, if_exists="replace", index=False)

    print("Data successfully inserted into the PostGIS database!")


def plot():
    import folium

    gdf_ports = gpd.read_file("data/ne_10m_ports/ne_10m_ports.shp")  
 
    # df = gdf_ports

    # Save the DataFrame to a CSV file
    # df.to_csv("ports.csv", index=False)

    gdf_depth_0 = gpd.read_file("data/ne_10m_bathymetry_all/ne_10m_bathymetry_L_0.shp")
    gdf_depth_200 = gpd.read_file("data/ne_10m_bathymetry_all/ne_10m_bathymetry_K_200.shp")
    gdf_depth_1000 = gpd.read_file("data/ne_10m_bathymetry_all/ne_10m_bathymetry_J_1000.shp")


    # Create a Folium map centered around the centroid
    m = folium.Map(location=[57.708870, 11.974560], zoom_start=10, tiles='CartoDB dark_matter')

    # Convert GeoDataFrame to GeoJSON and add it to the map
    folium.GeoJson(
        gdf_ports.to_json(),
        name='ports'
    ).add_to(m)

    folium.GeoJson(
        gdf_depth_0.to_json(),
        name='d 0'
    ).add_to(m)

    folium.GeoJson(
        gdf_depth_200.to_json(),
        name='d 200'
    ).add_to(m)

    folium.GeoJson(
        gdf_depth_1000.to_json(),
        name='d 1000'
    ).add_to(m)


    # Add a layer control panel
    folium.LayerControl().add_to(m)

    # Save the map to an HTML file
    m.save('ne_map.html')

if __name__=='__main__':
    # load_data_into_postgis()
    plot()