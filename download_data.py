import os
import urllib.request
import zipfile
import pandas as pd

DATA_DIR = "dataset"
DATA_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
ZIP_PATH = os.path.join(DATA_DIR, "ml-latest-small.zip")
EXTRACT_DIR = os.path.join(DATA_DIR, "ml-latest-small")

def download_and_prepare_data():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    if not os.path.exists(ZIP_PATH):
        print("Downloading MovieLens dataset...")
        urllib.request.urlretrieve(DATA_URL, ZIP_PATH)

    print("Extracting dataset...")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(DATA_DIR)

    # Load raw CSVs
    ratings_df = pd.read_csv(os.path.join(EXTRACT_DIR, "ratings.csv"))
    movies_df = pd.read_csv(os.path.join(EXTRACT_DIR, "movies.csv"))

    # Calculate Nodes
    unique_users = ratings_df['userId'].nunique()
    unique_movies = movies_df['movieId'].nunique()
    
    # Process Genres
    genres_set = set()
    movie_genre_edges = 0
    
    for genres_str in movies_df['genres']:
        if pd.notna(genres_str) and genres_str != "(no genres listed)":
            g_list = genres_str.split('|')
            genres_set.update(g_list)
            movie_genre_edges += len(g_list)

    total_nodes = unique_users + unique_movies + len(genres_set)
    total_relationships = len(ratings_df) + movie_genre_edges

    print("\n==========================================")
    print("      UPDATED GRAPH DATASET SUMMARY       ")
    print("==========================================")
    print(f"Nodes:")
    print(f"  - Users:        {unique_users:,}")
    print(f"  - Movies:       {unique_movies:,}")
    print(f"  - Genres:       {len(genres_set):,}")
    print(f"  - TOTAL NODES:  {total_nodes:,}")
    print("------------------------------------------")
    print(f"Relationships:")
    print(f"  - RATED:        {len(ratings_df):,}")
    print(f"  - HAS_GENRE:    {movie_genre_edges:,}")
    print(f"  - TOTAL EDGES:  {total_relationships:,}")
    print("==========================================")

if __name__ == "__main__":
    download_and_prepare_data()