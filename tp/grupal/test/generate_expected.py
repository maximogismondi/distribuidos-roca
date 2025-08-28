import json
from typing import Any
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib.pyplot as plt
import os
import langid
import time
import ast
import timeit
from transformers import pipeline
from torch import cuda

print("Is GPU available:", cuda.is_available())

MOVIES_PATH = os.path.join(os.path.dirname(
    __file__), '../.data/movies_metadata.csv')
RATINGS_PATH = os.path.join(os.path.dirname(__file__), '../.data/ratings.csv')
CREDITS_PATH = os.path.join(os.path.dirname(__file__), '../.data/credits.csv')
EXPECTED_FILE = os.path.join(os.path.dirname(
    __file__), "expected_results.json")

print("Load movies_metadata.csv")
movies_df = pd.read_csv(MOVIES_PATH, low_memory=False)

print("Load ratings.csv")
ratings_df = pd.read_csv(RATINGS_PATH)

print("Load credits.csv")
credits_df = pd.read_csv(CREDITS_PATH)

movies_df_columns = ["id", "title", "genres", "release_date", "overview",
                     "production_countries", "spoken_languages", "budget", "revenue"]
ratings_df_columns = ["movieId", "rating", "timestamp"]
credits_df_columns = ["id", "cast"]

print("Discard columns")
movies_df_cleaned = movies_df.dropna(subset=movies_df_columns)[
    movies_df_columns].copy()
ratings_df_cleaned = ratings_df.dropna(subset=ratings_df_columns)[
    ratings_df_columns].copy()
credits_df_cleaned = credits_df.dropna(subset=credits_df_columns)[
    credits_df_columns].copy()

del movies_df
del ratings_df
del credits_df


print("Format dates")
movies_df_cleaned['release_date'] = pd.to_datetime(
    movies_df_cleaned['release_date'], format='%Y-%m-%d', errors='coerce')
ratings_df_cleaned['timestamp'] = pd.to_datetime(
    ratings_df_cleaned['timestamp'], unit='s')

print("Format ints")
movies_df_cleaned['budget'] = pd.to_numeric(
    movies_df_cleaned['budget'], errors='coerce')
movies_df_cleaned['revenue'] = pd.to_numeric(
    movies_df_cleaned['revenue'], errors='coerce')

# Replace json fields with string arrays


def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)
        return [data['name'] for data in dictionary_list]
    except (ValueError, SyntaxError):
        return []


print("Replace json fields with string arrays")
movies_df_cleaned['genres'] = movies_df_cleaned['genres'].apply(
    dictionary_to_list)
movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].apply(
    dictionary_to_list)
movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].apply(
    dictionary_to_list)
credits_df_cleaned['cast'] = credits_df_cleaned['cast'].apply(
    dictionary_to_list)

movies_df_cleaned['genres'] = movies_df_cleaned['genres'].astype(str)
movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].astype(
    str)
movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].astype(
    str)

print("Get movies from Argentina and post 2000")
movies_argentina_post_2000_df = movies_df_cleaned[
    (movies_df_cleaned['production_countries'].str.contains('Argentina', case=False, na=False)) &
    (movies_df_cleaned['release_date'].dt.year >= 2000)
]


def query1(ds: pd.DataFrame):
    print("Query 1")
    movies_argentina_españa_00s_df = ds[
        (movies_df_cleaned['production_countries'].str.contains('Spain', case=False, na=False)) &
        (movies_df_cleaned['release_date'].dt.year < 2010)
    ]

    return movies_argentina_españa_00s_df[["title", "genres"]]


def query2():
    print("Query 2")
    solo_country_df = movies_df_cleaned[movies_df_cleaned['production_countries'].apply(
        lambda x: len(eval(x)) == 1)]
    solo_country_df.loc[:, 'country'] = solo_country_df['production_countries'].apply(
        lambda x: eval(x)[0])
    investment_by_country = solo_country_df.groupby(
        'country')['budget'].sum().sort_values(ascending=False)
    top_5_countries = investment_by_country.head(5)
    return top_5_countries


def query3(ds: pd.DataFrame):
    print("Query 3")

    movies_argentina_post_2000_df = ds.astype({
        'id': int})

    ranking_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(ratings_df_cleaned,
                                                                                    left_on="id",
                                                                                    right_on="movieId")
    mean_ranking_arg_post_2000_df = ranking_arg_post_2000_df.groupby(
        ["id", "title"])['rating'].mean().reset_index()

    mean_ranking_arg_post_2000_df = mean_ranking_arg_post_2000_df.drop(columns=[
                                                                       "id"])

    return (
        mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmax(
        )],
        mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmin(
        )]
    )


def query4(ds):
    print("Query 4")

    movies_argentina_post_2000_df = ds.astype({
        'id': int})

    cast_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(credits_df_cleaned,
                                                                                 on="id")
    cast_and_movie_arg_post_2000_df = cast_arg_post_2000_df.set_index(
        "id")["cast"].apply(pd.Series).stack().reset_index("id", name="name")
    cast_per_movie_quantities = cast_and_movie_arg_post_2000_df.groupby(
        ["name"]).count().reset_index().rename(columns={"id": "count"})
    return cast_per_movie_quantities.nlargest(10, 'count')


def query5():
    print("Query 5")
    q5_input_df = movies_df_cleaned.copy()
    q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
    q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]

    # Initialize the sentiment analyzer
    sentiment_analyzer = pipeline(
        'sentiment-analysis',
        model='distilbert-base-uncased-finetuned-sst-2-english',
        device=0  # Use GPU (device=0 for the first GPU)
    )

    # Function to truncate or split text
    def analyze_sentiment(text):
        if len(text) > 512:
            text = text[:512]  # Truncate to 512 characters
        return sentiment_analyzer(text)[0]['label']

    # Apply sentiment analysis
    q5_input_df['sentiment'] = q5_input_df['overview'].fillna(
        '').apply(analyze_sentiment)

    # Calculate rate_revenue_budget
    q5_input_df["rate_revenue_budget"] = q5_input_df["revenue"] / \
        q5_input_df["budget"]

    # Group by sentiment and calculate the mean
    average_rate_by_sentiment = q5_input_df.groupby(
        "sentiment")["rate_revenue_budget"].mean()
    sentiment_count = q5_input_df.groupby(
        "sentiment")["rate_revenue_budget"].count()

    print(sentiment_count)

    return average_rate_by_sentiment


def save_json(results: dict[str, Any], filename: str):
    with open(filename, "w") as f:
        json.dump(results, f, indent=4)


if __name__ == "__main__":
    results: dict[str, Any] = {}

    results["Query1"] = query1(
        movies_argentina_post_2000_df).to_dict(orient="records")
    results["Query2"] = query2().to_dict()

    results["Query3"] = dict()
    query3_tuple = query3(
        movies_argentina_post_2000_df)
    results["Query3"]["max"] = query3_tuple[0].to_dict()
    results["Query3"]["min"] = query3_tuple[1].to_dict()

    results["Query4"] = query4(
        movies_argentina_post_2000_df).to_dict(orient="records")
    results["Query5"] = query5().to_dict()

    save_json(results, EXPECTED_FILE)
